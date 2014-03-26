#include "treeCast.h"
#include "treecastHeader.h"
#include "localNode.h"
#include "oCommand.h"
#include "objectDataICommand.h"
#include "log.h"

#include <lunchbox/scopedMutex.h>
#include <lunchbox/uuid.h>

#include <algorithm>

namespace co {

TreecastConfig::TreecastConfig()
: smallMessageThreshold(12*1024)
, blacklistSize(10)
, baseTimeout(10000)
, masterTimeoutPerMB(300)
, messageTimeoutPerMB(330)
, sendRetryTimeout(100)
, sendRetryCount(50)
{
}

void Treecast::init()
{
}

void Treecast::shutdown()
{
}

void Treecast::updateConfig(TreecastConfig const& config)
{
    // I guard the updates with a mutex (but not the accesses) to make sure that after the update
    // we get a consistent state
    lunchbox::ScopedWrite mutex(_configUpdateMutex);
    _config = config;
}

Treecast::Treecast( TreecastConfig const& config)
: _config(config) 
, _messageRecordHandler()
{
}

void Treecast::send(lunchbox::Bufferb& data, uint32_t resendNr, Nodes const& nodes)
{
    LBASSERT(!nodes.empty());

    size_t byteCount = data.getSize();

    // create a vector of nodeIDs that we can work with
    std::vector<NodeID> myNodes;
    myNodes.reserve( nodes.size() + 1 );
    myNodes.push_back( _localNode->getNodeID( ));
    Nodes::const_iterator i = nodes.begin();
    for( ; i != nodes.end(); ++i )
    {
        NodePtr node = *i;
        myNodes.push_back( node->getNodeID( ));
    }

    // Sort the rest of the myNodes vector for quicker lookup later
    std::sort(myNodes.begin()+1, myNodes.end());
    size_t node_count_before_unique = myNodes.size();
    std::unique(myNodes.begin()+1, myNodes.end());
    size_t node_count_after_unique = myNodes.size();
    LBASSERTINFO(node_count_before_unique == node_count_after_unique,
        "The destination node list contains duplicate entries.");
    // Let's check that the user complied to the protocol and didn't include the local node in the myNodes vector
    std::vector<NodeID>::const_iterator it = std::lower_bound(myNodes.begin()+1, myNodes.end(), myNodes[0]);
    LBASSERT(it == myNodes.end() || *it != myNodes[0]);
    // Store metadata about this message
    lunchbox::UUID msgID(true);

    // Create the header
    ScatterHeader header;
    header.byteCount = byteCount;
    header.messageId = msgID;
    header.resendNr = resendNr;
    header.nodes = myNodes;

    LBLOG( LOG_TC ) << "send message " << msgID << " with " << data.getNumBytes() 
                    << "bytes of data" << std::endl;

    bool smallMessage = (byteCount <= _config.smallMessageThreshold);
    if (smallMessage)
    {
        processSmallScatterCommand(header, data);
    }
    else
    {
        processScatterCommand(header, data);
    }
}

void Treecast::checkForFinished(UUID const& messageId, uint32_t resendNr)
{
    TreecastMessageRecordPtr record =
        _messageRecordHandler.checkAndCleanUpMessageRecord(messageId, resendNr, true);

    if (record)
    {
        BufferPtr buffer = _localNode->allocBuffer( record->buffer.getSize() );
        buffer->replace( record->buffer.getData(), record->buffer.getSize() );
        ObjectDataICommand cmd( _localNode, _localNode->getNode( record->nodes[0] ), buffer, false );
        _localNode->dispatchCommand( cmd );
    }
}

void Treecast::onScatterCommand(ICommand& command)
{
    ScatterHeader header;
    lunchbox::Bufferb payload;
    command >> header >> payload;
    processScatterCommand(header, payload);
}

void Treecast::onSmallScatterCommand(ICommand& command)
{
    ScatterHeader header;
    lunchbox::Bufferb payload;
    command >> header >> payload;
    processSmallScatterCommand(header, payload);
}

void Treecast::onAllgatherCommand(ICommand& command)
{
    // Deserialize header
    AllgatherHeader header;
    lunchbox::Bufferb payload;
    command >> header >> payload;

    size_t nodeCount = header.nodes.size();
    const size_t rank = _messageRecordHandler.calculateRank(header.nodes);
    // Forward the message. Rank 0 can be skipped, as he doesn't care anyway.
    size_t destinationRank = (rank + 1) % nodeCount;
    // Let's check if the destination is the original source of the message
    if (destinationRank != header.pieceNr)
    {
        // Rank 0 doesn't care, he doesn't need this data.
        if (0 == destinationRank)
            destinationRank = 1;

        // Let's check again that he is not the original source of the message
        if (destinationRank != header.pieceNr)
        {
            // Send the message
            Array<const uint8_t> data( payload.getData(), payload.getSize());
            header.cmd = CMD_NODE_TREECAST_ALLGATHER;
            NodeID destinationId = header.nodes[destinationRank];
            _send(destinationId, header, data);
        }
    }
    // Process the message
    processAllgatherCommand(header, payload);
}

void Treecast::_send(NodeID destinationId, TreecastHeader const& header, Array<const uint8_t> const& data )
{
    NodePtr destNode = _localNode->getNode( destinationId );
    if ( !destNode.isValid() )
        destNode = _localNode->connect( destinationId );
    uint64_t size = data.getNumBytes();
    OCommand cmd(destNode->send( header.cmd ));
    cmd << header << size << data;
}

void Treecast::writeOnePiece(TreecastMessageRecordPtr record, size_t pos,
                             const uint8_t* beginInput, size_t size, uint64_t destOffset)
{
    LBASSERT(pos < record->state.size());
    // Mark that we are writing the current piece of data
    if (!record->state[pos].compareAndSwap(TREECAST_PIECE_STATE_EMPTY,
        TREECAST_PIECE_STATE_WRITING))
    {
        // We don't need to write this piece, someone else is doing it or has done it already
        return;
    }

    // Copy this piece into the result buffer
    memcpy( record->buffer.getData() + destOffset, beginInput, size);

    // Mark that we have finished writing the current piece of the data
    record->state[pos] = TREECAST_PIECE_STATE_FULL;
}

void Treecast::propagateScatter(TreecastHeader & header, lunchbox::Bufferb const& data, uint64_t receivedSize, uint64_t pieceSize, 
                                size_t nodeCount, size_t rank, bool smallMessage)
{
    // Let's set up the mask for the next operation, propagating the scatter to the children of this node in the tree.
    // If we are rank 0, then the children are all other ranks. Otherwise, we have to find the least significant 1 bit
    // in our rank, and all the zeros to the right of this 1 bit have to be varied, those ranks are the ones that
    // represent our subtree.
    size_t mask = 0x1;
    while (!(mask & rank) && mask < nodeCount)
    {
        mask <<= 1;
    }

    // This process is responsible for all processes that have bits
    // set from the least significant bit up to (but not including) mask.
    // Because of the "not including", we start by shifting mask back down
    // one.

    // Propagate the scatter
    uint64_t currSize = receivedSize;
    for (mask >>= 1; mask > 0; mask >>= 1)
    {
        if (rank + mask < nodeCount)
        {
            // mask is also the size of this process's subtree
            uint64_t sendSize;
            uint64_t sourceOffset;
            if (smallMessage)
            {
                sendSize = data.getSize();
                sourceOffset = 0;
                header.cmd = CMD_NODE_TREECAST_SMALLSCATTER;
            }
            else
            {
                sendSize = ((currSize <= (pieceSize * mask)) ? 0 : currSize - (pieceSize * mask));
                sourceOffset = mask * pieceSize;
                header.cmd = CMD_NODE_TREECAST_SCATTER;
                currSize -= sendSize;
            }

            // Usually we don't send 0-sized messages, but in the smallMessage case we need to
            // as this could be a message with an empty buffer
            if (sendSize > 0 || smallMessage)
            {
                Array<const uint8_t> sendData( data.getData() + sourceOffset, sendSize );
                size_t destinationRank = rank + mask;
                NodeID destinationId = header.nodes[destinationRank];
                LBLOG( LOG_TC ) << "sending " << (smallMessage ? "scatter " : "smallscatter ")
                    << header.messageId << " to " << destinationId << std::endl;
                _send(destinationId, header, sendData);
            }
        }
    }
}

void Treecast::initiateAllgather(ScatterHeader& header, lunchbox::Bufferb const& data,
                                 uint64_t receivedSize, uint64_t pieceSize, size_t pieceNr)
{
    // The rank is the same as the pieceNr
    size_t rank = pieceNr;
    // Initiate the allgather on this node
    uint64_t sendSize = std::min(pieceSize, receivedSize);
    // Initialize the header
    AllgatherHeader allgatherHeader;
    allgatherHeader.byteCount = header.byteCount;
    allgatherHeader.messageId = header.messageId;
    allgatherHeader.resendNr = header.resendNr;
    allgatherHeader.nodes = header.nodes;
    allgatherHeader.pieceNr = pieceNr;
    allgatherHeader.cmd = CMD_NODE_TREECAST_ALLGATHER;

    // Send the message using the ring
    Array<const uint8_t> sndData(data.getData(), sendSize);
    size_t destinationRank = (rank + 1) % header.nodes.size();
    NodeID destinationId = header.nodes[destinationRank];
    LBLOG( LOG_TC ) << "sending " << "gather " << allgatherHeader.messageId
        << "." << allgatherHeader.pieceNr << " to " << destinationId << std::endl; 

    _send(destinationId, allgatherHeader, sndData);
}

void Treecast::processScatterCommand(ScatterHeader& header, lunchbox::Bufferb const& data)
{
    size_t nodeCount = header.nodes.size();
    LBASSERT(nodeCount > 0);

    // Calculate basic values
    // This is the size of the currently received chunk
    uint64_t receivedSize = data.getSize();
    // This is the size of the basic piece
    uint64_t pieceSize = (header.byteCount + nodeCount - 1) / nodeCount;
    // The overall number of pieces
    size_t pieceCount = pieceSize ? (header.byteCount + pieceSize - 1) / pieceSize : 1;

    // Calculate my own rank in the communication
    const size_t rank = _messageRecordHandler.calculateRank(header.nodes);
    LBLOG(LOG_TC) << "Received scatter command with message id " << header.messageId
                  << ". My rank is: " << rank << std::endl;

    // Propagate the scatter
    propagateScatter(header, data, receivedSize, pieceSize, nodeCount, rank, false);

    // Initiate the allgather
    initiateAllgather(header, data, receivedSize, pieceSize, rank);

    TreecastMessageRecordPtr record;
    // If I am the initiator, I don't need to get the data, I have it already
    if (0 != rank)
    {
        // Make sure there is a record for this message
        record = _messageRecordHandler.createOrUpdateMessageRecord(header.messageId, header.resendNr,
            header.nodes, header.byteCount, pieceCount);

        // We could save all received pieces now, but it would complicate handling of
        // the allgather messages that arrive for those pieces, so I just don't save them now,
        // as we will receive them later anyway, through allgather. So let's just save
        // the first piece, where I will initiate the allgather.
        size_t endSrcPos = std::min(pieceSize, data.getSize());
        writeOnePiece(record, rank, data.getData(), data.getSize(), rank * pieceSize);
    }

    // If we are not the initiator, check for finish
    if (0 != rank)
    {
        // If we have all pieces already, notify the user about the newly arrived message
        checkForFinished(header.messageId, header.resendNr);
    }
}

void Treecast::processSmallScatterCommand( ScatterHeader& header, lunchbox::Bufferb const& data )
{
    size_t nodeCount = header.nodes.size();
    LBASSERT(nodeCount > 0);

    size_t payload_size = data.getSize();

    // Calculate my own rank in the communication
    const size_t rank = _messageRecordHandler.calculateRank(header.nodes);
    LBLOG(LOG_TC) << "Received small scatter command with message id " << header.messageId
        << ". My rank is: " << rank << std::endl;

    // Propagate the scatter
    propagateScatter(header, data, header.byteCount, header.byteCount, nodeCount, rank, true);

    TreecastMessageRecordPtr record;
    // If I am the initiator, I don't need to get the data, I have it already
    if (0 != rank)
    {
        // Make sure there is a record for this message
        record = _messageRecordHandler.createOrUpdateMessageRecord(header.messageId, header.resendNr,
            header.nodes, header.byteCount, 1);

        if (payload_size > 0)
        {
            writeOnePiece(record, 0, data.getData(), data.getSize(), 0);
        }
        else
        {
            writeOnePiece(record, 0, data.getData(), 0, 0);
        }
    }

    // If we are not the initiator, ...
    if (0 != rank)
    {
        // If we have all pieces already, notify the user about the newly arrived message
        checkForFinished(header.messageId, header.resendNr);
    }
}

void Treecast::processAllgatherCommand(AllgatherHeader const& header, lunchbox::Bufferb const& data)
{
    LBASSERT(!header.nodes.empty());
    size_t nodeCount = header.nodes.size();

    // Calculate basic values
    // This is the size of the basic piece
    size_t pieceSize = (header.byteCount + nodeCount - 1) / nodeCount;
    // The overall number of pieces
    size_t pieceCount = pieceSize ? (header.byteCount + pieceSize - 1) / pieceSize : 1;

    LBASSERT(header.pieceNr < pieceCount);

    // Calculate my own rank in the communication
    const size_t rank = _messageRecordHandler.calculateRank(header.nodes);
    LBASSERT(rank != header.pieceNr);
    LBLOG( LOG_TC) << "Received allgather command. My rank is: " << rank << std::endl;

    TreecastMessageRecordPtr record;
    // If I am the initiator, I don't need to get the data, I have it already
    if (0 != rank)
    {
        // Create message record if necessary
        record = _messageRecordHandler.createOrUpdateMessageRecord(header.messageId, header.resendNr, header.nodes,
            header.byteCount, pieceCount);
        writeOnePiece(record, header.pieceNr, data.getData(), data.getSize(), header.pieceNr * pieceSize);

        // If we have all pieces already, notify the user about the newly arrived message
        checkForFinished(header.messageId, header.resendNr);
    }
}

 void Treecast::setLocalNode(LocalNode* localNode)
 {
     _localNode = localNode;
     _messageRecordHandler.setLocalNode( _localNode );
 }
}
