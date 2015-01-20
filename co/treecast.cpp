#include "treecast.h"
#include "treecastHeader.h"
#include "localNode.h"
#include "oCommand.h"
#include "objectDataICommand.h"
#include "log.h"
#include "global.h"

#include <co/connectionDescription.h>

#include <lunchbox/scopedMutex.h>
#include <lunchbox/uuid.h>
#include <lunchbox/thread.h>

#include <boost/bind/bind.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/asio/placeholders.hpp>

#include <algorithm>

namespace co {

namespace detail
{
class TimerThread : public lunchbox::Thread
{
public:
    TimerThread() 
        : _io()
        , _timer( _io )
        , _work( _io ) {}
    virtual bool init()
    {
        setName( std::string("TimerTreeCastThread") );
        return true;
    }
    virtual void run() 
    {
        _io.run(); 
    }
    void stopIOService()
    {
        _timer.cancel();
        _io.stop();
    }

    boost::asio::deadline_timer& timer()
    {
        return _timer;
    }

private:
    boost::asio::io_service        _io;
    boost::asio::deadline_timer    _timer;
    boost::asio::io_service::work  _work;
};
}

void Treecast::init()
{
    _ioThread = new detail::TimerThread();
    _ioThread->start();
}

void Treecast::shutdown()
{
    _ioThread->stopIOService();
    _ioThread->join();
}

Treecast::Treecast()
: _messageRecordHandler()
, _ioThread( 0 )
, _queueMonitor( true )
{
}

Treecast::~Treecast()
{
}

void Treecast::send( lunchbox::Bufferb& data, Nodes const& nodes )
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
        if ( node->getNodeID() != _localNode->getNodeID() )
            myNodes.push_back( node->getNodeID( ));
    }

    // Sort the rest of the myNodes vector for quicker lookup later
    std::sort(myNodes.begin()+1, myNodes.end());
    std::unique(myNodes.begin()+1, myNodes.end());
    _filterNodeList( myNodes );

    // Store metadata about this message
    lunchbox::UUID msgID(true);

    // Create the header
    ScatterHeader header;
    header.byteCount = byteCount;
    header.messageId = msgID;
    header.nodes = myNodes;
    
    LBLOG( LOG_TC ) << "send message " << msgID << " with " << data.getNumBytes() 
                    << "bytes of data" << std::endl;
    _queueMonitor.waitEQ( true );
    TreecastMessageRecordPtr record = _messageRecordHandler.createOrUpdateMessageRecord( msgID, myNodes, byteCount, 0 );
    memcpy( record->buffer.getData(), data.getData(), data.getSize() );

    _executeSend(header, data, false);
}

boost::posix_time::ptime Treecast::_getCurrentTimeMilliseconds() 
{
#ifdef _WIN32
    return boost::posix_time::microsec_clock::local_time();
#else
    boost::posix_time::ptime micro_time = boost::posix_time::microsec_clock::local_time();
    boost::posix_time::time_duration duration( micro_time.time_of_day() );
    boost::posix_time::time_duration milli_duration = boost::posix_time::milliseconds( duration.total_milliseconds() );
    return boost::posix_time::ptime( micro_time.date(), milli_duration );
#endif
}

void Treecast::_executeSend( ScatterHeader& header, lunchbox::Bufferb const& data, bool needPop ) 
{
    OCommand cmd( _localNode, _localNode, CMD_NODE_TREECAST_SEND );
    Array<const uint8_t> payload( data.getData(), data.getSize());
    cmd << header << data.getSize() << payload << needPop;
}

void Treecast::onSendCommand(ICommand& command)
{
    ScatterHeader header;
    lunchbox::Bufferb payload;
    bool needPop;
    command >> header >> payload >> needPop;
    _filterNodeList( header.nodes );
    LBLOG( LOG_TC ) << "sending message " << header.messageId << " to " << header.nodes.size() << std::endl;
    
    {
        lunchbox::ScopedWrite lock(_mutex);
        bool isEmptyTimeQueue = _dataTimeQueue.empty();
        _dataTimeQueue.push_back( std::make_pair( header.messageId, _getCurrentTimeMilliseconds() ) );
        if( needPop )
            _deleteMessageFromTimeQueue( header.messageId );
        else
            _queueMonitor = _dataTimeQueue.size() < MAX_MESSAGE_BUFF_SIZE;
        //reset timer if we pushed new message in the queue
        if( isEmptyTimeQueue && !_dataTimeQueue.empty() ) 
            _resetTimers();
    }

    const uint64_t threshold = 
        Global::getIAttribute( Global::IATTR_TREECAST_SMALLSCATTER_THRESHOLD );
    bool smallMessage = (header.byteCount <= threshold );
    if (smallMessage)
    {
        processSmallScatterCommand(header, payload);
    }
    else
    {
        processScatterCommand(header, payload);
    }
}

//list of nodes we want to filter should be sorted
bool Treecast::_filterNodeList( std::vector<NodeID>& nodes ) 
{
    std::vector<NodeID> newNodes;
    newNodes.reserve( nodes.size() );
    if( _blackListNodes.empty() )
        return false;
    std::set<NodeID>::iterator it = _blackListNodes.begin();
    for( size_t i = 0; i < nodes.size(); ++i ) 
    {
        NodeID nodeId = nodes[i];
        NodePtr node = _localNode->getNode( nodeId );
        if( node && (it == _blackListNodes.end() || *it != nodeId) ) 
        {
            newNodes.push_back( nodeId );
        }
        else if( *it <= nodeId && it != _blackListNodes.end() ) 
        {
            it++;
        } 
    }
    if( nodes.size() == newNodes.size() )
        return false;
    nodes.swap(newNodes);
    return true;
}

void Treecast::_updateBlackNodeList( std::vector<NodeID>& nodes ) 
{
    for( size_t i = 1; i < nodes.size(); ++i ) 
    {
        NodeID nodeId = nodes[i];
        NodePtr node = _localNode->getNode( nodeId );
        if( node ) {
            //if node is not active, put in into the black list
            int64_t lastTime = _localNode->getTime64() - node->getLastReceiveTime();
            if( lastTime > co::Global::getTimeout() 
                || !node->isConnected() ) 
                _blackListNodes.insert( nodeId );
        }
        else 
            _blackListNodes.insert( nodeId );
    }
}

//ping nodes of the first message in the time queue
void Treecast::pingNodes( const boost::system::error_code& e ) 
{
    if( e == boost::asio::error::operation_aborted )
        return;
    UUID messageId;
    {
        lunchbox::ScopedRead lock( _mutex );

        if( _dataTimeQueue.empty() )
            return;
        messageId = _dataTimeQueue.front().first;
    }
    TreecastMessageRecordPtr record = _messageRecordHandler.getRecordByID( messageId );
    {
    lunchbox::ScopedFastRead nlock( record->m_nodesSpinLock );
    std::vector<NodeID>& nodes = record->nodes;
    for( size_t i = 0; i < nodes.size(); ++i ) 
    {
        NodeID nodeId = nodes[i];
        NodePtr node = _localNode->getNode( nodeId );
        if( node )
            _localNode->ping( node );
    }
    }
    lunchbox::ScopedRead lock( _mutex );
    _resetTimers();
}

void Treecast::resendBufferedMessage( const boost::system::error_code& e ) 
{
    if( e == boost::asio::error::operation_aborted )
        return;
    lunchbox::ScopedRead lock( _mutex );
    if( _dataTimeQueue.empty() )
        return;

    for( DataTimeQueueIt it = _dataTimeQueue.begin(); it != _dataTimeQueue.end(); ++it ) 
    {
        UUID messageId = it->first;
        TreecastMessageRecordPtr record = _messageRecordHandler.getRecordByID( messageId );
        std::vector<NodeID>& nodes = record->nodes;
        _updateBlackNodeList( nodes );
        //update nodes list
        {
            lunchbox::ScopedFastWrite nlock( record->m_nodesSpinLock );
            if( !_filterNodeList( nodes ))
                continue;
        }
        LBLOG( LOG_TC ) << "Resend message: "<< messageId <<std::endl;
        //if we don't have any nodes to send to, return
        if( nodes.empty() )
            continue;

        lunchbox::Bufferb& data = record->buffer;
        size_t byteCount = data.getSize();
        ScatterHeader header;
        header.byteCount = byteCount;
        header.messageId = messageId;
        header.nodes = nodes;

        _executeSend( header, data, true );
    }
    _resetTimers();
}

void Treecast::checkForFinished(UUID const& messageId, bool needDispatch)
{
    TreecastMessageRecordPtr record =
        _messageRecordHandler.checkAndCleanUpMessageRecord(messageId, true);

    if (record)
    {
        lunchbox::ScopedFastRead lock(record->m_nodesSpinLock);
        LBLOG( LOG_TC ) << "Received message: "<< messageId << std::endl;
        std::vector<NodeID> childNodes;
        _messageRecordHandler.getChildNodes( record->nodes, childNodes );
        //send ACK to the parent
        if( record->isFullyAcknowledged( childNodes ) ) 
        {
            LBLOG( LOG_TC ) << "TREECAST_ACK: Message " << messageId << std::endl;
            const size_t rank = _messageRecordHandler.calculateRank( record->nodes );
            const size_t parentRank = _messageRecordHandler.getParentRank( rank );
            NodePtr parentNode = _localNode->getNode( record->nodes[parentRank] );

            TreecastHeader header;
            header.messageId = messageId;
            header.nodes = record->nodes;
            _messageRecordHandler.deleteRecord( messageId );
            OCommand command( parentNode->send( CMD_NODE_TREECAST_ACKNOWLEDGE ) );
            command << header << _localNode->getNodeID();

        }
        if( needDispatch && std::find( _lastDispatchedMessages.begin(), _lastDispatchedMessages.end(), messageId ) == _lastDispatchedMessages.end() ) 
        {
            BufferPtr buffer = _localNode->allocBuffer( record->buffer.getSize() );
            buffer->replace( record->buffer.getData(), record->buffer.getSize() );
            ObjectDataICommand cmd( _localNode, _localNode->getNode( record->nodes[0] ), buffer, false );
            _localNode->dispatchCommand( cmd );
            LBLOG( LOG_TC ) << "Dispatched message: "<< messageId << std::endl;
            _lastDispatchedMessages.push_back( messageId );
            if( _lastDispatchedMessages.size() > MAX_MESSAGE_BUFF_SIZE )
                _lastDispatchedMessages.pop_front();
        }
    }
}

void Treecast::_deleteMessageFromTimeQueue( UUID const messageId ) 
{
    for( DataTimeQueueIt it = _dataTimeQueue.begin(); it != _dataTimeQueue.end(); ) 
    {
        UUID curMessageId = it->first;
        if( curMessageId == messageId ) 
        {
            it = _dataTimeQueue.erase( it );
            break;
        }
        else 
            ++it;
    }
    _queueMonitor = _dataTimeQueue.size() < MAX_MESSAGE_BUFF_SIZE;
}

void Treecast::_resetTimers() 
{
    //no messages
    if( _dataTimeQueue.empty() )
        return;

    boost::posix_time::ptime startTime = ( _dataTimeQueue.front() ).second ;
    boost::posix_time::ptime curTime = boost::posix_time::microsec_clock::local_time();

    boost::posix_time::time_duration duration = curTime - startTime;
    int64_t durationVal = duration.total_milliseconds();
    int64_t keepAliveTimeout = static_cast<int64_t>( co::Global::getKeepaliveTimeout() );
    int64_t timeout = static_cast<int64_t>( co::Global::getTimeout() );
    int64_t keepAliveVal = keepAliveTimeout - durationVal%keepAliveTimeout;
    int64_t timeoutVal = timeout - durationVal%timeout;
    if( timeoutVal <= keepAliveVal ) 
    {
        _ioThread->timer().expires_from_now( boost::posix_time::time_duration( boost::posix_time::milliseconds(timeoutVal) ) );
        _ioThread->timer().async_wait(  BOOST_BIND( &Treecast::resendBufferedMessage, this, boost::asio::placeholders::error ) );
    }
    else 
    {
        _ioThread->timer().expires_from_now( boost::posix_time::time_duration( boost::posix_time::milliseconds(keepAliveVal) ) );
        _ioThread->timer().async_wait(  BOOST_BIND( &Treecast::pingNodes, this, boost::asio::placeholders::error ) );
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

void Treecast::onAcknowledgeCommand(ICommand& command)
{
    TreecastHeader header;
    NodeID childId;
    command >> header >> childId ;
    UUID messageId = header.messageId;
    size_t rank = _messageRecordHandler.calculateRank( header.nodes );
    TreecastMessageRecordPtr record = _messageRecordHandler.getRecordByID( messageId );
    //if the record has been ACKed already 
    if( !record ) 
        return;
    record->ackNodes.insert( childId );
    //if we are not the master propagate ACK message further
    if( rank != 0 ) 
    {
        //we are not a root - forward ACK further
        checkForFinished( messageId, false );
    }
    else 
    {
        // if we are the root we decide if we need resend or not
        std::vector<NodeID> childNodes;
        _messageRecordHandler.getChildNodes( header.nodes, childNodes );
        sort( childNodes.begin(), childNodes.end() );
        if( record && record->isFullyAcknowledged( childNodes ) ) //all children send ACK to this message
        {
            LBLOG( LOG_TC ) << "Treecast Master got ACK message: " << messageId << std::endl;
            _messageRecordHandler.deleteRecord( messageId );
            lunchbox::ScopedWrite lock( _mutex );
            _deleteMessageFromTimeQueue( messageId );
            if( !_dataTimeQueue.empty() ) 
                _resetTimers();
            else 
                _ioThread->timer().cancel();
        }
    }
}

void Treecast::_send( NodeID destinationId, ScatterHeader const& header, Array<const uint8_t> const& data )
{
    NodePtr destNode = _localNode->getNode( destinationId );
    if ( !destNode )
    {
        if ( _blackListNodes.find(destinationId) == _blackListNodes.end() )
            destNode = _localNode->connect( destinationId );
    }
    if( !destNode ) 
    {
        _blackListNodes.insert( destinationId );
        LBERROR << "Can't connect to node: " <<destinationId << std::endl;
        return;
    }
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

void Treecast::propagateScatter(ScatterHeader & header, lunchbox::Bufferb const& data, uint64_t receivedSize, uint64_t pieceSize, 
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
                LBLOG( LOG_TC ) << "sending " << (smallMessage ? "smallscatter " : "scatter ")
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

    // If I am the initiator, I don't need to get the data, I have it already
    if (0 != rank)
    {
        // Make sure there is a record for this message
        TreecastMessageRecordPtr record = _messageRecordHandler.createOrUpdateMessageRecord(header.messageId,
            header.nodes, header.byteCount, pieceCount);

        // We could save all received pieces now, but it would complicate handling of
        // the allgather messages that arrive for those pieces, so I just don't save them now,
        // as we will receive them later anyway, through allgather. So let's just save
        // the first piece, where I will initiate the allgather.
        writeOnePiece(record, rank, data.getData(), data.getSize(), rank * pieceSize);
    }

    // If we are not the initiator, check for finish
    if (0 != rank)
    {
        // If we have all pieces already, notify the user about the newly arrived message
        checkForFinished(header.messageId, true);
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

    // If I am the initiator, I don't need to get the data, I have it already
    if (0 != rank)
    {
        // Make sure there is a record for this message
        TreecastMessageRecordPtr record = _messageRecordHandler.createOrUpdateMessageRecord(header.messageId,
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
        checkForFinished(header.messageId, true);
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

    // If I am the initiator, I don't need to get the data, I have it already
    if (0 != rank)
    {
        // Create message record if necessary
        {
            TreecastMessageRecordPtr record = _messageRecordHandler.createOrUpdateMessageRecord(header.messageId, header.nodes,
                header.byteCount, pieceCount);
            writeOnePiece(record, header.pieceNr, data.getData(), data.getSize(), header.pieceNr * pieceSize);
        }
        // If we have all pieces already, notify the user about the newly arrived message
        checkForFinished(header.messageId, true);
    }
}

 void Treecast::setLocalNode(LocalNode* localNode)
 {
     _localNode = localNode;
     _messageRecordHandler.setLocalNode( _localNode );
 }
}
