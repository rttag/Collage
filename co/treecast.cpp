#include "treecast.h"
#include "treecastHeader.h"
#include "localNode.h"
#include "oCommand.h"
#include "objectDataICommand.h"
#include "log.h"
#include "global.h"
#include <lunchbox/scopedMutex.h>
#include <lunchbox/uuid.h>
#include <boost/bind/bind.hpp>
#include <lunchbox/thread.h>
#include <boost/asio/time_traits.hpp>
#include <co/connectionDescription.h>

#include <algorithm>

typedef boost::asio::time_traits<boost::posix_time::ptime> time_traits_t; 

namespace co {


    namespace detail
    {
        class TimerThread : public lunchbox::Thread
        {
        public:
            TimerThread( boost::asio::io_service& io ) : _io(io), _work( new boost::asio::io_service::work( _io ) ) {}
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
                delete _work;
                _work = 0;
            }

        private:
            boost::asio::io_service& _io;
            boost::asio::io_service::work* _work;
        };
    }

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
, _pingTimer( _io )
, _resendTimer( _io )
, _messageRecordHandler()
, _ioThread( new detail::TimerThread( _io ) )
, _needToStartTimers( true )
, _queueMonitor( true )

{
    _ioThread->start();
}

Treecast::~Treecast()
{
    _pingTimer.cancel();
    _resendTimer.cancel();
    _ioThread->stopIOService();
    _ioThread->join();
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
        myNodes.push_back( node->getNodeID( ));
    }

    // Sort the rest of the myNodes vector for quicker lookup later
    std::sort(myNodes.begin()+1, myNodes.end());
    std::unique(myNodes.begin()+1, myNodes.end());
    _updateBlackNodeList( myNodes );
    _filterNodeList( myNodes );
    // Let's check that the user complied to the protocol and didn't include the local node in the myNodes vector
    std::vector<NodeID>::const_iterator it = std::lower_bound(myNodes.begin()+1, myNodes.end(), myNodes[0]);
    LBASSERT(it == myNodes.end() || *it != myNodes[0]);
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
    lunchbox::ScopedWrite lock( _mutex );
    _messageRecordHandler.createOrUpdateMessageRecord( msgID, myNodes, byteCount, 0 );
    TreecastMessageRecordPtr record = _messageRecordHandler.getRecordByID( msgID );
    memcpy( record->buffer.getData(), data.getData(), data.getSize() );

    _dataTimeQueue.push_back( std::make_pair(msgID, _getCurrentTimeMilliseconds() ) );
    _queueMonitor = _dataTimeQueue.size() < MAX_MESSAGE_BUFF_SIZE;
    _checkTimers();
    
    _executeSend(header, data);
}

boost::posix_time::ptime Treecast::_getCurrentTimeMilliseconds() 
{
    boost::posix_time::ptime micro_time = boost::posix_time::microsec_clock::local_time();
    boost::posix_time::time_duration duration( micro_time.time_of_day() );
    boost::posix_time::time_duration milli_duration = boost::posix_time::milliseconds( duration.total_milliseconds() );
    return boost::posix_time::ptime( micro_time.date(), milli_duration );
}

void Treecast::_checkTimers()
{
    if( _needToStartTimers )
    {
        _resetTimers();
        _needToStartTimers = false;
    }
}

void Treecast::_executeSend( ScatterHeader& header, lunchbox::Bufferb const& data ) 
{
    OCommand cmd( _localNode, _localNode, CMD_NODE_TREECAST_SEND );
    Array<const uint8_t> payload( data.getData(), data.getSize());
    cmd << header << data.getSize() << payload;
}

void Treecast::onSendCommand(ICommand& command)
{
    ScatterHeader header;
    lunchbox::Bufferb payload;
    command >> header >> payload;
    _updateBlackNodeList( header.nodes );
    _filterNodeList( header.nodes );
    bool smallMessage = (header.byteCount <= _config.smallMessageThreshold);
    if (smallMessage)
    {
        processSmallScatterCommand(header, payload);
    }
    else
    {
        processScatterCommand(header, payload);
    }
    _checkTimers();
}

//list of nodes we want to filter should be sorted
void Treecast::_filterNodeList( std::vector<NodeID>& nodes ) 
{
    std::vector<NodeID> newNodes;
    newNodes.reserve( nodes.size() );
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
    nodes = newNodes;
}

void Treecast::_printNodes( const std::vector<NodeID>& nodes ) 
{
    for( size_t i = 0; i < nodes.size(); ++i ) 
    {
        NodeID nodeId = nodes[i];
        NodePtr node = _localNode->getNode(nodeId);
        LBERROR << node->getConnectionDescriptions().front() << std::endl;
    }
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
    lunchbox::ScopedWrite lock( _mutex );

    if( _dataTimeQueue.empty() )
        return;
    UUID messageId = _dataTimeQueue.front().first;

    TreecastMessageRecordPtr record = _messageRecordHandler.getRecordByID( messageId );
    std::vector<NodeID>& nodes = record->nodes;
    for( size_t i = 0; i < nodes.size(); ++i ) 
    {
        NodeID nodeId = nodes[i];
        NodePtr node = _localNode->getNode( nodeId );
        if( node )
            _localNode->ping( node );
    }
}

void Treecast::resendBufferedMessage( const boost::system::error_code& e ) 
{
    if( e == boost::asio::error::operation_aborted )
        return;
    lunchbox::ScopedWrite lock( _mutex );
    if( _dataTimeQueue.empty() )
        return;
    
    size_t q_size = _dataTimeQueue.size();
    for( size_t i = 0; i < q_size; ++i ) 
    {
        UUID messageId = _dataTimeQueue.front().first;
        _dataTimeQueue.pop_front();
        TreecastMessageRecordPtr record = _messageRecordHandler.getRecordByID( messageId );
        std::vector<NodeID>& nodes = record->nodes;
        _updateBlackNodeList( nodes );
        //update nodes list
        _filterNodeList( nodes );
        /*LBERROR << "Resend is done!" <<std::endl;
        _printNodes( nodes );*/
        LBERROR << "Resend message: "<< messageId <<std::endl;
        //if we don't have any nodes to send to, return
        if( nodes.empty() )
            return;

        lunchbox::Bufferb& data = record->buffer;
        size_t byteCount = data.getSize();
        ScatterHeader header;
        header.byteCount = byteCount;
        header.messageId = messageId;
        header.nodes = nodes;
        //push message again into time queue, as we don't know if it will succeed 
        _dataTimeQueue.push_back( std::make_pair( messageId, _getCurrentTimeMilliseconds() ) );

        _needToStartTimers = true;
        _executeSend( header, data );
    }
    LBERROR << "Resend is done!" <<std::endl;
}

void Treecast::checkForFinished(UUID const& messageId, bool needDispatch)
{
    lunchbox::ScopedWrite lock( _mutex );
    TreecastMessageRecordPtr record =
        _messageRecordHandler.checkAndCleanUpMessageRecord(messageId, true);

    if (record)
    {
        std::vector<NodeID> childNodes;
        _messageRecordHandler.getChildNodes( record->nodes, childNodes );
        //send ACK to the parent
        if( record->isFullyAcknowledged( childNodes ) ) 
        {
            LBERROR << "TREECAST_ACK: Message " << messageId << std::endl;
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
            //check if buffer is ok
            ObjectDataICommand cmd( _localNode, _localNode->getNode( record->nodes[0] ), buffer, false );
            _localNode->dispatchCommand( cmd );
            _lastDispatchedMessages.push_back( messageId );
            if( _lastDispatchedMessages.size() >= MAX_MESSAGE_BUFF_SIZE )
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
}

void Treecast::_resetTimers() 
{
    //no messages
    lunchbox::ScopedWrite lock( _mutex );
    if( _dataTimeQueue.empty() )
        return;
    //boost::posix_time::ptime start = ( _dataTimeQueue.front() ).second ;
    //boost::posix_time::time_duration duration = boost::posix_time::milliseconds( co::Global::getTimeout() );
    //boost::posix_time::ptime resendDeadline = start + duration;
    _pingTimer.expires_from_now( boost::posix_time::time_duration( boost::posix_time::milliseconds( co::Global::getKeepaliveTimeout() ) ) );
    _resendTimer.expires_from_now( boost::posix_time::time_duration( boost::posix_time::milliseconds( co::Global::getTimeout() ) ) );
    //_resendTimer.expires_at( resendDeadline );

    _pingTimer.async_wait(  BOOST_BIND( &Treecast::pingNodes, this, boost::asio::placeholders::error ) );
    _resendTimer.async_wait( BOOST_BIND( &Treecast::resendBufferedMessage, this, boost::asio::placeholders::error ) );
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
    lunchbox::ScopedWrite lock( _mutex );
    TreecastHeader header;
    NodeID childId;
    command >> header >> childId ;
    UUID messageId = header.messageId;
    size_t rank = _messageRecordHandler.calculateRank( header.nodes );
    TreecastMessageRecordPtr record = _messageRecordHandler.getRecordByID( messageId );
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
        _updateBlackNodeList( childNodes );
        sort( childNodes.begin(), childNodes.end() );
        _filterNodeList( childNodes );
        if( record && record->isFullyAcknowledged( childNodes ) ) //all children send ACK to this message
        {
            LBERROR << "Treecast Master got ACK message: " << messageId << std::endl;
            _messageRecordHandler.deleteRecord( messageId );
            
            //bool restartTimers = _dataTimeQueue.front().first == messageId;
            _deleteMessageFromTimeQueue( messageId );
            _queueMonitor = _messageRecordHandler.mapSize() < MAX_MESSAGE_BUFF_SIZE;
            if( _dataTimeQueue.size() != 0 ) 
                _resetTimers();
            else 
            {
                _pingTimer.cancel();
                _resendTimer.cancel();
                _needToStartTimers = true;
            }
        }
    }
}

void Treecast::_send( NodeID destinationId, ScatterHeader const& header, Array<const uint8_t> const& data )
{
    NodePtr destNode = _localNode->getNode( destinationId );
    if ( !destNode )
        destNode = _localNode->connect( destinationId );
    if( !destNode ) {
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
        lunchbox::ScopedWrite lock( _mutex );
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
        lunchbox::ScopedWrite lock( _mutex );
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
            lunchbox::ScopedWrite lock( _mutex );
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
