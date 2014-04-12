#pragma once

#include "buffer.h"
#include "nodeCommand.h"
#include "treecastMessageRecordHandler.h"

#include <lunchbox/types.h>
#include <lunchbox/lock.h>
#include <boost/asio.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/system/error_code.hpp>
#define MAX_MESSAGE_BUFF_SIZE 64

namespace co {

namespace detail 
{
    class TimerThread;
}

class ScatterHeader;
class AllgatherHeader;
typedef std::list< std::pair<UUID, boost::posix_time::ptime > > DataTimeQueue;
typedef DataTimeQueue::iterator DataTimeQueueIt;
//typedef stde::hash_map<UUID, int64_t > DataTimerMap;
//typedef DataTimerMap::iterator DataTimerMapIt;
//typedef std::vector< std::pair<UUID, co::Array<const uint8_t> > > DataBuffVec;
template<typename T> class Array;

//! Struct representing the configuration of Treecast
//!
//! Note: the master and message timeout is meant on a per-megabyte basis.
struct TreecastConfig
{
public:
    // Constructor
    TreecastConfig();

public:
    size_t   smallMessageThreshold; //!< The size of the largest message considered small
    size_t   blacklistSize;         //!< The size of the blacklist ring buffer
    uint32_t baseTimeout; //!< The part of timeout independent of MBs
    uint32_t masterTimeoutPerMB;    //!< The timeout/MB on the sender node
    uint32_t messageTimeoutPerMB;   //!< The timeout/MB on the receiver nodes
    uint32_t sendRetryTimeout; //!< Sleep this much before retrying a send
    uint32_t sendRetryCount; //!< Don't retry a send more times than this
};

//! Class taking care of multicast communication.
class Treecast
{
public:

    //! \name Implementation of the IMulticast interface
    //@{
    void init();

    void shutdown();

    void send(lunchbox::Bufferb& data, Nodes const& nodes);
    //@}

    //! Used for updating the configuration at runtime
    //!
    //! The operation is not thread-safe, use only for testing and debugging, and only when no multicast
    //! communication is in progress.
    //!
    //! \param config The new configuration
    //!
    //! \throw nil doesn't throw
    void updateConfig(TreecastConfig const& config);

    //! Constructor
    //! \param clusterManager The ClusterManager has to be provided here
    //! \param threadPool A WorkerPool has to be provided here, might be the same as for the ClusterManager
    //! \param config A MulticastConfig has to be provided with the parameters of the Multicast
    //!
    //! \throw nil doesn't throw
    Treecast(TreecastConfig const& config);

    //! Dtor
    ~Treecast();

    void setLocalNode(LocalNode* localNode);

    //! Called when a scatter message is received during the scatter step of a multicast with large data.
    //!
    //! Every participating node receives exactly 0 or 1 scatter message as part of a multicast operation.
    //!
    //! \param sender The sender who sent this message to us. Unused.
    //! \param messageData The actual packet received in this message.
    //!
    //! \throw nil doesn't throw
    void onScatterCommand(ICommand& data);

    //! Called when a small scatter message has been received during the scatter step of a multicast with small data.
    //!
    //! Small scatter messages are used instead of scatter messages when the data is small enough to be completely
    //! included in this message. Every participating node receives exactly one small scatter message as part of such
    //! a multicast operation.
    //!
    //! \param sender The sender who sent this message to us. Unused.
    //! \param messageData The actual packet received in this message.
    //!
    //! \throw nil doesn't throw
    void onSmallScatterCommand(ICommand& data);

    //! Called when an allgather message has been received during the allgather step of a multicast with large data.
    //!
    //! Every participating node receives P-1 or P of these messages (depending on him being the initiator of such
    //! a message or not), where P is the number of pieces that the data was cut into in the scatter phase.
    //!
    //! \param sender The sender who sent this message to us. Unused.
    //! \param messageData The actual packet received in this message.
    //!
    //! \throw nil doesn't throw
    void onAllgatherCommand(ICommand& data);

    void onAcknowledgeCommand(ICommand& command);

    void onSendCommand(ICommand& data);

    uint32_t getRetryTimeout();

    void pingNodes( const boost::system::error_code& e );

    void resendBufferedMessage( const boost::system::error_code& e );

private:
    //! Checks if a message has been fully received. If yes, it calls the user callback with the message,
    //! then does the necessary cleanup after the message.
    //!
    //! \param messageId The id of the message
    //! \param resendNr The resendNr that we were triggered with
    //!
    //! \throw nil doesn't throw
    void checkForFinished(UUID const& messageId, bool needDispatch);

    //! Used for sending a message as part of the multicast operation, retrying a few times when necessary.
    //!
    //! When it reaches a pre-configured number of retries, it logs the problem and gives up trying. It doesn't throw.
    //!
    //! \param messageId The id of the message
    //! \param resendNr The resendNr we were triggered with
    //! \param destinationId The id of the node we want to send to
    //! \param data The actual packet we want to send
    //! \param retryCounter A counter which should start at 0 and is incremented by the recursive calls
    //!
    //! \throw nil doesn't throw
    void _send(NodeID destinationId, ScatterHeader const& header, Array<const uint8_t> const& data);

    void _executeSend( ScatterHeader& header, lunchbox::Bufferb const& data );

    bool _filterNodeList( std::vector<NodeID>& nodes );

    void _deleteMessageFromTimeQueue( UUID const messageId );

    void _resetTimers();
    
    void _checkTimers();

    void _updateBlackNodeList( std::vector<NodeID>& nodes );

    boost::posix_time::ptime _getCurrentTimeMilliseconds();
    //! Used for writing a newly received piece of data into the result buffer
    //!
    //! This function makes sure that no one is trying to access the same piece of the data at the same time and
    //! that no one has already written the same piece of data, and if all is good, he writes the piece himself.
    //! If someone else touched the same piece already, the call has no effect.
    //!
    //! \param record Pointer to the record object with the buffer and all other data about the message being received
    //! \param pos The index of the piece
    //! \param beginInput The beginning of the piece in the input buffer
    //! \param endInput The end of the piece in the input buffer
    //! \param beginOutput The position in the output buffer where the piece has to be written
    //!
    //! \throw nil doesn't throw
    void writeOnePiece(TreecastMessageRecordPtr record, size_t pos, const uint8_t* beginInput, size_t size, uint64_t destOffset);

    //! Used for propagating scatter messages down the tree in both small and large message modes
    //!
    //! In small message mode it sends the complete data down the tree, in large message mode only the
    //! data necessary for the other node to make his own propagate calls and initiate his own allgather message
    //!
    //! \param header The header of the scatter message we are propagating
    //! \param data The packet received
    //! \param blobOffset The offset into the packet where the header ends and the payload begins
    //! \param receivedSize The size of the payload received
    //! \param pieceSize The size of a single piece
    //! \param nodeCount The number of participating nodes
    //! \param rank The rank of the current node
    //! \param smallMessage Flag indicating if this is a small message or not
    //!
    //! \throw nil doesn't throw
    void propagateScatter(ScatterHeader & header, lunchbox::Bufferb const& data, uint64_t receivedSize,
        uint64_t pieceSize, size_t nodeCount, size_t rank, bool smallMessage);

    //! Used for initiating an allgather message around the ring of participating nodes.
    //!
    //! The message reaches every participating node (except for its initiator) exactly once as it travels the ring.
    //!
    //! \param header The header of the scatter message we are called from
    //! \param data The packet received
    //! \param blobOffset The offset into the packet where the header ends and the payload begins
    //! \param receivedSize The size of the payload received
    //! \param pieceSize The size of a single piece
    //! \param pieceNr The index of the piece being initiated, probably the same as the rank of the initiator
    //!
    //! \throw nil doesn't throw
    void initiateAllgather(ScatterHeader& header, lunchbox::Bufferb const& data, uint64_t receivedSize,
        uint64_t pieceSize, size_t pieceNr);

    //! Used for processing a large scatter command.
    //!
    //! This call is responsible for making every necessary step in the scatter phase in case of large messages.
    //!
    //! \param header The header of the scatter message received
    //! \param data The packet received
    //! \param blobOffset The offset into the packet where the header ends and the payload begins
    //!
    //! \throw nil doesn't throw
    void processScatterCommand(ScatterHeader& header, lunchbox::Bufferb const& data);

    //! Used for processing a small scatter command.
    //!
    //! This call is responsible for making every necessary step in the scatter phase in case of small messages.
    //!
    //! \param header The header of the scatter message received
    //! \param data The packet received
    //! \param blobOffset The offset into the packet where the header ends and the payload begins
    //!
    //! \throw nil doesn't throw
    void processSmallScatterCommand(ScatterHeader& header, lunchbox::Bufferb const& data);

    //! Used for processing an allgather command.
    //!
    //! This call is responsible for making every necessary step in the allgather phase about a single allgather call.
    //!
    //! \param header The header of the allgather message received
    //! \param data The packet received
    //! \param blobOffset The offset into the packet where the header ends and the payload begins
    //!
    //! \throw nil doesn't throw
    void processAllgatherCommand(AllgatherHeader const& header, lunchbox::Bufferb const& data);

private:
    TreecastConfig                                               _config;
    lunchbox::Lock                                               _configUpdateMutex;
    TreecastMessageRecordHandler                                 _messageRecordHandler;
    LocalNode*                                                   _localNode;
    DataTimeQueue                                                _dataTimeQueue;
    boost::asio::io_service                                      _io;
    boost::asio::deadline_timer                                  _pingTimer;
    boost::asio::deadline_timer                                  _resendTimer;
    lunchbox::a_int32_t                                          _needToStartTimers;
    std::deque<lunchbox::UUID>                                   _lastDispatchedMessages;
    std::set<NodeID>                                             _blackListNodes;
    detail::TimerThread*                                         _ioThread;
    lunchbox::Lock                                               _mutex;
    lunchbox::SpinLock                                           _spinLock;
    lunchbox::Monitor<bool>                                      _queueMonitor;
};
}
