
#pragma warning( disable: 4005 )
#include "treecastMessageRecordHandler.h"
#include "localNode.h"
#include "log.h"
#pragma warning( default: 4005 )

namespace co {

TreecastMessageRecordHandler::TreecastMessageRecordHandler()
{

}

void TreecastMessageRecordHandler::deleteRecord( UUID const& messageId )
{
    lunchbox::ScopedFastWrite lock( m_SpinLock );
    TreecastMessageRecordMap_T::iterator it = m_treecastMessageRecordMap.find( messageId );
    if( it != m_treecastMessageRecordMap.end() )
        m_treecastMessageRecordMap.erase(it);
    else
        LBLOG( LOG_TC ) << "ERROR: Wasn't able to find messageId: "<< messageId <<" in my map";
}

co::TreecastMessageRecordPtr TreecastMessageRecordHandler::getRecordByID( UUID const& messageId )
{
    lunchbox::ScopedFastWrite lock( m_SpinLock );
    TreecastMessageRecordMap_T::iterator it = m_treecastMessageRecordMap.find( messageId );
    if( it != m_treecastMessageRecordMap.end() )
    {
        return it->second;
    }
    return TreecastMessageRecordPtr();
}

co::TreecastMessageRecordPtr TreecastMessageRecordHandler::createOrUpdateMessageRecord(
    UUID const& messageId, std::vector<NodeID> const & nodes, size_t byteCount, size_t pieceCount )
{
    lunchbox::ScopedFastWrite lock(m_SpinLock);
    TreecastMessageRecordPtr record;
    
    TreecastMessageRecordMap_T::iterator messageIt = m_treecastMessageRecordMap.find(messageId);
    if (messageIt == m_treecastMessageRecordMap.end() 
        ||    nodes != messageIt->second->nodes ) 
    {
        // The message is new to me
        m_treecastMessageRecordMap[messageId] = record =
            TreecastMessageRecord::create(byteCount, pieceCount, nodes);
        LBLOG( LOG_TC ) << "CREATED new messageRecord: messageId: " << messageId << ", byteCount: " << byteCount
            << ", pieceCount: " << pieceCount  << std::endl;
    }
    else
    {
        // The message is already known. This could be a re-send, or an allgather message arrived earlier
        // In either case, we should make sure that the stored metadata is up-to-date.
        record = messageIt->second;
        
        LBASSERT(byteCount == record->buffer.getNumBytes());
        LBASSERT(pieceCount == record->state.size());
    }
    return record;
}

TreecastMessageRecordPtr TreecastMessageRecordHandler::checkAndCleanUpMessageRecord(UUID const& messageId,
     bool doCheck)
{
    LBLOG( LOG_TC ) << "START cleaning up message record: messageId: " << messageId << ", doCheck: " << doCheck << std::endl;
    lunchbox::ScopedFastWrite lock( m_SpinLock );
    TreecastMessageRecordMap_T::iterator mapIt =
        m_treecastMessageRecordMap.find(messageId);
    if (m_treecastMessageRecordMap.end() == mapIt)
    {
        LBLOG( LOG_TC ) << "I just got a cleanup request on an unknown multicast message." << std::endl;
        return TreecastMessageRecordPtr();
    }
    // Let's check the resendNr
    TreecastMessageRecordPtr record = mapIt->second;
    if (doCheck)
    {
        // Let's see if we have all pieces
        size_t pieceCount = record->state.size();
        size_t pos;
        for (pos = 0; pos < pieceCount; ++pos)
        {
            if (record->state[pos] != TREECAST_PIECE_STATE_FULL)
            {
                // This piece is still missing, we are not finished yet
                return TreecastMessageRecordPtr();
            }
        }
    }
    LBLOG( LOG_TC ) << "FINISHED cleaning up message record: messageId: " << messageId << ", doCheck: " << doCheck << std::endl;

    return record;
}

size_t TreecastMessageRecordHandler::getParentRank( size_t rank ) const
{
    //root doesn't have any children
    if( rank == 0 )
        return 0;
    size_t cur_parent = 0;
    while( true )
    {
        size_t low_rank = 0x1;
        size_t high_rank = low_rank<<1;

        for( ;!(low_rank <= rank && high_rank> rank); low_rank <<=1, high_rank <<=1  );

        if( low_rank >= rank )
            break;
        else
        {
            cur_parent += low_rank;
            rank -= low_rank;
        }
    }

    return cur_parent;
}

void TreecastMessageRecordHandler::getChildNodes( std::vector<NodeID> const& nodes, std::vector<NodeID>& childNodes )
{
    const size_t rank = calculateRank( nodes );
    const size_t parent = getParentRank( rank );
    size_t high_rank = (rank != 0)?( 2*rank - parent  ):( nodes.size() );
    high_rank = std::min( high_rank, nodes.size() );
    size_t next_child_rank = 0x1;
    while ( rank + next_child_rank < high_rank )
    {
        size_t childIdx = rank + next_child_rank;
        childNodes.push_back( nodes[childIdx] );
        next_child_rank <<= 1;
    }
}


size_t TreecastMessageRecordHandler::calculateRank(std::vector<NodeID> const& nodes) const
{
    NodeID myNodeId = m_localNode->getNodeID();
    size_t rank = 0;
    if (nodes[0] != myNodeId)
    {
        // If I am not the root node, check who I am
        std::vector<NodeID>::const_iterator it = std::lower_bound(nodes.begin()+1, nodes.end(), myNodeId);
        if (nodes.end() == it || *it != myNodeId)
        {
            LBUNREACHABLE;
        }
        rank = it - nodes.begin();
    }

    return rank;
}
}
