
#pragma warning( disable: 4005 )
#include "treecastMessageRecordHandler.h"
#include "localNode.h"
#include "log.h"
#pragma warning( default: 4005 )

namespace co {

TreecastMessageRecordHandler::TreecastMessageRecordHandler()
{

}

co::TreecastMessageRecordPtr TreecastMessageRecordHandler::createOrUpdateMessageRecord(
    UUID const& messageId, uint32_t resendNr, std::vector<NodeID> const & nodes,
    size_t byteCount, size_t pieceCount)
{
    TreecastMessageRecordPtr record;
    lunchbox::ScopedWrite lock(m_mutex);
    MulticastMessageRecordMap_T::iterator messageIt = m_multicastMessageRecordMap.find(messageId);
    if (messageIt == m_multicastMessageRecordMap.end())
    {
        // The message is new to me
        m_multicastMessageRecordMap[messageId] = record =
            TreecastMessageRecord::create(byteCount, pieceCount, resendNr, nodes);
        LBLOG( LOG_TC ) << "CREATED new messageRecord: messageId: " << messageId << ", resendNr: " << resendNr
                  << ", byteCount: " << byteCount << ", pieceCount: " << pieceCount  << std::endl;
    }
    else
    {
        // The message is already known. This could be a re-send, or an allgather message arrived earlier
        // In either case, we should make sure that the stored metadata is up-to-date.
        record = messageIt->second;
        LBASSERT(typeId == record->typeId);
        LBASSERT(byteCount == record->buffer.getNumBytes());
        LBASSERT(pieceCount == record->state.size());
        if (record->resendNr < resendNr)
        {
            record->resendNr = resendNr;
        }
        else if (record->resendNr > resendNr)
        {
            LBWARN << "This is strange, I got a resend with a lower resendNr for a message. Skipping." << std::endl;
        }
    }
    return record;
}

TreecastMessageRecordPtr TreecastMessageRecordHandler::checkAndCleanUpMessageRecord(UUID const& messageId,
    uint32_t resendNr, bool doCheck)
{
    LBLOG( LOG_TC ) << "START cleaning up message record: messageId: " << messageId << ", resendNr: " << resendNr
              << ", doCheck: " << doCheck << std::endl;
    TreecastMessageRecordPtr record;
    {
        lunchbox::ScopedWrite lock(m_mutex);
        MulticastMessageRecordMap_T::iterator mapIt =
            m_multicastMessageRecordMap.find(messageId);
        if (m_multicastMessageRecordMap.end() == mapIt)
        {
            LBLOG( LOG_TC ) << "I just got a cleanup request on an unknown multicast message." << std::endl;
            return TreecastMessageRecordPtr();
        }
        // Let's check the resendNr
        record = mapIt->second;
        if (record->resendNr != resendNr)
        {
            LBLOG( LOG_TC ) << "Wrong resendNr on cleanup of a multicast message record, skipping." << std::endl;
            return TreecastMessageRecordPtr();
        }
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
        // Delete the record
        m_multicastMessageRecordMap.erase(mapIt);
        LBLOG( LOG_TC ) << "FINISHED cleaning up message record: messageId: " << messageId << ", resendNr: " << resendNr
                  << ", doCheck: " << doCheck << std::endl;
    }
    // Calculate rank
    const size_t rank = calculateRank(record->nodes);
    // Return the record, so that the user may do one last thing with it before it disappears
    return record;
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
