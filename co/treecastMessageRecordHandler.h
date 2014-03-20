#pragma once

#include "treecastMessageRecord.h"
#include "types.h"

#include <lunchbox/lock.h>

#include <vector>

namespace co {

//! Class taking care of the incoming message records in Multicast
class TreecastMessageRecordHandler
{
  public:
    TreecastMessageRecordHandler( );
    TreecastMessageRecordPtr createOrUpdateMessageRecord(UUID const& messageId,
        uint32_t resendNr, std::vector<NodeID> const & nodes, size_t byteCount, size_t pieceCount);
    TreecastMessageRecordPtr checkAndCleanUpMessageRecord(UUID const& messageId, uint32_t resendNr, bool doCheck);
    size_t calculateRank(std::vector<NodeID> const& nodes) const;
    void setLocalNode(LocalNode* localNode) { m_localNode = localNode;}
  private:
    lunchbox::Lock                                                m_mutex;
    LocalNode*                                                    m_localNode;
    typedef stde::hash_map<UUID, TreecastMessageRecordPtr> MulticastMessageRecordMap_T;
    MulticastMessageRecordMap_T                                   m_multicastMessageRecordMap;
};
}
