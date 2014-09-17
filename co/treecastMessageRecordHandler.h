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
    TreecastMessageRecordPtr createOrUpdateMessageRecord( UUID const& messageId,
                             std::vector<NodeID> const & nodes, size_t byteCount, size_t pieceCount );
    TreecastMessageRecordPtr checkAndCleanUpMessageRecord( UUID const& messageId, bool doCheck );
    TreecastMessageRecordPtr getRecordByID( UUID const& messageId ) ;
    size_t calculateRank( std::vector<NodeID> const& nodes ) const;
    size_t getParentRank( size_t rank ) const;
    void getChildNodes( std::vector<NodeID> const& nodes, std::vector<NodeID>& childNodes );
    void deleteRecord( UUID const& messageId );
    //void updateRecordAckedNodes( UUID const& messageId, NodeID childId );

    void setLocalNode( LocalNode* localNode ) { m_localNode = localNode;}
  private:
    lunchbox::SpinLock                                            m_SpinLock;
    LocalNode*                                                    m_localNode;
    typedef stde::hash_map<UUID, TreecastMessageRecordPtr> TreecastMessageRecordMap_T;
    typedef TreecastMessageRecordMap_T::iterator          MulticastMessageRecordMap_TIt;
    TreecastMessageRecordMap_T                                   m_treecastMessageRecordMap;
};
}
