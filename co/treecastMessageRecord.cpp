#include "treecastMessageRecord.h"

namespace co {

TreecastMessageRecordPtr TreecastMessageRecord::create(size_t byteCount, size_t pieceCount,
                                                       std::vector<NodeID> const& nodes)
{
    return TreecastMessageRecordPtr(
        new TreecastMessageRecord(byteCount, pieceCount, nodes));
}

TreecastMessageRecord::TreecastMessageRecord(size_t byteCount, size_t pieceCount,
    std::vector<NodeID> const& nodes)
: state(pieceCount)
, nodes(nodes)
{
    buffer.reset(byteCount);
}

bool TreecastMessageRecord::isFullyAcknowledged( std::vector<NodeID> const& childNodes ) 
{
    return ( ackNodes.size() == childNodes.size() ) &&
           std::equal( childNodes.begin(), childNodes.end(), ackNodes.begin() );
}


TreecastMessageRecord::~TreecastMessageRecord()
{
}
}
