#include "treecastMessageRecord.h"

namespace co {

TreecastMessageRecordPtr TreecastMessageRecord::create(size_t byteCount, size_t pieceCount,
                                                       std::vector<NodeID> const& inNodes)
{
    return TreecastMessageRecordPtr(
        new TreecastMessageRecord(byteCount, pieceCount, inNodes));
}

TreecastMessageRecord::TreecastMessageRecord(size_t byteCount, size_t pieceCount,
    std::vector<NodeID> const& inNodes)
: state(pieceCount)
, nodes(inNodes)
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
