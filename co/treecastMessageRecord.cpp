#include "treecastMessageRecord.h"

namespace co {

TreecastMessageRecordPtr TreecastMessageRecord::create(size_t byteCount, size_t pieceCount,
    uint32_t resendNr, std::vector<NodeID> const& nodes)
{
    return TreecastMessageRecordPtr(
        new TreecastMessageRecord(byteCount, pieceCount, resendNr, nodes));
}

TreecastMessageRecord::TreecastMessageRecord(size_t byteCount, size_t pieceCount,
    uint32_t resendNr, std::vector<NodeID> const& nodes)
: state(pieceCount)
, resendNr(resendNr)
, nodes(nodes)
{
    buffer.reset(byteCount);
}

TreecastMessageRecord::~TreecastMessageRecord()
{
}
}
