#pragma once

#include "types.h"

#include <lunchbox/refptr.h>
#include <lunchbox/referenced.h>
#include <vector>
#include <set>

namespace co {
enum TreecastState {
    TREECAST_PIECE_STATE_EMPTY = 0,
    TREECAST_PIECE_STATE_WRITING = 1,
    TREECAST_PIECE_STATE_FULL = 2
};
struct TreecastMessageRecord;
typedef lunchbox::RefPtr<TreecastMessageRecord> TreecastMessageRecordPtr;

//! Class representing an incoming multicast message
struct TreecastMessageRecord : public lunchbox::Referenced
{
  public:
    static TreecastMessageRecordPtr create(size_t byteCount, size_t pieceCount, std::vector<NodeID> const& nodes);
    TreecastMessageRecord(size_t byteCount, size_t pieceCount, std::vector<NodeID> const& nodes);
    ~TreecastMessageRecord();
    bool isFullyAcknowledged( std::vector<NodeID> const& childNodes );
    lunchbox::Bufferb                  buffer;     //<! The buffer that stores the received pieces
    std::vector<lunchbox::a_int32_t>   state;      //<! Marks which parts of the message arrived already
    std::vector<NodeID>                nodes;      //<! The nodes participating in the communication
    std::set<NodeID>                   ackNodes;   //<! Nodes that have acknowledged this message
};
}
