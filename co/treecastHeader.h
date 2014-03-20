#pragma once

#include "types.h"
#include "dataIStream.h"
#include "dataOStream.h"

#include <vector>

namespace co {

class TreecastHeader
{
public:
    uint64_t            byteCount;      //<! The full size of the blob
    UUID                messageId;      //<! The unique id for identifying this message
    uint32_t            resendNr;       //<! If the message is re-sent, the user has to increment this number
    std::vector<NodeID> nodes;          //<! The list of nodes participating in the communication
    
    // not serialized
    enum NodeCommand    cmd;

    virtual void serialize( DataOStream& os ) const
    {
        os << byteCount << messageId << resendNr << nodes;
    }

    virtual void deserialize( DataIStream& is )
    {
        is >> byteCount >> messageId >> resendNr >> nodes;
    }

    virtual void byteswap()
    {
#ifdef COLLAGE_BIGENDIAN
        lunchbox::byteswap( byteCount );
        lunchbox::byteswap( messageId );
        lunchbox::byteswap( resendNr );
        lunchbox::byteswap( nodes );
#endif
    }
};


//! ScatterHeader is the header used in multicast for the messages in the scatter step
typedef TreecastHeader ScatterHeader;
//! AllgatherHeader is the header used in multicast for the messages in the allgather step
class AllgatherHeader : public TreecastHeader
{
public:
    size_t              pieceNr;    //<! Indicates which piece of the buffer this is

    virtual void serialize( DataOStream& os ) const
    {
        TreecastHeader::serialize(os);
        os << pieceNr;
    }

    virtual void deserialize( DataIStream& is )
    {
        TreecastHeader::deserialize(is);
        is >> pieceNr;
    }
    virtual void byteswap()
    {
        TreecastHeader::byteswap();
#ifdef COLLAGE_BIGENDIAN
        lunchbox::byteswap( pieceNr );
#endif
    }
};

DataOStream& operator<<( DataOStream& lhs, TreecastHeader const& rhs )
{
    rhs.serialize( lhs );
    return lhs;
}

DataIStream& operator>>( DataIStream& lhs, TreecastHeader& rhs )
{
    rhs.deserialize( lhs );
    return lhs;
}

DataOStream& operator<<( DataOStream& lhs, AllgatherHeader const& rhs )
{
    rhs.serialize( lhs );
    return lhs;
}

DataIStream& operator>>( DataIStream& lhs, AllgatherHeader& rhs )
{
    rhs.deserialize( lhs );
    return lhs;
}

}

namespace lunchbox
{
    template<> inline void byteswap( co::ScatterHeader& header ) { header.byteswap(); }
    template<> inline void byteswap( co::AllgatherHeader& header ) { header.byteswap(); }
}
