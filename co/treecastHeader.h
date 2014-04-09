#pragma once

#include "types.h"
#include "dataIStream.h"
#include "dataOStream.h"

#include <vector>

namespace co {

class TreecastHeader 
{
public:
    UUID                 messageId;   //<<! an ID
    std::vector<NodeID>  nodes;       //<<! list of nodes that take part in the communication
    

    virtual void serialize( DataOStream& os ) const 
    {
        os << messageId << nodes;
    }

    virtual void deserialize( DataIStream& is ) 
    {
        is >> messageId >> nodes;
    }

    virtual void byteswap()
    {
#ifdef COLLAGE_BIGENDIAN
        lunchbox::byteswap( messageId );
        lunchbox::byteswap( nodes );
#endif
    }
};

class ScatterHeader: public TreecastHeader
{
public:
    uint64_t            byteCount;      //<! The full size of the blob
    
    // not serialized
    enum NodeCommand    cmd;

    virtual void serialize( DataOStream& os ) const
    {
        TreecastHeader::serialize( os );
        os << byteCount;
    }

    virtual void deserialize( DataIStream& is )
    {
        TreecastHeader::deserialize( is );
        is >> byteCount;
    }

    virtual void byteswap()
    {
#ifdef COLLAGE_BIGENDIAN
        TreecastHeader::byteswap();
        lunchbox::byteswap( byteCount );
#endif
    }
};


//! ScatterHeader is the header used in multicast for the messages in the scatter step
//typedef TreecastHeader ScatterHeader;
//! AllgatherHeader is the header used in multicast for the messages in the allgather step
class AllgatherHeader : public ScatterHeader
{
public:
    size_t              pieceNr;        //<! Indicates which piece of the buffer this is

    virtual void serialize( DataOStream& os ) const
    {
        ScatterHeader::serialize(os);
        os << pieceNr;
    }

    virtual void deserialize( DataIStream& is )
    {
        ScatterHeader::deserialize(is);
        is >> pieceNr;
    }
    virtual void byteswap()
    {
        ScatterHeader::byteswap();
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

DataOStream& operator<<( DataOStream& lhs, ScatterHeader const& rhs )
{
    rhs.serialize( lhs );
    return lhs;
}

DataIStream& operator>>( DataIStream& lhs, ScatterHeader& rhs )
{
    rhs.deserialize( lhs );
    return lhs;
}

}

namespace lunchbox
{
    template<> inline void byteswap( co::ScatterHeader& header ) { header.byteswap(); }
    template<> inline void byteswap( co::AllgatherHeader& header ) { header.byteswap(); }
    template<> inline void byteswap( co::TreecastHeader& header ) { header.byteswap(); }
}
