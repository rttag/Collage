
/* Copyright (c) 2007-2013, Stefan Eilemann <eile@equalizergraphics.com>
 *                    2010, Cedric Stalder  <cedric.stalder@gmail.com>
 *                    2012, Daniel Nachbaur <danielnachbaur@gmail.com>
 *
 * This file is part of Collage <https://github.com/Eyescale/Collage>
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License version 2.1 as published
 * by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

#include "objectInstanceDataOStream.h"

#include "log.h"
#include "nodeCommand.h"
#include "object.h"
#include "objectDataIStream.h"
#include "objectDataOCommand.h"
#include "versionedMasterCM.h"
#include "treecast.h"
#include "connections.h"


namespace co
{
ObjectInstanceDataOStream::ObjectInstanceDataOStream( const ObjectCM* cm )
        : ObjectDataOStream( cm )
        , _instanceID( EQ_INSTANCE_NONE )
        , _command( 0 )
{}

ObjectInstanceDataOStream::~ObjectInstanceDataOStream()
{}

void ObjectInstanceDataOStream::reset()
{
    ObjectDataOStream::reset();
    _nodeID = 0;
    _instanceID = EQ_INSTANCE_NONE;
    _command = 0;
}

void ObjectInstanceDataOStream::enableCommit( const uint128_t& version,
                                              const Nodes& receivers )
{
    _command = CMD_NODE_OBJECT_INSTANCE_COMMIT;
    _nodeID = 0;
    _instanceID = EQ_INSTANCE_NONE;
    ObjectDataOStream::enableCommit( version, receivers );
}

void ObjectInstanceDataOStream::enablePush( const uint128_t& version,
                                            const Nodes& receivers )
{
    _command = CMD_NODE_OBJECT_INSTANCE_PUSH;
    _nodeID = 0;
    _instanceID = EQ_INSTANCE_NONE;
    ObjectDataOStream::enableCommit( version, receivers );
}

void ObjectInstanceDataOStream::push( const Nodes& receivers,
                                      const uint128_t& objectID,
                                      const uint128_t& groupID,
                                      const uint128_t& typeID,
                                      LocalNodePtr localNode)
{
    _command = CMD_NODE_OBJECT_INSTANCE_PUSH;
    _nodeID = 0;
    _instanceID = EQ_INSTANCE_NONE;

    bool treecast = useTreecast( receivers );
    if ( treecast )
    {
        lunchbox::Bufferb data;
        buildTreecastBuffer( data );
        _clearConnections();
        localNode->treecast( data, receivers );
    }

    _setupConnections( receivers );

    if (!treecast)
        _resend();

    OCommand( getConnections(), CMD_NODE_OBJECT_PUSH )
        << objectID << groupID << typeID;

    _clearConnections();
}

void ObjectInstanceDataOStream::pushMap( const Nodes& receivers,
                                         const uint128_t& objectID,
                                         const uint128_t& groupID,
                                         const uint128_t& typeID,
                                         const uint128_t& version,
                                         const uint32_t instanceID,
                                         const Object::ChangeType changeType,
                                         LocalNodePtr localNode )
{
    _command = CMD_NODE_OBJECT_INSTANCE_PUSH;
    _nodeID = 0;
    _instanceID = EQ_INSTANCE_NONE;
    
    bool treecast = useTreecast( receivers );
    if ( treecast )
    {
        lunchbox::Bufferb data;
        buildTreecastBuffer( data );
        _clearConnections();
        localNode->treecast( data, receivers );
    }
    
    _setupConnections( receivers );

    if (!treecast)
        _resend();

    OCommand( getConnections(), CMD_NODE_OBJECT_PUSH_MAP )
        << objectID << groupID << typeID << version << instanceID << changeType;

    _clearConnections();
}

void ObjectInstanceDataOStream::sendInstanceData( const Nodes& receivers )
{
    _command = CMD_NODE_OBJECT_INSTANCE;
    _nodeID = 0;
    _instanceID = EQ_INSTANCE_NONE;
    _setupConnections( receivers );
    _resend();
    _clearConnections();
}

void ObjectInstanceDataOStream::sendMapData( NodePtr node,
                                             const uint32_t instanceID )
{
    _command = CMD_NODE_OBJECT_INSTANCE_MAP;
    _nodeID = node->getNodeID();
    _instanceID = instanceID;
    bool mc = Global::getIAttribute( Global::IATTR_MULTICAST_MAPS ) == 1;
    _setupConnection( node, mc );
    _resend();
    _clearConnections();
}

void ObjectInstanceDataOStream::enableMap( const uint128_t& version,
                                           NodePtr node,
                                           const uint32_t instanceID )
{
    _command = CMD_NODE_OBJECT_INSTANCE_MAP;
    _nodeID = node->getNodeID();
    _instanceID = instanceID;
    _version = version;
    bool mc = Global::getIAttribute( Global::IATTR_MULTICAST_MAPS ) == 1;
    _setupConnection( node, mc );
    _enable();
}

void ObjectInstanceDataOStream::sendData( const void* buffer,
                                          const uint64_t size, const bool last )
{
    LBASSERT( _command );
    send( _command, COMMANDTYPE_NODE, _instanceID, size, last )
        << _nodeID << _cm->getObject()->getInstanceID();
}

void ObjectInstanceDataOStream::_buildTreecastBuffer( lunchbox::Bufferb& buf,
                                                      const uint64_t offset )
{
    LBASSERT( _command );
    LBASSERT( _version != VERSION_INVALID );
    _sequence = 0;

    ObjectDataOCommand odc( Connections(), _command, COMMANDTYPE_NODE,
                            _cm->getObject()->getID(), _instanceID, _version, 0,
                             getBuffer().getNumBytes(), true, this );
    odc << _nodeID << _cm->getObject()->getInstanceID();
    buf.append(odc.getBuffer().getData(), odc.getBuffer().getNumBytes());
    
    uint64_t cmdsize = odc.getBuffer().getNumBytes();
    if ( getCompressedDataSize() == 0 && getBuffer().getNumBytes() )
    {
        cmdsize += getBuffer().getNumBytes() - offset;
        buf.append( getBuffer().getData() + offset, 
            getBuffer().getNumBytes() - offset );
    }
    else
    {
        cmdsize = copyCompressedDataToBuffer( buf );
    }
    reinterpret_cast< uint64_t* >( buf.getData() )[ 0 ] = cmdsize;
}
}
