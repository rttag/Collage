
/* Copyright (c) 2007-2012, Stefan Eilemann <eile@equalizergraphics.com>
 *                    2010, Cedric Stalder <cedric.stalder@gmail.com>
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

#include "deltaMasterCM.h"

#include "log.h"
#include "node.h"
#include "object.h"
#include "objectDataIStream.h"
#include "global.h"
#include "connections.h"

namespace co
{

DeltaMasterCM::DeltaMasterCM( Object* object )
        : FullMasterCM( object )
#pragma warning(push)
#pragma warning(disable : 4355)
        , _deltaData( this )
#pragma warning(pop)
{}

DeltaMasterCM::~DeltaMasterCM()
{}

void DeltaMasterCM::_commit()
{
    uint32_t old = Global::getObjectBufferSize();
    bool treecast = useTreecast( *_slaves );
    if( !_slaves->empty( ))
    {
        _deltaData.reset();
        _deltaData.enableCommit( _version + 1, *_slaves );
        if ( treecast )
            Global::setObjectBufferSize( ~0u );
        _object->pack( _deltaData );
        if ( treecast )
        {
            _deltaData.treecastDisable( *_slaves, _object->getLocalNode());
            Global::setObjectBufferSize( old );
        }
        else
            _deltaData.disable();
    }

    if( _slaves->empty() || _deltaData.hasSentData( ))
    {
        // save instance data
        InstanceData* instanceData = _newInstanceData();

        uint32_t old = Global::getObjectBufferSize();
        instanceData->os.enableCommit( _version + 1, Nodes( ));
        if ( treecast )
            Global::setObjectBufferSize( ~0u );

        _object->getInstanceData( instanceData->os );
        if ( treecast )
        {
            instanceData->os.treecastDisable( *_slaves, _object->getLocalNode());
            Global::setObjectBufferSize( old );
        }
        else
            instanceData->os.disable();

        if( _deltaData.hasSentData() || instanceData->os.hasSentData( ))
        {
            ++_version;
            LBASSERT( _version != VERSION_NONE );

            _addInstanceData( instanceData );
        }
        else
            _releaseInstanceData( instanceData );

#if 0
        LBLOG( LOG_OBJECTS ) << "Committed v" << _version << " " << *_object
                             << std::endl;
#endif
    }
}

}
