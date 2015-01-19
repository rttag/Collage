
/* Copyright (c) 2009-2013, Stefan Eilemann <eile@equalizergraphics.com>
 *                    2010, Daniel Nachbaur <danielnachbaur@gmail.com>
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

#ifndef CO_SERIALIZABLE_H
#define CO_SERIALIZABLE_H

#include <co/object.h>        // base class

namespace co
{
namespace detail { class Serializable; }

/**
 * Base class for distributed, inheritable objects.
 *
 * This class implements one usage pattern of Object, which allows subclassing
 * and serialization of distributed Objects. The implementation hierarchy
 * Serializable -> eq::fabric::Object -> eq::fabric::Frustum illustrates the
 * usage of this class.
 */
class CO_API_CL Serializable : public Object
{
public:
    /** @return the current dirty bit mask. @version 1.0 */
    CO_API uint64_t getDirty() const;

    /** @return true if the serializable has to be committed. @version 1.0 */
    CO_API virtual bool isDirty() const;

    /** @return true if the given dirty bit is set. @version 1.0 */
    CO_API virtual bool isDirty( const uint64_t dirtyBits ) const;

    /** @sa Object::commit() */
    CO_API virtual uint128_t commit( const uint32_t incarnation =
                                     CO_COMMIT_NEXT );

protected:
    /** Construct a new Serializable. @version 1.0 */
    CO_API Serializable();

    /**
     * Construct an unmapped, unregistered copy of a serializable.
     * @version 1.0
     */
    CO_API Serializable( const Serializable& );

    /** Destruct the serializable. @version 1.0 */
    CO_API virtual ~Serializable();

    /**
     * Worker for pack() and getInstanceData().
     *
     * Override this and deserialize() to distribute subclassed data.
     *
     * This method is called with DIRTY_ALL from getInstanceData() and with
     * the actual dirty bits from pack(), which also resets the dirty state
     * afterwards. The dirty bits are transmitted beforehand, and do not
     * need to be transmitted by the overriding method.
     * @version 1.0
     */
    virtual void serialize( co::DataOStream&, const uint64_t ){}

    /**
     * Worker for unpack() and applyInstanceData().
     *
     * This function is called with the dirty bits send by the master
     * instance. The dirty bits are received beforehand, and do not need to
     * be deserialized by the overriding method.
     *
     * @sa serialize()
     * @version 1.0
     */
    virtual void deserialize( co::DataIStream&, const uint64_t ){}

    /**
     * The changed parts of the serializable since the last pack().
     *
     * Subclasses should define their own bits, starting at DIRTY_CUSTOM.
     * @version 1.0
     */
    enum DirtyBits
    {
        DIRTY_NONE       = 0,
        DIRTY_CUSTOM     = 1,
        DIRTY_ALL        = 0xFFFFFFFFFFFFFFFFull
    };

    /** Add dirty flags to mark data for distribution. @version 1.0 */
    CO_API virtual void setDirty( const uint64_t bits );

    /** Remove dirty flags to clear data from distribution. @version 1.0 */
    CO_API virtual void unsetDirty( const uint64_t bits );

    /** @sa Object::getChangeType() */
    virtual ChangeType getChangeType() const { return DELTA; }

    /** @sa Object::notifyAttached() */
    CO_API virtual void notifyAttached();

private:
    detail::Serializable* const _impl;
    friend class detail::Serializable;

    virtual void getInstanceData( co::DataOStream& os )
        { serialize( os, DIRTY_ALL ); }

    CO_API virtual void applyInstanceData( co::DataIStream& is );

    CO_API virtual void pack( co::DataOStream& os );
    CO_API virtual void unpack( co::DataIStream& is );
};
}
#endif // CO_SERIALIZABLE_H
