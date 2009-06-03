
/* Copyright (c) 2005-2009, Stefan Eilemann <eile@equalizergraphics.com> 
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

#ifndef EQNET_PIPE_CONNECTION_H
#define EQNET_PIPE_CONNECTION_H

#ifdef WIN32
#  include <eq/net/connection.h>
#else
#  include <eq/net/fdConnection.h>
#endif

#include <eq/base/thread.h>

namespace eq
{
namespace net
{
    /**
     * A uni-directional pipe connection.
     */
    class PipeConnection 
#ifdef WIN32
        : public Connection
#else
        : public FDConnection
#endif
    {
    public:
        EQ_EXPORT PipeConnection();
        EQ_EXPORT virtual ~PipeConnection();

        virtual bool connect();
        virtual void close();

#ifdef WIN32
        virtual Notifier getNotifier() const { return _dataPending; }
        bool hasData() const 
            { return WaitForSingleObject( _dataPending, 0 ) == WAIT_OBJECT_0; }
#endif

    protected:
        PipeConnection( const PipeConnection& conn );

#ifdef WIN32
        virtual void readNB( void* buffer, const uint64_t bytes );
        virtual int64_t readSync( void* buffer, const uint64_t bytes );
        virtual int64_t write( const void* buffer, const uint64_t bytes ) const;
#endif

    private:
        bool _createPipe();

#ifdef WIN32
        HANDLE _readHandle;
        HANDLE _writeHandle;
        mutable base::Lock _mutex;
        mutable uint64_t   _size;
        mutable HANDLE     _dataPending;
#endif
    };
}
}

#endif //EQNET_PIPE_CONNECTION_H
