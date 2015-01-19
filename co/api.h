
/* Copyright (c) 2010, Daniel Pfeifer <daniel@pfeifer-mail.de>
 *               2010-2012, Stefan Eilemann <eile@eyescale.ch>
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

#ifndef CO_API_H
#define CO_API_H

#include <co/defines.h>

#if defined(_MSC_VER) || defined(__declspec)
# define CO_DLLEXPORT LB_DLLEXPORT
# define CO_DLLIMPORT LB_DLLIMPORT
#elif __GNUC__ >= 4
#  define CO_DLLEXPORT __attribute__ ((visibility ("default"))) 
#  define CO_DLLIMPORT __attribute__ ((visibility ("default")))
#else 
#  define CO_DLLEXPORT
#  define CO_DLLIMPORT
#endif

#if defined(COLLAGE_STATIC)
#  define CO_API
#elif defined(COLLAGE_SHARED)
#  define CO_API CO_DLLEXPORT
#else
#  define CO_API CO_DLLIMPORT
#endif

#if __GNUC__ >= 4
#  define CO_API_CL CO_API
#else
#  define CO_API_CL
#endif

#endif //CO_API_H
