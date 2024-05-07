/******************************************************************************
 *                               DEBUG UTILITY                                *
 *                                                                            *
 *   The Ddebug Utility provides trace methods for debug purposed, not        *
 * present on a release build.                                                *
 ******************************************************************************/

#ifndef COMMON_DEBUG_H
#define COMMON_DEBUG_H

#include "common/error.h"

#define UNUSED(x) (void)(x)

#ifdef DEBUG
    #define DEBUG_PRINT(...) do{ fprintf( stderr, __VA_ARGS__ ); } while( 0 )
#else
    #define DEBUG_PRINT(...) do{ } while ( 0 )
#endif

#endif