/****
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2021. ALL RIGHTS RESERVED.
 * File name: securectype.h
 * Description:
 *             define internal used macro and data type. The marco of SECUREC_ON_64BITS
 *             will be determined in this header file, which is a switch for part
 *        ***************************************************************************     of code. Some macro are used to supress waining by MS complier.
 * Note:
 *             user can change the value of SECUREC_STRING_MAX_LEN and SECUREC_MEM_MAX_LEN
 *             macro to meet their specitial need.
 * History:
 *     1. Date: 2014/4/10
 *        Author: LiShunda
 *        Modification: add error detection macro. If pointer size of dest system is NOT
 *        4 bytes or 8 bytes, use #error "unsupported system..." to report the compiling error.
 *     2. Date:2014/5/16
 *        Author: LiShunda
 *        Modification: in HP UX system, UINT8T is defined, so add a macro(_hpux) to detect
 *                   whether on this system.
 *     3. Date:2014/6/3
 *        Author: LiShunda
 *        Modification: remove <limits.h>, for pclint will give a warning on including this file.
 *     4. Date:2014/6/10
 *        Author: LiShunda
 *        Modification: change uint8_t to UINT8T, which can avoid type redefinition.
 ******************************************************************************
 */

#ifndef __SECURECTYPE_H__A7BBB686_AADA_451B_B9F9_44DACDAE18A7
#define __SECURECTYPE_H__A7BBB686_AADA_451B_B9F9_44DACDAE18A7


/*Shielding VC symbol redefinition warning*/
#if defined(_MSC_VER) && (_MSC_VER >= 1400)
#ifdef __STDC_WANT_SECURE_LIB__
    #undef __STDC_WANT_SECURE_LIB__
#endif
    #define __STDC_WANT_SECURE_LIB__ 0
#ifdef _CRTIMP_ALTERNATIVE
    #undef _CRTIMP_ALTERNATIVE
#endif
    #define _CRTIMP_ALTERNATIVE //comment microsoft *_s function
#endif

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
/* #include <limits.h> this file is used to define some macros, such as  INT_MAX and SIZE_MAX */

#if (defined(_WIN32) || defined(_WIN64))
/* in windows platform, can't use optimized function for there is no __builtin_constant_p like function */

#ifdef WITH_PERFORMANCE_ADDONS
#undef WITH_PERFORMANCE_ADDONS
#endif
/* If need optimized macro, can define this: #define __builtin_constant_p(x) 1 */
#endif

#if defined(__GNUC__) && ((__GNUC__ > 3 || (__GNUC__ == 3 && __GNUC_MINOR__ > 3 /*above 3.4*/ ))  ) 
    long __builtin_expect(long exp, long c);
#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x) 
#endif

#ifndef TWO_MIN
#define TWO_MIN(a, b) ((a) < (b) ? (a) : (b))
#endif

#define WCHAR_SIZE sizeof(wchar_t)

/*ref //sourceforge.net/p/predef/wiki/OperatingSystems/ 
#if !(defined(__hpux) || defined(_AIX) || defined(__VXWORKS__) || defined(__vxworks) ||defined(__ANDROID__) || defined(__WRLINUX__)|| defined(_TYPE_uint8_t))
typedef unsigned char unit8_t;
#endif 
*/ 
typedef char INT8T;
typedef unsigned char UINT8T;

/* define the max length of the string */
#define SECUREC_STRING_MAX_LEN (0x7fffffffUL)
#define SECUREC_WCHAR_STRING_MAX_LEN (SECUREC_STRING_MAX_LEN / WCHAR_SIZE)

/* LSD SECUREC_MEM_MAX_LEN for memcpy and memmove*/
#define SECUREC_MEM_MAX_LEN (0x7fffffffUL)
#define SECUREC_WCHAR_MEM_MAX_LEN (SECUREC_MEM_MAX_LEN / WCHAR_SIZE)

#if SECUREC_STRING_MAX_LEN > 0x7fffffff
#error "max string is 2G, or you may remove this macro"
#endif

#define IN_REGISTER register

#if defined(WITH_PERFORMANCE_ADDONS) 
    /* for strncpy_s performance optimization */
#define STRNCPY_SM(dest, destMax, src, count) \
    ((NULL != (void*)dest && NULL != (void*)src && (size_t)destMax >0 && (size_t)destMax <= SECUREC_STRING_MAX_LEN) && (TWO_MIN(count , strlen(src)) + 1) <= (size_t)destMax ) ?  ( (count < strlen(src))? (memcpy(dest, src, count), *((char*)dest + count) = '\0', EOK) :( memcpy(dest, src, strlen(src) + 1), EOK ) ) :(strncpy_error(dest, destMax, src, count))  )

#define STRCPY_SM(dest, destMax, src) \
    (( NULL != (void*)dest && NULL != (void*)src  && (size_t)destMax >0 && (size_t)destMax <= SECUREC_STRING_MAX_LEN && ( strlen(src) + 1) <= (size_t)destMax )? (memcpy(dest, src, strlen(src) + 1), EOK) :( strcpy_error(dest, destMax, src)))

   /* for strcat_s performance optimization */
#if defined(__GNUC__)
#define STRCAT_SM(dest, destMax, src) \
    ({ int catRet =EOK;\
    if ((dest) != NULL && (src) != NULL && (size_t)(destMax) > 0 && (size_t)(destMax) <= SECUREC_STRING_MAX_LEN && (size_t)(destMax) <= SECUREC_STRING_MAX_LEN) {\
        char* pCatTmpDst = (dest);\
        size_t catRestSz = (destMax);\
        do{\
            while(catRestSz > 0 && *pCatTmpDst) {\
                ++pCatTmpDst;\
                --catRestSz;\
            }\
            if (catRestSz == 0) {\
                catRet = EINVAL;\
                break;\
            }\
            if ( ( strlen(src) + 1) <= catRestSz ) {\
                memcpy(pCatTmpDst, (src), strlen(src) + 1);\
                catRet = EOK;\
            }else{\
                catRet = ERANGE;\
            }\
        }while(0);\
        if ( EOK != catRet) catRet = strcat_s((dest), (destMax), (src));\
    }else{\
        catRet = strcat_s((dest), (destMax), (src));\
    }\
    catRet;})
#else
#define STRCAT_SM(dest, destMax, src) strcat_s(dest, destMax, src)
#endif

    /*for strncat_s performance optimization*/
#if defined(__GNUC__)
#define STRNCAT_SM(dest, destMax, src, count) \
    ({ int ncatRet = EOK;\
    if ((dest) != NULL && (src) != NULL && (size_t)destMax > 0 && (size_t)destMax <= SECUREC_STRING_MAX_LEN && (size_t)count <= SECUREC_STRING_MAX_LEN) {\
        char* pCatTmpDest = (dest);\
        size_t ncatRestSz = (destMax);\
        do{\
            while(ncatRestSz > 0 && *pCatTmpDest) {\
                ++pCatTmpDest;\
                --ncatRestSz;\
            }\
            if (ncatRestSz == 0) {\
                ncatRet = EINVAL;\
                break;\
            }\
            if ( (TWO_MIN((count) , strlen(src)) + 1) <= ncatRestSz ) {\
                if ((count) < strlen(src)) {\
                    memcpy(pCatTmpDest, (src), (count));\
                    *(pCatTmpDest + (count)) = '\0';\
                }else {\
                    memcpy(pCatTmpDest, (src), strlen(src) + 1);\
                }\
            }else{\
                ncatRet = ERANGE;\
            }\
        }while(0);\
        if ( EOK != ncatRet) ncatRet = strncat_s((dest), (destMax), (src), (count));\
    }else{\
        ncatRet = strncat_s((dest), (destMax), (src), (count));\
    }\
    ncatRet;})
#else
#define STRNCAT_SM(dest, destMax, src, count) strncat_s(dest, destMax, src, count)
#endif

    /*
    MEMCPY_SM do NOT check buffer overlap by default, or you can add this check to improve security
    condCheck = condCheck || (dest == src) || (dest > src && dest < (void*)((UINT8T*)src + count));\
    condCheck = condCheck || (src > dest && src < (void*)((UINT8T*)dest + count)); \
    */

#define  MEMCPY_SM(dest, destMax, src, count)\
    (!(((size_t)destMax== 0 )||((((UINT64T)(destMax) << 1 ) >> 1) > SECUREC_MEM_MAX_LEN)||((size_t)count > (size_t)destMax) || (NULL == (void*)dest) || (NULL == (void*)src))? (memcpy(dest, src, count), EOK) : (memcpy_s(dest, destMax, src, count)))

#define  MEMSET_SM(dest, destMax, c, count)\
    (!(((size_t)destMax == 0 ) || ((((UINT64T)(destMax) << 1 ) >> 1) > SECUREC_MEM_MAX_LEN) || (NULL == (void*)dest) || ((size_t)count > (size_t)destMax)) ? (memset(dest, c, count), EOK) : ( memset_s(dest, destMax, c, count)))

#endif /* WITH_PERFORMANCE_ADDONS */

#if defined(_MSC_VER) || defined(__ARMCC_VERSION)
typedef  __int64 INT64T;
typedef unsigned __int64 UINT64T;
#if defined(__ARMCC_VERSION)
typedef int INT32T;
typedef unsigned int UINT32T;
#else
typedef  __int32 INT32T;
typedef unsigned __int32 UINT32T;
#endif
#else
typedef int INT32T;
typedef unsigned int UINT32T;
typedef long long INT64T;
typedef unsigned long long UINT64T;
#endif

#ifdef _WIN64
#define SECUREC_ON_64BITS
#endif

#if defined(__LP64__) || defined(_LP64)
#define SECUREC_ON_64BITS
#endif

#if (defined(__GNUC__ ) && defined(__SIZEOF_POINTER__ ))
#if (__SIZEOF_POINTER__ != 4) && (__SIZEOF_POINTER__ != 8)
#error "unsupported system, contact Security Design Technology Department of 2012 Labs"
#endif
#endif

#if (!defined(SECUREC_ON_64BITS) && defined(__GNUC__ ) && defined(__SIZEOF_POINTER__ ))
#if __SIZEOF_POINTER__ == 8
#define SECUREC_ON_64BITS
#endif
#endif

#if (defined(__VXWORKS__) || defined(__vxworks) || defined(__VXWORKS))
#ifndef _VXWORKS_PLATFORM_
#define _VXWORKS_PLATFORM_
#endif
#endif

#if defined(__SVR4) || defined(__svr4__)
#define __SOLARIS
#endif

#if (defined(__hpux) || defined(_AIX) || defined(__SOLARIS))
#define __UNIX
#endif

/*if enable COMPATIBLE_LINUX_FORMAT, the output format will be compatible to Linux.*/
#if !(defined(_WIN32) || defined(_WIN64)|| defined(_VXWORKS_PLATFORM_))
#define COMPATIBLE_LINUX_FORMAT
#endif

#ifdef COMPATIBLE_LINUX_FORMAT
#include <stddef.h>
#include <stdint.h>
#endif

#ifdef _VXWORKS_PLATFORM_
#include <version.h>
#endif

#endif /*__SECURECTYPE_H__A7BBB686_AADA_451B_B9F9_44DACDAE18A7*/