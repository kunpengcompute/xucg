/*******************************************************************************
* Copyright @ Huawei Technologies Co., Ltd. 1998-2014. All rights reserved.
* File name: memcpy_s.c
* History:
*     1. Date:
*         Author:
*         Modification:
********************************************************************************
*/

#include <ucg/secure/include/securec.h>
#include "securecutil.h"

/*******************************************************************************
* <NAME>
*    memcpy_s
*
* <SYNOPSIS>
*    errno_t memcpy_s(void *dest, size_t destMax, const void *src, size_t count);
*
* <FUNCTION DESCRIPTION>
*    memcpy_s copies count bytes from src to dest
*
* <INPUT PARAMETERS>
*    dest                       new buffer.
*    destMax                    Size of the destination buffer.
*    src                        Buffer to copy from.
*    count                      Number of characters to copy
*
* <OUTPUT PARAMETERS>
*    dest buffer                is updated.
*
* <RETURN VALUE>
*    EOK                        Success
*    EINVAL                     dest == NULL or strSrc == NULL
*    ERANGE                     count > destMax or destMax >
*                               SECUREC_MEM_MAX_LEN or destMax == 0
*    EOVERLAP_AND_RESET         dest buffer and source buffer are overlapped
*
*    if an error occured, dest will be filled with 0.
*    If the source and destination overlap, the behavior of memcpy_s is undefined.
*    Use memmove_s to handle overlapping regions.
*******************************************************************************
*/

/* assembly language memcpy  */
extern void* memcpy_opt(void* dest, const void* src, size_t n);

errno_t memcpy_s(void* dest, size_t destMax, const void* src, size_t count)
{
    if (destMax == 0 || destMax > SECUREC_MEM_MAX_LEN) {
        SECUREC_ERROR_INVALID_RANGE("memcpy_s");
        return ERANGE;
    }
    if (dest == NULL || src == NULL) {
        SECUREC_ERROR_INVALID_PARAMTER("memcpy_s");
        if (dest != NULL) {
            (void)memset(dest, 0, destMax);
            return EINVAL_AND_RESET;
        }
        return EINVAL;
    }
    if (count > destMax) {
        (void)memset(dest, 0, destMax);
        SECUREC_ERROR_INVALID_RANGE("memcpy_s");
        return ERANGE_AND_RESET;
    }
    if (dest == src) {
        return EOK;
    }
    if ((dest > src && dest < (void *)((UINT8T*)src + count)) ||
        (src > dest && src < (void *)((UINT8T*)dest + count)) )
    {
        (void)memset(dest, 0, destMax);
        SECUREC_ERROR_BUFFER_OVERLAP("memcpy_s");
        return ERANGE_AND_RESET;
    }
    (void)memcpy(dest, src, count);
    return EOK;
}


#if defined(WITH_PERFORMANCE_ADDONS)

errno_t memcpy_sOptAsm(void* dest, size_t destMax, const void* src, size_t count)
{
    if (LIKELY( count <= destMax && dest && src   /*&& dest != src*/
        && destMax <= SECUREC_MEM_MAX_LEN
        && count > 0
        && ( (dest > src  &&  (void*)((UINT8T*)src  + count) <= dest) ||
        (src  > dest &&  (void*)((UINT8T*)dest + count) <= src) )
        ) )
    {
        if (count > 32)
        {
            /*large enough, let system API do it*/
#ifdef USE_ASM
            memcpy_opt(dest, src, count);
#else
            (void)memcpy(dest, src, count);
#endif
            return EOK;
        }
        else
        {
            switch (count)
            {
                case 1:*(MY_STR1 *)dest=*(MY_STR1 *)src;break;
                case 2:*(MY_STR2 *)dest=*(MY_STR2 *)src;break;
                case 3:*(MY_STR3 *)dest=*(MY_STR3 *)src;break;
                case 4:*(MY_STR4 *)dest=*(MY_STR4 *)src;break;
                case 5:*(MY_STR5 *)dest=*(MY_STR5 *)src;break;
                case 6:*(MY_STR6 *)dest=*(MY_STR6 *)src;break;
                case 7:*(MY_STR7 *)dest=*(MY_STR7 *)src;break;
                case 8:*(MY_STR8 *)dest=*(MY_STR8 *)src;break;
                case 9:*(MY_STR9 *)dest=*(MY_STR9 *)src;break;
                case 10:*(MY_STR10 *)dest=*(MY_STR10 *)src;break;
                case 11:*(MY_STR11 *)dest=*(MY_STR11 *)src;break;
                case 12:*(MY_STR12 *)dest=*(MY_STR12 *)src;break;
                case 13:*(MY_STR13 *)dest=*(MY_STR13 *)src;break;
                case 14:*(MY_STR14 *)dest=*(MY_STR14 *)src;break;
                case 15:*(MY_STR15 *)dest=*(MY_STR15 *)src;break;
                case 16:*(MY_STR16 *)dest=*(MY_STR16 *)src;break;
                case 17:*(MY_STR17 *)dest=*(MY_STR17 *)src;break;
                case 18:*(MY_STR18 *)dest=*(MY_STR18 *)src;break;
                case 19:*(MY_STR19 *)dest=*(MY_STR19 *)src;break;
                case 20:*(MY_STR20 *)dest=*(MY_STR20 *)src;break;
                case 21:*(MY_STR21 *)dest=*(MY_STR21 *)src;break;
                case 22:*(MY_STR22 *)dest=*(MY_STR22 *)src;break;
                case 23:*(MY_STR23 *)dest=*(MY_STR23 *)src;break;
                case 24:*(MY_STR24*)dest=*(MY_STR24 *)src;break;
                case 25:*(MY_STR25 *)dest=*(MY_STR25 *)src;break;
                case 26:*(MY_STR26 *)dest=*(MY_STR26 *)src;break;
                case 27:*(MY_STR27 *)dest=*(MY_STR27 *)src;break;
                case 28:*(MY_STR28 *)dest=*(MY_STR28 *)src;break;
                case 29:*(MY_STR29 *)dest=*(MY_STR29 *)src;break;
                case 30:*(MY_STR30 *)dest=*(MY_STR30 *)src;break;
                case 31:*(MY_STR31 *)dest=*(MY_STR31 *)src;break;
                case 32:*(MY_STR32 *)dest=*(MY_STR32 *)src;break;
            }
            return EOK;
        }
    } else {
        /* call it only to return error code */
        return memcpy_s(dest, destMax, src, count);
    }
}

/*trim judgement on "destMax <= SECUREC_MEM_MAX_LEN"  */
errno_t memcpy_sOptTc(void* dest, size_t destMax, const void* src, size_t count)
{
    if (LIKELY( count <= destMax && dest && src   /*&& dest != src*/
        && count > 0
        && ( (dest > src  &&  (void*)((UINT8T*)src  + count) <= dest) ||
        (src  > dest &&  (void*)((UINT8T*)dest + count) <= src) )
        ) )
    {
        if (count > 32)  {
            /*large enough, let system API do it*/
#ifdef USE_ASM
            memcpy_opt(dest, src, count);
#else
            (void)memcpy(dest, src, count);
#endif
            return EOK;
        } else {
                        /* use struct assignment */
            switch (count)
            {
                case 1:*(MY_STR1 *)dest=*(MY_STR1 *)src;break;
                case 2:*(MY_STR2 *)dest=*(MY_STR2 *)src;break;
                case 3:*(MY_STR3 *)dest=*(MY_STR3 *)src;break;
                case 4:*(MY_STR4 *)dest=*(MY_STR4 *)src;break;
                case 5:*(MY_STR5 *)dest=*(MY_STR5 *)src;break;
                case 6:*(MY_STR6 *)dest=*(MY_STR6 *)src;break;
                case 7:*(MY_STR7 *)dest=*(MY_STR7 *)src;break;
                case 8:*(MY_STR8 *)dest=*(MY_STR8 *)src;break;
                case 9:*(MY_STR9 *)dest=*(MY_STR9 *)src;break;
                case 10:*(MY_STR10 *)dest=*(MY_STR10 *)src;break;
                case 11:*(MY_STR11 *)dest=*(MY_STR11 *)src;break;
                case 12:*(MY_STR12 *)dest=*(MY_STR12 *)src;break;
                case 13:*(MY_STR13 *)dest=*(MY_STR13 *)src;break;
                case 14:*(MY_STR14 *)dest=*(MY_STR14 *)src;break;
                case 15:*(MY_STR15 *)dest=*(MY_STR15 *)src;break;
                case 16:*(MY_STR16 *)dest=*(MY_STR16 *)src;break;
                case 17:*(MY_STR17 *)dest=*(MY_STR17 *)src;break;
                case 18:*(MY_STR18 *)dest=*(MY_STR18 *)src;break;
                case 19:*(MY_STR19 *)dest=*(MY_STR19 *)src;break;
                case 20:*(MY_STR20 *)dest=*(MY_STR20 *)src;break;
                case 21:*(MY_STR21 *)dest=*(MY_STR21 *)src;break;
                case 22:*(MY_STR22 *)dest=*(MY_STR22 *)src;break;
                case 23:*(MY_STR23 *)dest=*(MY_STR23 *)src;break;
                case 24:*(MY_STR24*)dest=*(MY_STR24 *)src;break;
                case 25:*(MY_STR25 *)dest=*(MY_STR25 *)src;break;
                case 26:*(MY_STR26 *)dest=*(MY_STR26 *)src;break;
                case 27:*(MY_STR27 *)dest=*(MY_STR27 *)src;break;
                case 28:*(MY_STR28 *)dest=*(MY_STR28 *)src;break;
                case 29:*(MY_STR29 *)dest=*(MY_STR29 *)src;break;
                case 30:*(MY_STR30 *)dest=*(MY_STR30 *)src;break;
                case 31:*(MY_STR31 *)dest=*(MY_STR31 *)src;break;
                case 32:*(MY_STR32 *)dest=*(MY_STR32 *)src;break;
            }
                return EOK;
        }
    }
    else
    {
        /* call it only to return error code */
        return memcpy_s(dest, destMax, src, count);
    }
}
#endif /* WITH_PERFORMANCE_ADDONS */
