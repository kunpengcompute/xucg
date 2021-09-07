/*******************************************************************************
* Copyright @ Huawei Technologies Co., Ltd. 1998-2014. All rights reserved.
* File name: memset_s.c
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
*    memset_s
*
* <SYNOPSIS>
*    errno_t memset_s(void* dest, size_t destMax, int c, size_t count)
*
* <FUNCTION DESCRIPTION>
*    Sets buffers to a specified character.
*
* <INPUT PARAMETERS>
*    dest                       Pointer to destination.
*    destMax                    The size of the buffer.
*    c                          Character to set.
*    count                      Number of characters.
*
* <OUTPUT PARAMETERS>
*    dest buffer                is uptdated.
*
* <RETURN VALUE>
*    EOK                        Success
*    EINVAL                     dest == NULL
*    ERANGE                     count > destMax or destMax > SECUREC_MEM_MAX_LEN
*                               or destMax == 0
*******************************************************************************
*/


errno_t memset_s(void* dest, size_t destMax, int c, size_t count)
{
    if (destMax == 0 || destMax > SECUREC_MEM_MAX_LEN) {
        SECUREC_ERROR_INVALID_RANGE("memset_s");
        return ERANGE;
    }
    if (dest == NULL) {
        SECUREC_ERROR_INVALID_PARAMTER("memset_s");
        return EINVAL;
    }

    if (count > destMax) {
        memset(dest, c, destMax);
        SECUREC_ERROR_INVALID_RANGE("memset_s");
        return ERANGE_AND_RESET;
    }
    memset(dest, c, count);
    return EOK;
}

#if defined(WITH_PERFORMANCE_ADDONS)
/* assemble language memset */
extern void *memset_opt(void *d, int c, size_t cnt);
static const MY_STR32 myStr = {"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"};
static const MY_STR32 myStrAllFF = {"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"};

errno_t memset_sOptAsm(void* dest, size_t destMax, int c, size_t count)
{
    if (LIKELY(count <= destMax  && dest  && destMax <= SECUREC_MEM_MAX_LEN ))
    {
        if (count > 32)
        {
#ifdef USE_ASM
            (void)memset_opt(dest, c, count);
#else
            (void)memset(dest, c, count);
#endif
            return EOK;
        }
        else
        {
            /* use struct assignment */
            switch (c)
            {
                case 0:
                {
                case 1:*(MY_STR1 *)dest=*(MY_STR1 *)(&myStr);break;
                case 2:*(MY_STR2 *)dest=*(MY_STR2 *)(&myStr);break;
                case 3:*(MY_STR3 *)dest=*(MY_STR3 *)(&myStr);break;
                case 4:*(MY_STR4 *)dest=*(MY_STR4 *)(&myStr);break;
                case 5:*(MY_STR5 *)dest=*(MY_STR5 *)(&myStr);break;
                case 6:*(MY_STR6 *)dest=*(MY_STR6 *)(&myStr);break;
                case 7:*(MY_STR7 *)dest=*(MY_STR7 *)(&myStr);break;
                case 8:*(MY_STR8 *)dest=*(MY_STR8 *)(&myStr);break;
                case 9:*(MY_STR9 *)dest=*(MY_STR9 *)(&myStr);break;
                case 10:*(MY_STR10 *)dest=*(MY_STR10 *)(&myStr);break;
                case 11:*(MY_STR11 *)dest=*(MY_STR11 *)(&myStr);break;
                case 12:*(MY_STR12 *)dest=*(MY_STR12 *)(&myStr);break;
                case 13:*(MY_STR13 *)dest=*(MY_STR13 *)(&myStr);break;
                case 14:*(MY_STR14 *)dest=*(MY_STR14 *)(&myStr);break;
                case 15:*(MY_STR15 *)dest=*(MY_STR15 *)(&myStr);break;
                case 16:*(MY_STR16 *)dest=*(MY_STR16 *)(&myStr);break;
                case 17:*(MY_STR17 *)dest=*(MY_STR17 *)(&myStr);break;
                case 18:*(MY_STR18 *)dest=*(MY_STR18 *)(&myStr);break;
                case 19:*(MY_STR19 *)dest=*(MY_STR19 *)(&myStr);break;
                case 20:*(MY_STR20 *)dest=*(MY_STR20 *)(&myStr);break;
                case 21:*(MY_STR21 *)dest=*(MY_STR21 *)(&myStr);break;
                case 22:*(MY_STR22 *)dest=*(MY_STR22 *)(&myStr);break;
                case 23:*(MY_STR23 *)dest=*(MY_STR23 *)(&myStr);break;
                case 24:*(MY_STR24*)dest=*(MY_STR24 *)(&myStr);break;
                case 25:*(MY_STR25 *)dest=*(MY_STR25 *)(&myStr);break;
                case 26:*(MY_STR26 *)dest=*(MY_STR26 *)(&myStr);break;
                case 27:*(MY_STR27 *)dest=*(MY_STR27 *)(&myStr);break;
                case 28:*(MY_STR28 *)dest=*(MY_STR28 *)(&myStr);break;
                case 29:*(MY_STR29 *)dest=*(MY_STR29 *)(&myStr);break;
                case 30:*(MY_STR30 *)dest=*(MY_STR30 *)(&myStr);break;
                case 31:*(MY_STR31 *)dest=*(MY_STR31 *)(&myStr);break;
                case 32:*(MY_STR32 *)dest=*(MY_STR32 *)(&myStr);break;
                }

                return EOK;

            case 0xFF:
                switch (count)
                {
                case 1:*(MY_STR1 *)dest=*(MY_STR1 *)(&myStrAllFF);break;
                case 2:*(MY_STR2 *)dest=*(MY_STR2 *)(&myStrAllFF);break;
                case 3:*(MY_STR3 *)dest=*(MY_STR3 *)(&myStrAllFF);break;
                case 4:*(MY_STR4 *)dest=*(MY_STR4 *)(&myStrAllFF);break;
                case 5:*(MY_STR5 *)dest=*(MY_STR5 *)(&myStrAllFF);break;
                case 6:*(MY_STR6 *)dest=*(MY_STR6 *)(&myStrAllFF);break;
                case 7:*(MY_STR7 *)dest=*(MY_STR7 *)(&myStrAllFF);break;
                case 8:*(MY_STR8 *)dest=*(MY_STR8 *)(&myStrAllFF);break;
                case 9:*(MY_STR9 *)dest=*(MY_STR9 *)(&myStrAllFF);break;
                case 10:*(MY_STR10 *)dest=*(MY_STR10 *)(&myStrAllFF);break;
                case 11:*(MY_STR11 *)dest=*(MY_STR11 *)(&myStrAllFF);break;
                case 12:*(MY_STR12 *)dest=*(MY_STR12 *)(&myStrAllFF);break;
                case 13:*(MY_STR13 *)dest=*(MY_STR13 *)(&myStrAllFF);break;
                case 14:*(MY_STR14 *)dest=*(MY_STR14 *)(&myStrAllFF);break;
                case 15:*(MY_STR15 *)dest=*(MY_STR15 *)(&myStrAllFF);break;
                case 16:*(MY_STR16 *)dest=*(MY_STR16 *)(&myStrAllFF);break;
                case 17:*(MY_STR17 *)dest=*(MY_STR17 *)(&myStrAllFF);break;
                case 18:*(MY_STR18 *)dest=*(MY_STR18 *)(&myStrAllFF);break;
                case 19:*(MY_STR19 *)dest=*(MY_STR19 *)(&myStrAllFF);break;
                case 20:*(MY_STR20 *)dest=*(MY_STR20 *)(&myStrAllFF);break;
                case 21:*(MY_STR21 *)dest=*(MY_STR21 *)(&myStrAllFF);break;
                case 22:*(MY_STR22 *)dest=*(MY_STR22 *)(&myStrAllFF);break;
                case 23:*(MY_STR23 *)dest=*(MY_STR23 *)(&myStrAllFF);break;
                case 24:*(MY_STR24 *)dest=*(MY_STR24 *)(&myStrAllFF);break;
                case 25:*(MY_STR25 *)dest=*(MY_STR25 *)(&myStrAllFF);break;
                case 26:*(MY_STR26 *)dest=*(MY_STR26 *)(&myStrAllFF);break;
                case 27:*(MY_STR27 *)dest=*(MY_STR27 *)(&myStrAllFF);break;
                case 28:*(MY_STR28 *)dest=*(MY_STR28 *)(&myStrAllFF);break;
                case 29:*(MY_STR29 *)dest=*(MY_STR29 *)(&myStrAllFF);break;
                case 30:*(MY_STR30 *)dest=*(MY_STR30 *)(&myStrAllFF);break;
                case 31:*(MY_STR31 *)dest=*(MY_STR31 *)(&myStrAllFF);break;
                case 32:*(MY_STR32 *)dest=*(MY_STR32 *)(&myStrAllFF);break;
                }
                return EOK;
            }
            memset(dest, c, count);
            return EOK;
        }
    }
    else
    {
        return memset_s(dest, destMax, c, count);
    }
}

errno_t memset_sOptTc(void* dest, size_t destMax, int c, size_t count)
{
    if (LIKELY(count <= destMax  && dest ))
    {
        if (count > 32)
        {
#ifdef USE_ASM
            (void)memset_opt(dest, c, count);
#else
            (void)memset(dest, c, count);
#endif
            return EOK;
        }
        else
        {
            /* use struct assignment */
            switch (c)
            {
                case 0:
                {
                case 1:*(MY_STR1 *)dest=*(MY_STR1 *)(&myStr);break;
                case 2:*(MY_STR2 *)dest=*(MY_STR2 *)(&myStr);break;
                case 3:*(MY_STR3 *)dest=*(MY_STR3 *)(&myStr);break;
                case 4:*(MY_STR4 *)dest=*(MY_STR4 *)(&myStr);break;
                case 5:*(MY_STR5 *)dest=*(MY_STR5 *)(&myStr);break;
                case 6:*(MY_STR6 *)dest=*(MY_STR6 *)(&myStr);break;
                case 7:*(MY_STR7 *)dest=*(MY_STR7 *)(&myStr);break;
                case 8:*(MY_STR8 *)dest=*(MY_STR8 *)(&myStr);break;
                case 9:*(MY_STR9 *)dest=*(MY_STR9 *)(&myStr);break;
                case 10:*(MY_STR10 *)dest=*(MY_STR10 *)(&myStr);break;
                case 11:*(MY_STR11 *)dest=*(MY_STR11 *)(&myStr);break;
                case 12:*(MY_STR12 *)dest=*(MY_STR12 *)(&myStr);break;
                case 13:*(MY_STR13 *)dest=*(MY_STR13 *)(&myStr);break;
                case 14:*(MY_STR14 *)dest=*(MY_STR14 *)(&myStr);break;
                case 15:*(MY_STR15 *)dest=*(MY_STR15 *)(&myStr);break;
                case 16:*(MY_STR16 *)dest=*(MY_STR16 *)(&myStr);break;
                case 17:*(MY_STR17 *)dest=*(MY_STR17 *)(&myStr);break;
                case 18:*(MY_STR18 *)dest=*(MY_STR18 *)(&myStr);break;
                case 19:*(MY_STR19 *)dest=*(MY_STR19 *)(&myStr);break;
                case 20:*(MY_STR20 *)dest=*(MY_STR20 *)(&myStr);break;
                case 21:*(MY_STR21 *)dest=*(MY_STR21 *)(&myStr);break;
                case 22:*(MY_STR22 *)dest=*(MY_STR22 *)(&myStr);break;
                case 23:*(MY_STR23 *)dest=*(MY_STR23 *)(&myStr);break;
                case 24:*(MY_STR24*)dest=*(MY_STR24 *)(&myStr);break;
                case 25:*(MY_STR25 *)dest=*(MY_STR25 *)(&myStr);break;
                case 26:*(MY_STR26 *)dest=*(MY_STR26 *)(&myStr);break;
                case 27:*(MY_STR27 *)dest=*(MY_STR27 *)(&myStr);break;
                case 28:*(MY_STR28 *)dest=*(MY_STR28 *)(&myStr);break;
                case 29:*(MY_STR29 *)dest=*(MY_STR29 *)(&myStr);break;
                case 30:*(MY_STR30 *)dest=*(MY_STR30 *)(&myStr);break;
                case 31:*(MY_STR31 *)dest=*(MY_STR31 *)(&myStr);break;
                case 32:*(MY_STR32 *)dest=*(MY_STR32 *)(&myStr);break;
                }

                return EOK;

            case 0xFF:
                switch (count)
                {
                case 1:*(MY_STR1 *)dest=*(MY_STR1 *)(&myStrAllFF);break;
                case 2:*(MY_STR2 *)dest=*(MY_STR2 *)(&myStrAllFF);break;
                case 3:*(MY_STR3 *)dest=*(MY_STR3 *)(&myStrAllFF);break;
                case 4:*(MY_STR4 *)dest=*(MY_STR4 *)(&myStrAllFF);break;
                case 5:*(MY_STR5 *)dest=*(MY_STR5 *)(&myStrAllFF);break;
                case 6:*(MY_STR6 *)dest=*(MY_STR6 *)(&myStrAllFF);break;
                case 7:*(MY_STR7 *)dest=*(MY_STR7 *)(&myStrAllFF);break;
                case 8:*(MY_STR8 *)dest=*(MY_STR8 *)(&myStrAllFF);break;
                case 9:*(MY_STR9 *)dest=*(MY_STR9 *)(&myStrAllFF);break;
                case 10:*(MY_STR10 *)dest=*(MY_STR10 *)(&myStrAllFF);break;
                case 11:*(MY_STR11 *)dest=*(MY_STR11 *)(&myStrAllFF);break;
                case 12:*(MY_STR12 *)dest=*(MY_STR12 *)(&myStrAllFF);break;
                case 13:*(MY_STR13 *)dest=*(MY_STR13 *)(&myStrAllFF);break;
                case 14:*(MY_STR14 *)dest=*(MY_STR14 *)(&myStrAllFF);break;
                case 15:*(MY_STR15 *)dest=*(MY_STR15 *)(&myStrAllFF);break;
                case 16:*(MY_STR16 *)dest=*(MY_STR16 *)(&myStrAllFF);break;
                case 17:*(MY_STR17 *)dest=*(MY_STR17 *)(&myStrAllFF);break;
                case 18:*(MY_STR18 *)dest=*(MY_STR18 *)(&myStrAllFF);break;
                case 19:*(MY_STR19 *)dest=*(MY_STR19 *)(&myStrAllFF);break;
                case 20:*(MY_STR20 *)dest=*(MY_STR20 *)(&myStrAllFF);break;
                case 21:*(MY_STR21 *)dest=*(MY_STR21 *)(&myStrAllFF);break;
                case 22:*(MY_STR22 *)dest=*(MY_STR22 *)(&myStrAllFF);break;
                case 23:*(MY_STR23 *)dest=*(MY_STR23 *)(&myStrAllFF);break;
                case 24:*(MY_STR24*)dest=*(MY_STR24 *)(&myStrAllFF);break;
                case 25:*(MY_STR25 *)dest=*(MY_STR25 *)(&myStrAllFF);break;
                case 26:*(MY_STR26 *)dest=*(MY_STR26 *)(&myStrAllFF);break;
                case 27:*(MY_STR27 *)dest=*(MY_STR27 *)(&myStrAllFF);break;
                case 28:*(MY_STR28 *)dest=*(MY_STR28 *)(&myStrAllFF);break;
                case 29:*(MY_STR29 *)dest=*(MY_STR29 *)(&myStrAllFF);break;
                case 30:*(MY_STR30 *)dest=*(MY_STR30 *)(&myStrAllFF);break;
                case 31:*(MY_STR31 *)dest=*(MY_STR31 *)(&myStrAllFF);break;
                case 32:*(MY_STR32 *)dest=*(MY_STR32 *)(&myStrAllFF);break;
                }
                return EOK;
            }
            memset(dest, c, count);
            return EOK;
        }
    }
    else
    {
        return memset_s(dest, destMax, c, count);
    }
}
#endif /* WITH_PERFORMANCE_ADDONS */
