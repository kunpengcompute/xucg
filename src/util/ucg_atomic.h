/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_ATOMIC_H_
#define UCG_ATOMIC_H_

#include <ucs/arch/atomic.h>

#define ucg_atomic_fadd32(_ptr, _val)   ucs_atomic_fadd32(_ptr, _val)
#define ucg_atomic_sub32(_ptr, _val)    ucs_atomic_sub32(_ptr, _val)
#define ucg_atomic_add64(_ptr, _val)    ucs_atomic_add64(_ptr, _val)
#define ucg_atomic_sub64(_ptr, _val)    ucs_atomic_sub64(_ptr, _val)
#define ucg_atomic_cswap8(_ptr, _compare, _swap)        ucs_atomic_cswap8(_ptr, _compare, _swap)
#define ucg_atomic_bool_cswap8(_ptr, _compare, _swap)   ucs_atomic_bool_cswap8(_ptr, _compare, _swap)
#define ucg_atomic_bool_cswap64(_ptr, _compare, _swap)  ucs_atomic_bool_cswap64(_ptr, _compare, _swap)
#endif