/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_HCCL_DT_H_
#define UCG_PLANC_HCCL_DT_H_

#include "ucg/api/ucg.h"
#include "core/ucg_dt.h"
#include "util/ucg_helper.h"

#include <hccl/hccl.h>

extern HcclDataType ucg_planc_hccl_dt_table[UCG_DT_TYPE_PREDEFINED_LAST];
extern HcclReduceOp ucg_planc_hccl_op_table[UCG_OP_TYPE_PREDEFINED_LAST];
extern int ucg_planc_hccl_dt_size_table[HCCL_DATA_TYPE_RESERVED];

static inline HcclDataType ucg_planc_hccl_dt_u2h(const ucg_dt_t *ucg_dt)
{
    if (!ucg_dt_is_predefined(ucg_dt)) {
        return HCCL_DATA_TYPE_RESERVED;
    }
    return ucg_planc_hccl_dt_table[ucg_dt_type(ucg_dt)];
}

static inline int ucg_planc_hccl_dt_is_supported(const ucg_dt_t *ucg_dt)
{
    return ucg_planc_hccl_dt_u2h(ucg_dt) != HCCL_DATA_TYPE_RESERVED;
}

static inline int ucg_planc_hccl_dt_size(HcclDataType dt)
{
    ucg_assert(dt >= 0 && dt <= HCCL_DATA_TYPE_RESERVED);
    return ucg_planc_hccl_dt_size_table[dt];
}

static inline HcclReduceOp ucg_planc_hccl_op_u2h(const ucg_op_t *ucg_op)
{
    if (!ucg_op_is_predefined(ucg_op)) {
        return HCCL_REDUCE_RESERVED;
    }

    return ucg_planc_hccl_op_table[ucg_op_type(ucg_op)];
}

static inline int ucg_planc_hccl_op_is_supported(const ucg_op_t *ucg_op)
{
    return ucg_planc_hccl_op_u2h(ucg_op) != HCCL_REDUCE_RESERVED;
}

#endif