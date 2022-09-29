/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_HCCL_GROUP_H_
#define UCG_PLANC_HCCL_GROUP_H_

#include <hccl/hccl.h>
#include "ucg/api/ucg.h"

#include "planc_hccl_context.h"
#include "core/ucg_group.h"
#include "planc/ucg_planc.h"
#include "util/ucg_mpool.h"


/* Choose context rank 0 as root. */
#define UCG_PLANC_HCCL_ROOT_RANK 0

#define UCG_PLANC_HCCL_GROUP_OOB(_group) &((_group)->super.super.group->oob_group)
#define UCG_PLANC_HCCL_GROUP_MYRANK(_group) (_group)->super.super.group->myrank
#define UCG_PLANC_HCCL_GROUP_SIZE(_group) (_group)->super.super.group->size

#define UCG_PLANC_HCCL_GROUP_IS_ROOT(_group) \
    UCG_PLANC_HCCL_GROUP_MYRANK(_group) == UCG_PLANC_HCCL_ROOT_RANK


typedef struct ucg_planc_hccl_group {
    ucg_planc_group_t super;
    ucg_planc_hccl_context_t *context;

    HcclComm comm;
    aclrtStream stream; /**< all operations must be mounted to stream. */
    ucg_status_t *dev_status;   /**< device pointer, it's always UCG_OK */
} ucg_planc_hccl_group_t;

ucg_status_t ucg_planc_hccl_group_create(ucg_planc_context_h context,
                                         const ucg_planc_group_params_t *params,
                                         ucg_planc_group_h *planc_group);
void ucg_planc_hccl_group_destroy(ucg_planc_group_h planc_group);

#endif