/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "bcast.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_p2p.h"
#include "core/ucg_dt.h"
#include "core/ucg_group.h"
#include "util/algo/ucg_kntree.h"
#include "util/ucg_log.h"
#include "util/ucg_malloc.h"

ucg_status_t ucg_planc_ucx_bcast_bntree_prepare(ucg_vgroup_t *vgroup,
                                                const ucg_coll_args_t *args,
                                                ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_bcast_config_t *bcast_config;
    bcast_config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, bcast,
                                                       UCG_COLL_TYPE_BCAST);
    ucg_planc_ucx_bcast_config_t config;
    config.kntree_degree = 2;
    config.root_adjust = bcast_config->root_adjust;
    ucg_planc_ucx_op_t *ucx_op = ucg_planc_ucx_bcast_kntree_op_new(ucx_group,
                                                                   vgroup,
                                                                   args,
                                                                   &config);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &ucx_op->super;
    return UCG_OK;
}