/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "bcast.h"
#include "planc_hccl_dt.h"

#include "util/ucg_helper.h"


static ucg_status_t ucg_planc_hccl_bcast_native_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_coll_bcast_args_t *coll_args = &ucg_op->super.args.bcast;
    ucg_planc_hccl_op_t *op = ucg_derived_of(ucg_op, ucg_planc_hccl_op_t);
    ucg_planc_hccl_group_t *hccl_group = op->hccl_group;

    UCG_PLANC_HCCL_OP_TRIGGER(HcclBroadcast(coll_args->buffer,
                                            (uint64_t)coll_args->count,
                                            ucg_planc_hccl_dt_u2h(coll_args->dt),
                                            coll_args->root,
                                            hccl_group->comm,
                                            hccl_group->stream),
                              op, status);

    return status;
}

ucg_status_t ucg_planc_hccl_bcast_native_prepare(ucg_vgroup_t *vgroup,
                                                 const ucg_coll_args_t *args,
                                                 ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_status_t status;
    ucg_planc_hccl_group_t *hccl_group;
    ucg_planc_hccl_op_t *hccl_op;

    const ucg_coll_bcast_args_t *coll_args = &args->bcast;
    if (!ucg_planc_hccl_dt_is_supported(coll_args->dt)) {
        return UCG_ERR_UNSUPPORTED;
    }

    hccl_group = ucg_derived_of(vgroup, ucg_planc_hccl_group_t);
    hccl_op = ucg_mpool_get(&hccl_group->context->op_mp);
    if (hccl_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &hccl_op->super, vgroup,
                                 ucg_planc_hccl_bcast_native_op_trigger,
                                 ucg_planc_hccl_op_progress,
                                 ucg_planc_hccl_op_discard,
                                 args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of hccl op");
        ucg_mpool_put(hccl_op);
        return status;
    }

    hccl_op->hccl_group = hccl_group;

    *op = &hccl_op->super;
    return UCG_OK;
}