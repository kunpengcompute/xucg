/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "barrier.h"

#include "util/ucg_helper.h"


static ucg_status_t ucg_planc_hccl_barrier_native_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_hccl_op_t *op = ucg_derived_of(ucg_op, ucg_planc_hccl_op_t);
    ucg_planc_hccl_group_t *hccl_group = op->hccl_group;

    UCG_PLANC_HCCL_OP_TRIGGER(HcclBarrier(hccl_group->comm, hccl_group->stream),
                              op, status);

    return status;
}

ucg_status_t ucg_planc_hccl_barrier_native_prepare(ucg_vgroup_t *vgroup,
                                                   const ucg_coll_args_t *args,
                                                   ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_status_t status = UCG_OK;
    ucg_planc_hccl_group_t *hccl_group = ucg_derived_of(vgroup, ucg_planc_hccl_group_t);
    ucg_planc_hccl_op_t *hccl_op = ucg_mpool_get(&hccl_group->context->op_mp);
    if (hccl_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &hccl_op->super, vgroup,
                                 ucg_planc_hccl_barrier_native_op_trigger,
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