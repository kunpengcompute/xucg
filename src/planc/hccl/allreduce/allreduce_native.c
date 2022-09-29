/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "allreduce.h"
#include "planc_hccl_dt.h"

#include "util/ucg_helper.h"


static ucg_status_t ucg_planc_hccl_allreduce_native_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_coll_allreduce_args_t *coll_args = &ucg_op->super.args.allreduce;
    ucg_planc_hccl_op_t *op = ucg_derived_of(ucg_op, ucg_planc_hccl_op_t);
    ucg_planc_hccl_group_t *hccl_group = op->hccl_group;

    UCG_PLANC_HCCL_OP_TRIGGER(HcclAllReduce((void*)coll_args->sendbuf,
                                            coll_args->recvbuf,
                                            (uint64_t)coll_args->count,
                                            ucg_planc_hccl_dt_u2h(coll_args->dt),
                                            ucg_planc_hccl_op_u2h(coll_args->op),
                                            hccl_group->comm,
                                            hccl_group->stream),
                              op, status);
    return status;
}

ucg_status_t ucg_planc_hccl_allreduce_native_prepare(ucg_vgroup_t *vgroup,
                                                     const ucg_coll_args_t *args,
                                                     ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    const ucg_coll_allreduce_args_t *coll_args = &args->allreduce;
    if (!ucg_planc_hccl_dt_is_supported(coll_args->dt) ||
        !ucg_planc_hccl_op_is_supported(coll_args->op)) {
        return UCG_ERR_UNSUPPORTED;
    }

    ucg_status_t status = UCG_OK;
    ucg_planc_hccl_group_t *hccl_group = ucg_derived_of(vgroup, ucg_planc_hccl_group_t);
    ucg_planc_hccl_op_t *hccl_op = ucg_mpool_get(&hccl_group->context->op_mp);
    if (hccl_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &hccl_op->super, vgroup,
                                 ucg_planc_hccl_allreduce_native_op_trigger,
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
