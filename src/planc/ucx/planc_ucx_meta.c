/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "planc_ucx_meta.h"
#include "planc_ucx_p2p.h"
#include "util/ucg_log.h"

static ucg_status_t ucg_planc_ucx_empty_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_op_reset(op);
    ucg_op->super.status = UCG_OK;
    return UCG_OK;
}

static ucg_status_t ucg_planc_ucx_empty_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_op->super.status = UCG_OK;
    return UCG_OK;
}

static ucg_status_t ucg_planc_ucx_empty_op_discard(ucg_plan_op_t *ucg_op)
{
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, ucg_op);
    ucg_mpool_put(ucg_op);
    return UCG_OK;
}

static ucg_planc_ucx_op_t* ucg_planc_ucx_empty_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                      ucg_vgroup_t *vgroup,
                                                      const ucg_coll_args_t *args)
{
    ucg_planc_ucx_op_t *ucx_op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (ucx_op == NULL) {
        return NULL;
    }

    ucg_status_t status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &ucx_op->super, vgroup,
                                              ucg_planc_ucx_empty_op_trigger,
                                              ucg_planc_ucx_empty_op_progress,
                                              ucg_planc_ucx_empty_op_discard,
                                              args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }

    ucg_planc_ucx_op_init(ucx_op, ucx_group);
    return ucx_op;

err_free_op:
    ucg_mpool_put(ucx_op);
    return NULL;
}

/* An empty op is used to ensure the op->tag can be matched. */
ucg_status_t ucg_planc_ucx_add_empty_op(ucg_plan_meta_op_t *meta_op,
                                        ucg_planc_ucx_group_t *ucx_group,
                                        ucg_vgroup_t *vgroup)
{
    ucg_coll_args_t args = {
        .type = UCG_COLL_TYPE_BARRIER,
    };
    ucg_planc_ucx_op_t *ucx_op = NULL;
    ucx_op = ucg_planc_ucx_empty_op_new(ucx_group, vgroup, &args);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    return ucg_plan_meta_op_add(meta_op, &ucx_op->super);
}