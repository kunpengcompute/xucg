/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_HCCL_PLAN_H_
#define UCG_PLANC_HCCL_PLAN_H_

#include "ucg/api/ucg.h"

#include "planc_hccl_helper.h"
#include "planc_hccl_group.h"

#include "core/ucg_plan.h"
#include "planc/ucg_planc.h"


#ifndef UCG_PLANC_HCCL_DEFAULT_SCORE
    #define UCG_PLANC_HCCL_DEFAULT_SCORE 20
#endif

/* trigger template of planc hccl op. */
#define UCG_PLANC_HCCL_OP_TRIGGER_INNER(_hccl_func, _op, _status) \
    do { \
        _op->super.super.status = UCG_INPROGRESS; \
        _status = UCG_PLANC_HCCL_CHECK_HCCL(_hccl_func); \
        if (status != UCG_OK) { \
            _op->super.super.status = _status;\
            break; \
        } \
        \
        _status = ucg_planc_hccl_op_set_sync(_op); \
        if (_status != UCG_OK) { \
            _op->super.super.status = _status; \
            break; \
        } \
        \
        _status = _op->super.progress(&_op->super); \
        if (_status == UCG_INPROGRESS) { \
            _status = UCG_OK; \
        } \
    } while (0)

#define UCG_PLANC_HCCL_OP_TRIGGER(_hccl_func, _op, _status) \
    do { \
        void *old_rt = ucg_planc_hccl_op_set_rt(_op->hccl_group->context->acl_context); \
        UCG_PLANC_HCCL_OP_TRIGGER_INNER(_hccl_func, _op, _status); \
        ucg_planc_hccl_op_set_rt(old_rt); \
    } while (0)

typedef struct ucg_planc_hccl_op {
    ucg_plan_op_t super;

    void *dev_stage_area;    // temporary space storage buffer
    ucg_status_t dev_status; // Status of the collective operation on the device.
    ucg_planc_hccl_group_t *hccl_group;
} ucg_planc_hccl_op_t;


UCG_PLAN_ATTR_TABLE_DECLARE(ucg_planc_hccl);

static inline void* ucg_planc_hccl_op_set_rt(void *rt)
{
    /* Ignore the error. If the runtime setting fails, subsequent operations will fail.*/
    aclrtContext old_acl_context = NULL;
    (void)aclrtGetCurrentContext(&old_acl_context);
    if (rt != old_acl_context) {
        (void)aclrtSetCurrentContext((aclrtContext)rt);
    }
    return old_acl_context;
}

ucg_status_t ucg_planc_hccl_op_set_sync(ucg_planc_hccl_op_t *op);
ucg_status_t ucg_planc_hccl_op_progress(ucg_plan_op_t *ucg_op);
ucg_status_t ucg_planc_hccl_op_discard(ucg_plan_op_t *ucg_op);


ucg_status_t ucg_planc_hccl_get_plans(ucg_planc_group_h planc_group, ucg_plans_t *plans);

#endif