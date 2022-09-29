/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */
#include "planc_hccl_group.h"
#include "planc_hccl_context.h"
#include "planc_hccl_helper.h"
#include "planc_hccl_global.h"

#include "core/ucg_group.h"


static ucg_status_t ucg_planc_hccl_group_fill_resource(ucg_planc_hccl_group_t *hccl_group)
{
    ucg_assert(hccl_group != NULL);

    ucg_status_t status = UCG_OK;

    uint32_t group_size = UCG_PLANC_HCCL_GROUP_SIZE(hccl_group);
    HcclRootInfo *root_info = ucg_malloc(sizeof(HcclRootInfo) * (group_size + 1),
                                         "hccl root info");
    if (root_info == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    if (UCG_PLANC_HCCL_GROUP_IS_ROOT(hccl_group)) {
        UCG_PLANC_HCCL_CHECK_HCCL_GOTO(HcclGetRootInfo(&root_info[group_size]),
                                       status, err_free_root_info);
    }

    ucg_oob_group_t *oob_group = UCG_PLANC_HCCL_GROUP_OOB(hccl_group);
    ucg_assert(oob_group->size == group_size);
    status = oob_group->allgather(&root_info[group_size], root_info,
                                  sizeof(HcclRootInfo), oob_group->group);
    if (status != UCG_OK) {
        ucg_error("Failed to oob allgather");
        goto err_free_root_info;
    }

    /* initialize HCCL comm */
    UCG_PLANC_HCCL_CHECK_HCCL_GOTO(HcclCommInitRootInfo(group_size,
                                                        &root_info[UCG_PLANC_HCCL_ROOT_RANK],
                                                        UCG_PLANC_HCCL_GROUP_MYRANK(hccl_group),
                                                        &hccl_group->comm),
                                   status, err_free_root_info);

    /* create the HCCL stream instance */
    UCG_PLANC_HCCL_CHECK_ACL_GOTO(aclrtCreateStream(&hccl_group->stream),
                                  status, err_free_comm);

    /* Used to confirm whether the non-blocking collective operation is completed.*/
    UCG_PLANC_HCCL_CHECK_ACL_GOTO(aclrtMalloc((void **)&hccl_group->dev_status,
                                              sizeof(ucg_status_t),
                                              ACL_MEM_MALLOC_NORMAL_ONLY),
                                  status, err_free_stream);

    ucg_status_t host_status = UCG_OK;
    UCG_PLANC_HCCL_CHECK_ACL_GOTO(aclrtMemcpy(hccl_group->dev_status, sizeof(ucg_status_t),
                                              &host_status, sizeof(ucg_status_t),
                                              ACL_MEMCPY_HOST_TO_DEVICE),
                                  status, err_free_dev_status);

    ucg_free(root_info);
    return UCG_OK;

err_free_dev_status:
    aclrtFree(hccl_group->dev_status);
err_free_stream:
    aclrtDestroyStream(hccl_group->stream);
err_free_comm:
    HcclCommDestroy(hccl_group->comm);
err_free_root_info:
    ucg_free(root_info);
    return status;
}

static void ucg_planc_hccl_group_free_resource(ucg_planc_hccl_group_t *hccl_group)
{
    aclrtFree(hccl_group->dev_status);
    aclrtDestroyStream(hccl_group->stream);
    HcclCommDestroy(hccl_group->comm);
    return;
}

ucg_status_t ucg_planc_hccl_group_create(ucg_planc_context_h context,
                                         const ucg_planc_group_params_t *params,
                                         ucg_planc_group_h *planc_group)
{
    UCG_CHECK_NULL_INVALID(context, params, planc_group);

    ucg_status_t status;
    ucg_planc_hccl_group_t *hccl_group;

    hccl_group = ucg_calloc(1, sizeof(ucg_planc_hccl_group_t), "ucg planc hccl group");
    if (hccl_group == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    hccl_group->context = context;

    status = UCG_CLASS_CONSTRUCT(ucg_planc_group_t, &hccl_group->super, params->group);
    if (status != UCG_OK) {
        ucg_error("Failed to init planc hccl group");
        goto err_free_group;
    }

    status = ucg_planc_hccl_group_fill_resource(hccl_group);
    if (status != UCG_OK) {
        goto err_destruct;
    }

    *planc_group = (ucg_planc_group_h)hccl_group;
    return UCG_OK;

err_destruct:
    UCG_CLASS_DESTRUCT(ucg_planc_group_t, &hccl_group->super);
err_free_group:
    ucg_free(hccl_group);
    return status;
}

void ucg_planc_hccl_group_destroy(ucg_planc_group_h planc_group)
{
    UCG_CHECK_NULL_VOID(planc_group);

    ucg_planc_hccl_group_t *hccl_group = (ucg_planc_hccl_group_t *)planc_group;

    ucg_planc_hccl_group_free_resource(hccl_group);
    UCG_CLASS_DESTRUCT(ucg_planc_group_t, &hccl_group->super);
    ucg_free(hccl_group);

    return;
}
