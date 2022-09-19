/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "planc_ucx_global.h"
#include "planc_ucx_base.h"
#include "planc/ucg_planm.h"
#include "util/ucg_log.h"
#include "util/ucg_helper.h"
#include "util/ucg_component.h"

ucg_planc_ucx_algo_table_t ucx_algo_global_table[UCG_COLL_TYPE_LAST][UCX_MODULE_LAST];
ucg_components_t ucg_planc_ucx_planm = {0, NULL};

static void ucg_planc_ucx_global_fill_oob_resource(const ucg_global_params_t *params,
                                                   ucg_planc_ucx_oob_resource_t *ucx_oob_resource)
{
    if (ucg_test_flags(params->field_mask, UCG_GLOBAL_PARAMS_FIELD_OOB_RESOURCE)) {
        const ucg_oob_resource_t *ucg_oob_resource = &params->oob_resource;
        ucx_oob_resource->get_ucp_ep = ucg_oob_resource->get_ucp_ep;
        ucx_oob_resource->get_ucp_worker = ucg_oob_resource->get_ucp_worker;
        ucx_oob_resource->arg = ucg_oob_resource->arg;
    } else {
        ucx_oob_resource->get_ucp_ep = (ucg_planc_ucx_get_ucp_ep_cb_t)ucg_empty_function_return_null;
        ucx_oob_resource->get_ucp_worker = (ucg_planc_ucx_get_ucp_worker_cb_t)ucg_empty_function_return_null;
        ucx_oob_resource->arg = NULL;
    }
    return;
}

static ucg_status_t ucg_planc_ucx_global_init(const ucg_global_params_t *params)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_t *planc_ucx = ucg_planc_ucx_instance();
    ucg_planc_ucx_global_fill_oob_resource(params, &planc_ucx->oob_resource);

    /* Try to load ucx component extra modules */
    status = ucg_planm_load(planc_ucx->super.super.name, &ucg_planc_ucx_planm);
    if (status != UCG_OK) {
        ucg_info("Failed to try-load %s component modules", planc_ucx->super.super.name);
    }

    return UCG_OK;
}

static void ucg_planc_ucx_global_cleanup()
{
    ucg_planm_unload(&ucg_planc_ucx_planm);
    return;
}

ucg_planc_ucx_t UCG_PLANC_OBJNAME(ucx) = {
    .super.super.name       = "ucx",
    .super.mem_query        = ucg_planc_ucx_mem_query,

    .super.global_init      = ucg_planc_ucx_global_init,
    .super.global_cleanup   = ucg_planc_ucx_global_cleanup,

    .super.config_read      = ucg_planc_ucx_config_read,
    .super.config_modify    = ucg_planc_ucx_config_modify,
    .super.config_release   = ucg_planc_ucx_config_release,

    .super.context_init     = ucg_planc_ucx_context_init,
    .super.context_cleanup  = ucg_planc_ucx_context_cleanup,
    .super.context_query    = ucg_planc_ucx_context_query,

    .super.group_create     = ucg_planc_ucx_group_create,
    .super.group_destroy    = ucg_planc_ucx_group_destroy,

    .super.get_plans        = ucg_planc_ucx_get_plans,
};

ucg_planc_ucx_t *ucg_planc_ucx_instance()
{
    return &UCG_PLANC_OBJNAME(ucx);
}