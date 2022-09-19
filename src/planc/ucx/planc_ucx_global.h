/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_UCX_GLOBAL_H_
#define UCG_PLANC_UCX_GLOBAL_H_

#include "planc_ucx_plan.h"
#include "core/ucg_request.h"
#include "planc/ucg_planc.h"

typedef void* (*ucg_planc_ucx_get_ucp_ep_cb_t)(void *arg, void *group, int rank);

typedef void* (*ucg_planc_ucx_get_ucp_worker_cb_t)(void *arg);

typedef struct ucg_planc_ucx_oob_resource {
    void *arg;
    ucg_planc_ucx_get_ucp_worker_cb_t get_ucp_worker;
    ucg_planc_ucx_get_ucp_ep_cb_t get_ucp_ep;
} ucg_planc_ucx_oob_resource_t;

typedef struct ucg_planc_ucx {
    ucg_planc_t super;
    ucg_planc_ucx_oob_resource_t oob_resource;
} ucg_planc_ucx_t;

extern ucg_planc_ucx_algo_table_t ucx_algo_global_table[UCG_COLL_TYPE_LAST][UCX_MODULE_LAST];
extern ucg_components_t ucg_planc_ucx_planm;

ucg_planc_ucx_t *ucg_planc_ucx_instance();

static inline ucp_worker_h ucg_planc_ucx_get_oob_ucp_worker()
{
    ucg_planc_ucx_t *planc_ucx = ucg_planc_ucx_instance();
    ucg_planc_ucx_oob_resource_t *ucx_oob_resource = &planc_ucx->oob_resource;
    void *arg = ucx_oob_resource->arg;
    return (ucp_worker_h)ucx_oob_resource->get_ucp_worker(arg);
}

static inline ucp_ep_h ucg_planc_ucx_get_oob_ucp_ep(void *group, int rank)
{
    ucg_planc_ucx_t *planc_ucx = ucg_planc_ucx_instance();
    ucg_planc_ucx_oob_resource_t *ucx_oob_resource = &planc_ucx->oob_resource;
    void *arg = ucx_oob_resource->arg;
    return (ucp_ep_h)ucx_oob_resource->get_ucp_ep(arg, group, rank);
}

#endif