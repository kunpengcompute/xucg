/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_UCX_CONTEXT_H_
#define UCG_PLANC_UCX_CONTEXT_H_

#include <ucp/api/ucp_def.h>

#include "ucg/api/ucg.h"
#include "planc/ucg_planc_def.h"
#include "planc/ucg_planc.h"
#include "planc/ucg_planm.h"
#include "core/ucg_context.h"
#include "core/ucg_request.h"
#include "util/ucg_mpool.h"

typedef enum {
    UCX_BUILTIN,
    UCX_HICOLL,
    UCX_MODULE_LAST,
} ucx_module_type_t;

typedef struct ucg_planc_ucx_config_bundle {
    ucg_config_field_t *table;
    char data[];
} ucg_planc_ucx_config_bundle_t;

typedef struct ucg_planc_ucx_config {
    ucg_planc_ucx_config_bundle_t *config_bundle[UCG_COLL_TYPE_LAST][UCX_MODULE_LAST];

    /** Attributes of collective operation plans */
    char *plan_attr[UCG_COLL_TYPE_LAST];

    int n_polls;
    int estimated_num_eps;
    int estimated_num_ppn;
    ucg_ternary_auto_value_t use_oob;
    ucg_config_names_array_t planm;
} ucg_planc_ucx_config_t;

typedef struct ucg_planc_ucx_resource_planm {
    ucg_planm_t *planm;
} ucg_planc_ucx_resource_planm_t;

typedef struct ucg_planc_ucx_context {
    ucg_context_t *ucg_context;
    ucg_planc_ucx_config_t config;

    /* something related UCP */
    ucp_context_h ucp_context;
    ucp_worker_h ucp_worker;
    size_t ucp_addrlen;
    ucp_address_t *worker_address;

    /* The length of the eps array is determined by @ref ucg_oob_group_t::size */
    ucp_ep_h *eps;

    /* pool of @ref ucg_planc_ucx_op_t */
    ucg_mpool_t op_mp;

    int32_t num_planm_rscs;
    ucg_planc_ucx_resource_planm_t *planm_rscs;
} ucg_planc_ucx_context_t;

#define UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(_context, _coll_name, _coll_type) \
    (ucg_planc_ucx_##_coll_name##_config_t *)UCG_PLANC_UCX_CONTEXT_CONFIG_BUNDLE(_context, _coll_name, _coll_type, UCX_BUILTIN)

#define UCG_PLANC_UCX_CONTEXT_CONFIG_BUNDLE(_context, _coll_name, _coll_type, _module_type) \
    _context->config.config_bundle[_coll_type][_module_type]->data
/** Configuration */
ucg_status_t ucg_planc_ucx_config_read(const char *env_prefix,
                                       const char *filename,
                                       ucg_planc_config_h *config);
ucg_status_t ucg_planc_ucx_config_modify(ucg_planc_config_h config,
                                         const char *name,
                                         const char *value);
void ucg_planc_ucx_config_release(ucg_planc_config_h config);

/** Context */
ucg_status_t ucg_planc_ucx_context_init(const ucg_planc_params_t *params,
                                        const ucg_planc_config_h config,
                                        ucg_planc_context_h *context);
void ucg_planc_ucx_context_cleanup(ucg_planc_context_h context);
ucg_status_t ucg_planc_ucx_context_query(ucg_planc_context_h context,
                                         ucg_planc_context_attr_t *attr);

#endif