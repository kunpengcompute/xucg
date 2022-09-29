/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_HCCL_CONTEXT_H_
#define UCG_PLANC_HCCL_CONTEXT_H_

#include <hccl/hccl.h>

#include "ucg/api/ucg.h"
#include "core/ucg_request.h"
#include "planc/ucg_planc.h"
#include "util/ucg_mpool.h"


typedef struct ucg_planc_hccl_config {
    int whitelist_disable;
    const char *whitelist_file;
    /** Attributes of collective operation plans */
    char *plan_attr[UCG_COLL_TYPE_LAST];
} ucg_planc_hccl_config_t;

typedef struct ucg_planc_hccl_context {
    ucg_context_t *ucg_context;
    aclrtContext acl_context;

    /** pool of @ref ucg_planc_hccl_op_t */
    ucg_mpool_t op_mp;
    /** User-defined plan attribute */
    char *plan_attr[UCG_COLL_TYPE_LAST];
} ucg_planc_hccl_context_t;

/** Configuration */
ucg_status_t ucg_planc_hccl_config_read(const char *env_prefix,
                                        const char *filename,
                                        ucg_planc_config_h *config);
ucg_status_t ucg_planc_hccl_config_modify(ucg_planc_config_h config,
                                          const char *name,
                                          const char *value);
void ucg_planc_hccl_config_release(ucg_planc_config_h config);

/** Context */
ucg_status_t ucg_planc_hccl_context_init(const ucg_planc_params_t *params,
                                         const ucg_planc_config_h config,
                                         ucg_planc_context_h *context);
void ucg_planc_hccl_context_cleanup(ucg_planc_context_h context);
ucg_status_t ucg_planc_hccl_context_query(ucg_planc_context_h context,
                                          ucg_planc_context_attr_t *attr);

#endif