/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2020.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_CONTEXT_H_
#define UCG_CONTEXT_H_

#include <ucg/api/ucg.h>
#include <ucg/base/ucg_component.h>
#include <uct/api/uct.h>
#include <ucs/config/types.h>
#include <ucs/datastruct/list.h>
#include <ucs/debug/log.h>

#define UCG_CHECK_REQUIRED_FIELD(_mask, _field, _label) \
    if (!(_mask & _field)) { \
        ucs_error("The field \"%s\" is required", #_field); \
        goto _label; \
    }

typedef struct ucg_config {
    ucs_config_names_array_t  planners;
    char                     *env_prefix;
} ucg_config_t;

typedef struct ucg_context {
    ucg_params_t     params;
    ucs_list_link_t  groups_head;
    ucs_list_link_t  planners_head; /* planner with high priority is in the front */
    ucg_planc_ctx_h *planc_ctx; /* number is length(planners_head) */
} ucg_context_t;

ucs_status_t ucg_context_set_am_handler(uct_am_callback_t cb,
                                        void* arg,
                                        uct_am_tracer_t tracer,
                                        uint8_t *am_id);

#endif