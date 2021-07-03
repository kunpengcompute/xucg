/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_MINIMAL_H_
#define UCG_MINIMAL_H_

#include <ucg/api/ucg.h>

BEGIN_C_DECLS

/*
 * Below is a minimal API for broadcasting messages using UCG.
 */

typedef struct ucg_minimal_ctx {
    ucg_context_h       context;
    ucp_worker_h        worker;
    ucg_group_h         group;
    ucg_listener_h      listener;
} ucg_minimal_ctx_t;

enum ucg_minimal_init_flags {
    UCG_MINIMAL_FLAG_SERVER = UCS_BIT(0) /* otherwise act as a client */
};

static UCS_F_ALWAYS_INLINE ucs_status_t
ucg_minimal_init(ucg_minimal_ctx_t *ctx,
                 ucs_sock_addr_t *server_address,
                 unsigned num_connections_to_wait,
                 uint64_t flags)
{
    ucs_status_t status;
    ucg_config_t *context_config;
    ucp_worker_params_t worker_params;
    ucp_params_t ucp_context_params = {0};
    ucg_params_t ucg_context_params = {0};
    int is_server                   = (flags & UCG_MINIMAL_FLAG_SERVER);
    ucp_context_params.field_mask   = UCP_PARAM_FIELD_FEATURES;
    ucp_context_params.features     = UCP_FEATURE_GROUPS;
    ucg_context_params.super        = &ucp_context_params;
    ucg_context_params.field_mask   = UCG_PARAM_FIELD_ADDRESS_CB;
    ucg_group_attr_t group_attr     = {
            .field_mask             = UCG_GROUP_ATTR_FIELD_MEMBER_COUNT
    };
    ucg_group_params_t group_params = {
            .field_mask             = UCG_GROUP_PARAM_FIELD_MEMBER_COUNT |
                                      UCG_GROUP_PARAM_FIELD_MEMBER_INDEX,
            .member_index           = !is_server,
            .member_count           = 1 + !is_server
    };

    status = ucg_config_read(NULL, NULL, &context_config);
    if (status != UCS_OK) {
        return status;
    }

    status = ucg_init(&ucg_context_params, context_config, &ctx->context);
    ucg_config_release(context_config);
    if (status != UCS_OK) {
        return status;
    }

    status = ucp_worker_create(ucg_context_get_ucp(ctx->context),
                               &worker_params, &ctx->worker);
    if (status != UCS_OK) {
        goto cleanup_context;
    }

    status = ucg_group_create(ctx->worker, &group_params, &ctx->group);
    if (status != UCS_OK) {
        goto cleanup_worker;
    }

    if (is_server) {
        status = ucg_group_listener_connect(ctx->group, server_address);
        if (status != UCS_OK) {
            goto cleanup_group;
        }

        return UCS_OK;
    }

    status = ucg_group_listener_create(ctx->group, server_address, &ctx->listener);
    if (status != UCS_OK) {
        goto cleanup_group;
    }

    do {
        status = ucp_worker_progress(ctx->worker);
        if (status != UCS_OK) {
            goto cleanup_listener;
        }

        status = ucg_group_query(ctx->group, &group_attr);
        if (status != UCS_OK) {
            goto cleanup_listener;
        }
    } while (group_attr.member_count < (num_connections_to_wait + 1));

    ucg_group_listener_destroy(ctx->listener);

    return UCS_OK;

cleanup_listener:
    ucg_group_listener_destroy(ctx->listener);

cleanup_group:
    ucg_group_destroy(ctx->group);

cleanup_worker:
    ucp_worker_destroy(ctx->worker);

cleanup_context:
    ucg_cleanup(ctx->context);
    return status;
}

static UCS_F_ALWAYS_INLINE void
ucg_minimal_finalize(ucg_minimal_ctx_t *ctx)
{
    ucg_group_destroy(ctx->group);
    ucp_worker_destroy(ctx->worker);
    ucg_cleanup(ctx->context);
}

static UCS_F_ALWAYS_INLINE ucs_status_t
ucg_minimal_broadcast(ucg_minimal_ctx_t *ctx, void *buffer, size_t length)
{
    int is_done = 0;
    ucg_coll_h collh;
    ucs_status_t status;
    ucg_collective_params_t bcast_params = {
        .send = {
            .type = {
                .modifiers = UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST |
                             UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_SOURCE,
                .root      = 0
            },
            .buffer        = buffer,
            .count         = length,
            .dtype         = (void*)ucp_dt_make_contig(1)
        },
        .recv = {
            .buffer        = buffer,
            .count         = length,
            .dtype         = (void*)ucp_dt_make_contig(1)
        }
    };

    status = ucg_collective_create(ctx->group, &bcast_params, &collh);
    if (status != UCS_OK) {
        return status;
    }

    status = ucg_collective_start(collh, &is_done);
    if (status != UCS_OK) {
        if (status != UCS_INPROGRESS) {
            goto cleanup_bcast;
        }
        status = UCS_OK;

        while (!is_done) {
            ucp_worker_progress(ctx->worker);
        }
    }

cleanup_bcast:
    ucg_collective_destroy(collh);
    return status;
}

END_C_DECLS

#endif
