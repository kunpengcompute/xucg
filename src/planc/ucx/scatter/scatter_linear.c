/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "scatter.h"
#include "planc_ucx_plan.h"

enum {
    UCG_SCATTER_LINEAR_RECV = UCG_BIT(0),
    UCG_SCATTER_LINEAR_SEND = UCG_BIT(1),
};

#define UCG_SCATTER_LINEAR_FLAGS UCG_SCATTER_LINEAR_RECV | UCG_SCATTER_LINEAR_SEND

enum {
    UCG_SCATTER_SEND_TYPE_ONE_BY_ONE,
    UCG_SCATTER_SEND_TYPE_BATCH,
};

#define SINGLE_PROCESS_SEND_BATCH_LOWER_BOUND 4096
#define SINGLE_PROCESS_SEND_BATCH_UPPER_BOUND 65536
#define SINGLE_NODE_SEND_BATCH_LOWER_BOUND    8256
#define SINGLE_NODE_SEND_BATCH_UPPER_BOUND    SIZE_MAX

static ucg_status_t ucg_planc_ucx_scatter_linear_op_root_send_one_by_one(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_coll_scatter_args_t *args = &op->super.super.args.scatter;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    uint32_t group_size = vgroup->size;
    int *idx = &op->scatter.linear.idx;
    int64_t sendtype_extent = ucg_dt_extent(args->sendtype);
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);

    while (*idx < group_size) {
        if (ucg_test_and_clear_flags(&op->flags, UCG_SCATTER_LINEAR_SEND)) {
            void *sbuf = (char*)args->sendbuf + args->sendcount * (*idx) * sendtype_extent;
            int32_t scount = args->sendcount;
            if (*idx == args->root) {
                if (scount > 0 && args->recvbuf != UCG_IN_PLACE) {
                    status = ucg_dt_memcpy(args->recvbuf, args->recvcount, args->recvtype,
                                           sbuf, scount, args->sendtype);
                }
            } else {
                if (scount > 0) {
                    status = ucg_planc_ucx_p2p_isend(sbuf, scount, args->sendtype, *idx,
                                                     op->tag, vgroup, &params);
                }
            }
            UCG_CHECK_GOTO(status, out);
        }
        status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
        UCG_CHECK_GOTO(status, out);
        op->flags |= UCG_SCATTER_LINEAR_SEND;
        (*idx)++;
    }
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_scatter_linear_op_root_send_batch(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_coll_scatter_args_t *args = &op->super.super.args.scatter;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    uint32_t group_size = vgroup->size;
    int64_t sendtype_extent = ucg_dt_extent(args->sendtype);
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);

    if (ucg_test_and_clear_flags(&op->flags, UCG_SCATTER_LINEAR_SEND)) {
        for (int i = 0; i < group_size; ++i) {
            void *sbuf = (char*)args->sendbuf + args->sendcount * i * sendtype_extent;
            int32_t scount = args->sendcount;
            if (i == args->root) {
                if (scount > 0 && args->recvbuf != UCG_IN_PLACE) {
                    status = ucg_dt_memcpy(args->recvbuf, args->recvcount, args->recvtype,
                                           sbuf, scount, args->sendtype);
                }
            } else {
                if (scount > 0) {
                    status = ucg_planc_ucx_p2p_isend(sbuf, scount, args->sendtype, i,
                                                     op->tag, vgroup, &params);
                }
            }
            UCG_CHECK_ERR_GOTO(status, out);
        }
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
    UCG_CHECK_GOTO(status, out);

out:
    return status;
}

static ucg_status_t ucg_planc_ucx_scatter_linear_op_non_root_recv(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_coll_scatter_args_t *args = &op->super.super.args.scatter;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);

    if (ucg_test_and_clear_flags(&op->flags, UCG_SCATTER_LINEAR_RECV)) {
        if (args->recvcount > 0) {
            status = ucg_planc_ucx_p2p_irecv(args->recvbuf, args->recvcount,
                                             args->recvtype, args->root, op->tag,
                                             vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
out:
    return status;
}

ucg_status_t ucg_planc_ucx_scatter_linear_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_coll_scatter_args_t *args = &op->super.super.args.scatter;
    ucg_rank_t myrank = op->super.vgroup->myrank;
    if (myrank == args->root) {
        status = (op->scatter.linear.send_type == UCG_SCATTER_SEND_TYPE_ONE_BY_ONE) ?
                 ucg_planc_ucx_scatter_linear_op_root_send_one_by_one(op) :
                 ucg_planc_ucx_scatter_linear_op_root_send_batch(op);
    } else {
        status = ucg_planc_ucx_scatter_linear_op_non_root_recv(op);
    }
    op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_scatter_linear_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_op_reset(op);
    op->scatter.linear.idx = 0;
    op->flags = UCG_SCATTER_LINEAR_FLAGS;
    status = ucg_planc_ucx_scatter_linear_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}

static void
ucg_planc_ucx_scatter_linear_bsend_threshold(const ucg_planc_ucx_scatter_config_t *config,
                                              ucg_vgroup_t *vgroup,
                                              size_t *min, size_t *max)
{
    int32_t ppn = vgroup->group->topo->ppn;
    int32_t nnode = vgroup->group->topo->detail.nnode;

    *min = config->min_bsend;
    *max = config->max_bsend;

    if (config->min_bsend == UCG_MEMUNITS_INF) {
        *max = UCG_MEMUNITS_INF;
    }

    /* Experience test result */
    if (config->min_bsend == UCG_MEMUNITS_AUTO) {
        if (nnode == 1) {
            *min = SINGLE_NODE_SEND_BATCH_LOWER_BOUND;
        } else if (ppn == 1) {
            *min = SINGLE_PROCESS_SEND_BATCH_LOWER_BOUND;
        } else if (ppn > 1) {
            if (nnode <= 4) {
                *min = 32768;
            } else if (nnode <= 8 && ppn <= 32) {
                *min = 16384;
            } else if (nnode <= 8) {
                *min = 32768;
            } else if (nnode <= 16 && ppn <= 4) {
                *min = 16384;
            } else {
                *min = 32768;
            }
        } else {
            *min = UCG_MEMUNITS_INF;
        }
    }

    /* Experience test result */
    if (config->max_bsend == UCG_MEMUNITS_AUTO) {
        if (nnode == 1) {
            *max = SINGLE_NODE_SEND_BATCH_UPPER_BOUND;
        } else if (ppn == 1) {
            *max = SINGLE_PROCESS_SEND_BATCH_UPPER_BOUND;
        } else if (ppn > 1) {
            if (nnode <= 4) {
                *max = UCG_MEMUNITS_INF;
            } else if (nnode <= 8 && ppn <= 4) {
                *max = 32768;
            } else if (nnode <= 8 && ppn <= 8) {
                *max = 65536;
            } else if (nnode <= 8 && ppn <= 32) {
                *max = 16384;
            } else if (nnode <= 8) {
                *max = UCG_MEMUNITS_INF;
            } else if (nnode <= 16 && ppn <= 4) {
                *max = 16384;
            } else {
                *max = 131072;
            }
        } else {
            *max = UCG_MEMUNITS_INF;
        }
    }
}

static void ucg_planc_ucx_scatter_linear_op_init(ucg_planc_ucx_op_t *op,
                                                  const ucg_planc_ucx_scatter_config_t *config)
{
    ucg_coll_scatter_args_t *args = &op->super.super.args.scatter;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    if (myrank != args->root) {
        return;
    }

    size_t min_bsend;
    size_t max_bsend;
    ucg_planc_ucx_scatter_linear_bsend_threshold(config, vgroup,
                                                  &min_bsend, &max_bsend);

    uint8_t send_type;
    uint32_t group_size = vgroup->size;
    uint64_t dt_size = ucg_dt_extent(args->sendtype);
    uint64_t total_msg_size = 0;
    for (int i = 0; i < group_size; ++i) {
        total_msg_size += dt_size * args->sendcount;
    }
    uint64_t avg_size = total_msg_size / group_size;
    if (avg_size < min_bsend || avg_size > max_bsend) {
        send_type = UCG_SCATTER_SEND_TYPE_ONE_BY_ONE;
    } else {
        send_type = UCG_SCATTER_SEND_TYPE_BATCH;
    }
    op->scatter.linear.send_type = send_type;
    ucg_info("scatter linear send type: %s",
             send_type == UCG_SCATTER_SEND_TYPE_ONE_BY_ONE ? "one by one" : "batch");

    return;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_scatter_linear_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                         ucg_vgroup_t *vgroup,
                                                         const ucg_coll_args_t *args,
                                                         const ucg_planc_ucx_scatter_config_t *config)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args);

    ucg_planc_ucx_op_t *ucx_op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (ucx_op == NULL) {
        goto err;
    }

    ucg_status_t status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &ucx_op->super, vgroup,
                                              ucg_planc_ucx_scatter_linear_op_trigger,
                                              ucg_planc_ucx_scatter_linear_op_progress,
                                              ucg_planc_ucx_op_discard,
                                              args);

    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }

    ucg_planc_ucx_op_init(ucx_op, ucx_group);
    ucg_planc_ucx_scatter_linear_op_init(ucx_op, config);
    return ucx_op;

err_free_op:
    ucg_mpool_put(ucx_op);
err:
    return NULL;
}

ucg_status_t ucg_planc_ucx_scatter_linear_prepare(ucg_vgroup_t *vgroup,
                                                   const ucg_coll_args_t *args,
                                                   ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_scatter_config_t *config;
    config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, scatter,
                                                         UCG_COLL_TYPE_SCATTER);
    ucg_planc_ucx_op_t *linear_op;
    linear_op = ucg_planc_ucx_scatter_linear_op_new(ucx_group, vgroup, args, config);
    if (linear_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &linear_op->super;
    return UCG_OK;
}
