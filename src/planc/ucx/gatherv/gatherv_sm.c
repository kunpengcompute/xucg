/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

#include "gatherv.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_p2p.h"
#include "core/ucg_dt.h"
#include "core/ucg_group.h"
#include "util/ucg_log.h"
#include "util/ucg_malloc.h"

#define ucs_aarch64_dmb(_op)            asm volatile ("dmb " #_op ::: "memory")
#define ucs_memory_cpu_store_fence()    ucs_aarch64_dmb(ishst)
#define ucs_memory_cpu_load_fence()     ucs_aarch64_dmb(ishld)

enum {
    UCG_BCAST_ADJUST_ROOT           = UCG_BIT(0),
    UCG_BCAST_ADJUST_ROOT_SEND      = UCG_BIT(1),
    UCG_BCAST_ADJUST_ROOT_RECV      = UCG_BIT(2),
    UCG_BCAST_RECV_FROM_PARENT      = UCG_BIT(3),
    UCG_BCAST_RECV_FROM_PARENT_RECV = UCG_BIT(4),
};

#define UCG_BCAST_KNTREE_FLAGS ( UCG_BCAST_ADJUST_ROOT | \
                                 UCG_BCAST_ADJUST_ROOT_SEND | \
                                 UCG_BCAST_ADJUST_ROOT_RECV | \
                                 UCG_BCAST_RECV_FROM_PARENT | \
                                 UCG_BCAST_RECV_FROM_PARENT_RECV )

enum {
    UCG_GATHERV_SM_WRITE_DISPLS     = UCG_BIT(5),
    UCG_GATHERV_SM_BCAST            = UCG_BIT(6),
    UCG_GATHERV_SM_GATHERV          = UCG_BIT(7),
};

#define UCG_GATHERV_SM_BCAST_FLAGS  ( UCG_BCAST_KNTREE_FLAGS | \
                                      UCG_GATHERV_SM_BCAST )
                        
#define UCG_GATHERV_SM_FLAGS        ( UCG_GATHERV_SM_WRITE_DISPLS | \
                                      UCG_GATHERV_SM_BCAST_FLAGS | \
                                      UCG_GATHERV_SM_GATHERV )

enum {
    SHMEM_SYNC_INIT      = 0,
    SHMEM_SYNC_FINISH    = 1,
};

#define GROUP_DISPLS_COUNT 1

static ucg_status_t ucg_planc_ucx_gatherv_sm_check(const ucg_vgroup_t *vgroup)
{
    ucg_topo_group_t *intra_group;

    intra_group = ucg_topo_get_group(vgroup->group->topo, UCG_TOPO_GROUP_TYPE_NODE);
    if (intra_group == NULL || intra_group->state == UCG_TOPO_GROUP_STATE_ERROR) {
        return UCG_ERR_UNSUPPORTED;
    }

    if (vgroup->size != intra_group->super.size) {
        ucg_info("Gatherv sm don't support inter-node communication.");
        return UCG_ERR_UNSUPPORTED;
    }

    return UCG_OK;
}

void ucg_planc_ucx_gatherv_sm_set_mp(ucg_planc_ucx_op_t *ucx_op,
                                     ucg_shmem_pool_t *shmem_pool,
                                     uint32_t is_extern_mp)
{
    ucg_planc_ucx_gatherv_sm_args_t *algo_args = &ucx_op->gatherv.sm_args;
    algo_args->shmem_pool = shmem_pool;
    algo_args->is_extern_mp = is_extern_mp;
}

void ucg_planc_ucx_gatherv_sm_set_allgatherv_like(ucg_planc_ucx_op_t *ucx_op,
                                                  uint32_t is_allgatherv_like)
{
    ucg_planc_ucx_gatherv_sm_args_t *algo_args = &ucx_op->gatherv.sm_args;
    algo_args->is_allgatherv_like = is_allgatherv_like;
}

static void ucg_planc_ucx_gatherv_sm_init(ucg_planc_ucx_op_t *ucx_op,
                                          const ucg_coll_args_t *coll_args)
{
    ucg_planc_ucx_gatherv_sm_args_t *algo_args = &ucx_op->gatherv.sm_args;
    algo_args->origin_coll_args = ucx_op->super.super.args;
    algo_args->shmem_segment = NULL;
    algo_args->is_initialized = 0;
    ucg_planc_ucx_gatherv_sm_set_mp(ucx_op, NULL, 0);
    ucg_planc_ucx_gatherv_sm_set_allgatherv_like(ucx_op, 0);
}

static void ucg_planc_ucx_gatherv_sm_init_flags(int *flags, int group_size, int value)
{
    for (int i = 0; i < group_size * GROUP_DISPLS_COUNT; i++) {
        flags[i] = value;
    }
}

static ucg_status_t ucg_planm_ucx_gatherv_sm_notify_root(int *flags, int group_size, int rank, int root)
{
    /* central counter */
    if (root == rank) {
        for (int i = 0; i < group_size; i++) {
            if (rank != i) {
                while (flags[i * GROUP_DISPLS_COUNT] != SHMEM_SYNC_FINISH) {
                    ucs_memory_cpu_load_fence();
                }
            }
        }
    } else {
        flags[rank * GROUP_DISPLS_COUNT] = SHMEM_SYNC_FINISH;
    }

    return UCG_OK;
}

static ucg_status_t ucg_planm_ucx_gatherv_sm_notify_others(int *flags, int group_size, int rank, int root)
{
    /* central counter */
    if (root == rank) {
        flags[rank * GROUP_DISPLS_COUNT] = SHMEM_SYNC_FINISH;
        ucs_memory_cpu_store_fence();
    } else {
        while (flags[root * GROUP_DISPLS_COUNT] != SHMEM_SYNC_FINISH) {
            ucs_memory_cpu_load_fence();
        }
    }

    return UCG_OK;
}

static int *ucg_planc_ucx_gatherv_sm_flags(ucg_shmem_segment_t *shmseg, int group_size)
{
    char *valid_buf = ((char *)shmseg->seg_base_addr) + shmseg->start_addr_disp + sizeof(ucg_shmem_segment_t);
    int *flags = (int *)valid_buf;

    return flags;
}

static int *ucg_planc_ucx_gatherv_sm_displs(ucg_shmem_segment_t *shmseg, int group_size)
{
    char *valid_buf = ((char *)shmseg->seg_base_addr) + shmseg->start_addr_disp + sizeof(ucg_shmem_segment_t);
    int *displs = (int *)valid_buf + group_size * GROUP_DISPLS_COUNT;

    return displs;
}

static void *ucg_planc_ucx_gatherv_sm_recvbuf(ucg_shmem_segment_t *shmseg, int group_size)
{
    char *valid_buf = ((char *)shmseg->seg_base_addr) + shmseg->start_addr_disp + sizeof(ucg_shmem_segment_t);
    int *recvbuf_temp = (int *)valid_buf + group_size * (GROUP_DISPLS_COUNT + 1);
    void * recvbuf = (void *)recvbuf_temp;

    return recvbuf;
}

static inline ucg_status_t ucg_planc_ucx_gatherv_sm_get_shmseg(ucg_mpool_t *mp, ucg_shmem_segment_t *shmseg)
{
    if (shmseg == NULL || mp == NULL) {
        return UCG_ERR_INVALID_PARAM;
    }

    void *obj = ucg_mpool_get(mp);
    UCG_CHECK_NULL(UCG_ERR_NO_MEMORY, obj);

    memcpy(shmseg, obj, sizeof(ucg_shmem_segment_t));
    shmseg->start_addr_disp = (char *)obj - (char *)shmseg->seg_base_addr;

    ucg_mpool_put(obj);

    return UCG_OK;
}

void *ucg_planc_ucx_gatherv_get_recvbuf_by_mp(ucg_shmem_pool_t *shmem_pool, int group_size)
{
    ucg_status_t status;
    ucg_shmem_segment_t shmseg;

    status = ucg_planc_ucx_gatherv_sm_get_shmseg(&shmem_pool->super, &shmseg);
    if (status != UCG_OK) {
        return NULL;
    }

    void *recvbuf = ucg_planc_ucx_gatherv_sm_recvbuf(&shmseg, group_size);
    return recvbuf;
}

static void ucg_planm_ucx_gatherv_sm_bcast_prepare(ucg_planc_ucx_op_t *ucx_op)
{
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_planc_ucx_gatherv_sm_args_t *algo_args = &ucx_op->gatherv.sm_args;
    algo_args->phase_bcast.type = UCG_COLL_TYPE_BCAST;

    ucg_coll_bcast_args_t *bcast_args = &algo_args->phase_bcast.bcast;
    bcast_args->buffer = (void *)&algo_args->shmem_remote_fd;
    bcast_args->count = sizeof(ucg_shmem_remote_fd_t);
    bcast_args->dt = ucg_dt_get_predefined(UCG_DT_TYPE_INT8);
    bcast_args->root = 0;

    ucx_op->super.super.args = algo_args->phase_bcast;

    ucg_rank_t group_rank = vgroup->myrank;
    uint32_t group_size = vgroup->size;
    ucg_algo_kntree_iter_t *iter = &ucx_op->bcast.kntree_iter;
    ucg_algo_kntree_iter_init(iter, group_size, 4, 0, group_rank, 1);
    ucg_algo_kntree_iter_reset(iter);

    return;
}

ucg_status_t ucg_planc_ucx_gatherv_sm_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_context_t *ctx = vgroup->group->context;
    ucg_rank_t myrank = vgroup->myrank;
    uint32_t group_size = vgroup->size;

    ucg_planc_ucx_gatherv_sm_args_t *algo_args = &ucx_op->gatherv.sm_args;
    ucg_shmem_segment_t *shmem_segment = algo_args->shmem_segment;
    uint32_t is_extern_mp = algo_args->is_extern_mp;
    uint32_t is_allgatherv_like = algo_args->is_allgatherv_like;

    ucg_coll_gatherv_args_t *gatherv_args = &algo_args->origin_coll_args.gatherv;
    const void *sendbuf = gatherv_args->sendbuf;
    void *recvbuf = gatherv_args->recvbuf;
    int32_t sendcount = gatherv_args->sendcount;
    const int32_t *recvcounts = gatherv_args->recvcounts;
    int32_t root = gatherv_args->root;
    ucg_dt_t *sendtype = gatherv_args->sendtype;
    uint64_t stype_len = (uint64_t)ucg_dt_extent(sendtype);

    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(ucx_op, &params);

    if (ucg_test_and_clear_flags(&ucx_op->flags, UCG_GATHERV_SM_WRITE_DISPLS)) {
        if (myrank == gatherv_args->root) {
            int *flags = ucg_planc_ucx_gatherv_sm_flags(shmem_segment, group_size);
            ucg_planc_ucx_gatherv_sm_init_flags(flags, group_size, SHMEM_SYNC_INIT);

            int *displs = ucg_planc_ucx_gatherv_sm_displs(shmem_segment, group_size);
            memcpy(displs, gatherv_args->displs, group_size * sizeof(int));
        }
    }

    if (ucg_test_flags(ucx_op->flags, UCG_GATHERV_SM_BCAST)) {
        if (algo_args->is_initialized == 0) {
            ucg_planm_ucx_gatherv_sm_bcast_prepare(ucx_op);
            algo_args->is_initialized = 1;
        }
        
        status = ucg_planc_ucx_bcast_kntree_op_progress(&ucx_op->super);
        if (status != UCG_OK) {
            if (status != UCG_INPROGRESS) {
                ucg_error("gatherv bcast failed");
            }
            goto out;
        }

        ucg_clear_flags(&ucx_op->flags, UCG_GATHERV_SM_BCAST);
        algo_args->is_initialized = 0;
    }

    if (ucg_test_and_clear_flags(&ucx_op->flags, UCG_GATHERV_SM_GATHERV)) {
        if (myrank != root) {
            ucg_shmem_remote_fd_t *shmem_remote_fd = &algo_args->shmem_remote_fd;
            algo_args->shmem_segment = ucg_shmem_segment_get(&ctx->shmem_segment_list, shmem_remote_fd);
        }

        int *flags = ucg_planc_ucx_gatherv_sm_flags(algo_args->shmem_segment, group_size);
        int *displs = ucg_planc_ucx_gatherv_sm_displs(algo_args->shmem_segment, group_size);
        void *temp_recvbuf = ucg_planc_ucx_gatherv_sm_recvbuf(algo_args->shmem_segment, group_size);

        memcpy(temp_recvbuf + displs[myrank] * stype_len, sendbuf, sendcount * stype_len);
        ucg_planm_ucx_gatherv_sm_notify_root(flags, group_size, myrank, root);
        
        if (is_allgatherv_like) {
            /* for allgatherv_like condition, children should wait for root */
            ucg_planm_ucx_gatherv_sm_notify_others(flags, group_size, myrank, root);
            gatherv_args->recvbuf = temp_recvbuf;
        }

        if (myrank == root && (!is_extern_mp)) {
            ucg_dt_t *recvtype = gatherv_args->recvtype;
            uint64_t rtype_len = (uint64_t)ucg_dt_extent(recvtype);

            for (int i = 0; i < group_size; i++) {
                memcpy(recvbuf + displs[i] * rtype_len, temp_recvbuf + displs[i] * rtype_len, recvcounts[i] * rtype_len);
            }
        }

        ucg_clear_flags(&ucx_op->flags, UCG_GATHERV_SM_GATHERV);
        ucx_op->super.super.args = algo_args->origin_coll_args;

        if (myrank == root) {
            if (!is_extern_mp) {
                ucg_list_shmem_pool_put(&ctx->shmem_mp_list, algo_args->shmem_pool);
            }
        } else {
            status = ucg_shmem_segment_put(&ctx->shmem_segment_list, algo_args->shmem_segment);
        }
    }

out:
    ucx_op->super.super.status = status;
    return status;
}

static inline int ucg_planc_ucx_gatherv_sm_is_root(const ucg_vgroup_t *vgroup,
                                                   const ucg_coll_gatherv_args_t *gatherv_args)
{
    return (vgroup->myrank == gatherv_args->root) ? 1 : 0;
}

static inline size_t ucg_planc_ucx_gatherv_sm_estimate_mp_size(const ucg_vgroup_t *vgroup,
                                                               const ucg_coll_gatherv_args_t *gatherv_args)
{
    size_t buf_len = 0;
    uint64_t recv_dt_size = ucg_dt_extent(gatherv_args->recvtype);
    uint32_t group_size = vgroup->size;

    for (uint32_t i = 0; i < group_size; i++) {
        buf_len += gatherv_args->recvcounts[i];
    }
    buf_len *= recv_dt_size;

    buf_len += group_size * sizeof(int) * GROUP_DISPLS_COUNT;
    buf_len += group_size * sizeof(int);

    return buf_len;
}

static ucg_status_t ucg_planc_ucx_gatherv_sm_check_mp(ucg_planc_ucx_op_t *ucx_op)
{
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_context_t *ctx = vgroup->group->context;
    ucg_coll_gatherv_args_t *gatherv_args = &ucx_op->super.super.args.gatherv;
    ucg_planc_ucx_gatherv_sm_args_t *algo_args = &ucx_op->gatherv.sm_args;
    ucg_coll_args_t *origin_coll_args = &algo_args->origin_coll_args;
    ucg_shmem_remote_fd_t *shmem_remote_fd = &algo_args->shmem_remote_fd;

    if (algo_args->is_extern_mp == 0) {
        size_t buf_len = ucg_planc_ucx_gatherv_sm_estimate_mp_size(vgroup, gatherv_args);
        ucg_shmem_pool_params_t shmem_pool_params;
        shmem_pool_params.length = buf_len;
        ucg_shmem_pool_t *shmem_pool = ucg_list_shmem_pool_get(&ctx->shmem_mp_list, &shmem_pool_params);
        algo_args->shmem_pool = shmem_pool;
    }
    algo_args->shmem_segment = (ucg_shmem_segment_t *)malloc(sizeof(ucg_shmem_segment_t));
    if (algo_args->shmem_segment == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    ucg_status_t status = ucg_planc_ucx_gatherv_sm_get_shmseg(&algo_args->shmem_pool->super, algo_args->shmem_segment);
    if (status != UCG_OK) {
        return status;
    }

    shmem_remote_fd->seg_size = algo_args->shmem_segment->seg_size;
    shmem_remote_fd->start_addr_disp = algo_args->shmem_segment->start_addr_disp;
    shmem_remote_fd->actual_addr_disp = algo_args->shmem_segment->actual_addr_disp;
    memcpy(shmem_remote_fd->seg_name, algo_args->shmem_segment->seg_name, SHM_PATH_MAX);

    if (algo_args->is_extern_mp != 0) {
        origin_coll_args->gatherv.recvbuf = ucg_planc_ucx_gatherv_sm_recvbuf(algo_args->shmem_segment, vgroup->size);
    }

    return status;
}

static ucg_status_t ucg_planc_ucx_gatherv_sm_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_coll_gatherv_args_t *gatherv_args = &ucx_op->super.super.args.gatherv;
    ucg_planc_ucx_op_reset(ucx_op);
    ucx_op->flags = UCG_GATHERV_SM_FLAGS;

    int is_root = ucg_planc_ucx_gatherv_sm_is_root(vgroup, gatherv_args);
    if (is_root) {
        status = ucg_planc_ucx_gatherv_sm_check_mp(ucx_op);
        UCG_CHECK_STATUS(status, status);
    }

    status = ucg_planc_ucx_gatherv_sm_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_gatherv_sm_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                    ucg_vgroup_t *vgroup,
                                                    const ucg_coll_args_t *coll_args)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, coll_args);

    ucg_planc_ucx_op_t *ucx_op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (ucx_op == NULL) {
        goto err;
    }

    ucg_status_t status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &ucx_op->super, vgroup,
                                              ucg_planc_ucx_gatherv_sm_op_trigger,
                                              ucg_planc_ucx_gatherv_sm_op_progress,
                                              ucg_planc_ucx_op_discard,
                                              coll_args);
    UCG_CHECK_GOTO(status, err_free_op);

    ucg_planc_ucx_op_init(ucx_op, ucx_group);
    ucg_planc_ucx_gatherv_sm_init(ucx_op, coll_args);
    return ucx_op;

err_free_op:
    ucg_mpool_put(ucx_op);
err:
    return NULL;
}

ucg_status_t ucg_planc_ucx_gatherv_sm_prepare(ucg_vgroup_t *vgroup,
                                              const ucg_coll_args_t *coll_args,
                                              ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, coll_args, op);

    ucg_status_t status;
    status = ucg_planc_ucx_gatherv_sm_check(vgroup);
    UCG_CHECK_STATUS(status, status);

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_op_t *ucx_op = ucg_planc_ucx_gatherv_sm_op_new(ucx_group, vgroup, coll_args);
    UCG_CHECK_NULL(UCG_ERR_NO_MEMORY, ucx_op);
    *op = &ucx_op->super;
    return UCG_OK;
}

/* internal implementation for gatherw_ddt */
static void ucg_planc_ucx_gatherw_ddt_sm_init(ucg_planc_ucx_op_t *ucx_op,
                                              const ucg_ddt_args_t *gatherw_ddt_args)
{
    ucg_planc_ucx_gatherv_sm_args_t *algo_args = &ucx_op->gatherv.sm_args;
    algo_args->gatherw_ddt_args = gatherw_ddt_args;
}

ucg_status_t ucg_planc_ucx_gatherw_ddt_sm_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_context_t *ctx = vgroup->group->context;
    ucg_rank_t myrank = vgroup->myrank;
    uint32_t group_size = vgroup->size;

    ucg_planc_ucx_gatherv_sm_args_t *algo_args = &ucx_op->gatherv.sm_args;
    ucg_shmem_segment_t *shmem_segment = algo_args->shmem_segment;
    uint32_t is_extern_mp = algo_args->is_extern_mp;

    ucg_coll_gatherv_args_t *gatherv_args = &algo_args->origin_coll_args.gatherv;
    const void *sendbuf = gatherv_args->sendbuf;
    void *recvbuf = gatherv_args->recvbuf;
    const int32_t *recvcounts = gatherv_args->recvcounts;
    int32_t root = gatherv_args->root;
    ucg_dt_t *sendtype = gatherv_args->sendtype;
    uint64_t stype_len = (uint64_t)ucg_dt_extent(sendtype);

    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(ucx_op, &params);

    if (ucg_test_and_clear_flags(&ucx_op->flags, UCG_GATHERV_SM_WRITE_DISPLS)) {
        if (myrank == gatherv_args->root) {
            int *flags = ucg_planc_ucx_gatherv_sm_flags(shmem_segment, group_size);
            ucg_planc_ucx_gatherv_sm_init_flags(flags, group_size, SHMEM_SYNC_INIT);

            int *displs = ucg_planc_ucx_gatherv_sm_displs(shmem_segment, group_size);
            memcpy(displs, gatherv_args->displs, group_size * sizeof(int));
        }
    }

    if (ucg_test_flags(ucx_op->flags, UCG_GATHERV_SM_BCAST)) {
        if (algo_args->is_initialized == 0) {
            ucg_planm_ucx_gatherv_sm_bcast_prepare(ucx_op);
            algo_args->is_initialized =1;
        }

        status = ucg_planc_ucx_bcast_kntree_op_progress(&ucx_op->super);
        if (status != UCG_OK) {
            if (status != UCG_INPROGRESS) {
                ucg_error("gatherv bcast failed.");
            }
            goto out;
        }

        ucg_clear_flags(&ucx_op->flags, UCG_GATHERV_SM_BCAST);
        algo_args->is_initialized = 0;
    }

    if (ucg_test_and_clear_flags(&ucx_op->flags, UCG_GATHERV_SM_GATHERV)) {
        if (myrank != root) {
            ucg_shmem_remote_fd_t *shmem_remote_fd = &algo_args->shmem_remote_fd;
            algo_args->shmem_segment = ucg_shmem_segment_get(&ctx->shmem_segment_list, shmem_remote_fd);
        }

        int *flags = ucg_planc_ucx_gatherv_sm_flags(algo_args->shmem_segment, group_size);
        int *displs = ucg_planc_ucx_gatherv_sm_displs(algo_args->shmem_segment, group_size);
        void *temp_recvbuf = ucg_planc_ucx_gatherv_sm_recvbuf(algo_args->shmem_segment, group_size);

        int32_t count = algo_args->gatherw_ddt_args->type_indexed_args.count;
        const int64_t *array_of_blocklengths = algo_args->gatherw_ddt_args->type_indexed_args.array_of_blocklengths;
        const int64_t *array_of_displacements = algo_args->gatherw_ddt_args->type_indexed_args.array_of_displacements;
        uint64_t sdispls = 0;

        for (int32_t i = 0; i < count; i++) {
            memcpy(temp_recvbuf + array_of_displacements[i] * stype_len, sendbuf + sdispls, array_of_blocklengths[i] * stype_len);
            sdispls += array_of_blocklengths[i] * stype_len;
        }
        ucg_planm_ucx_gatherv_sm_notify_root(flags, group_size, myrank, root);

        if (myrank == root && (!is_extern_mp)) {
            ucg_dt_t *recvtype = gatherv_args->recvtype;
            uint64_t rtype_len = (uint64_t)ucg_dt_extent(recvtype);

            for (int i = 0; i < group_size; i++) {
                memcpy(recvbuf + displs[i] * rtype_len, temp_recvbuf + displs[i] * rtype_len, recvcounts[i] * rtype_len);
            }
        }

        ucg_clear_flags(&ucx_op->flags, UCG_GATHERV_SM_GATHERV);
        ucx_op->super.super.args = algo_args->origin_coll_args;

        if (myrank == root) {
            if (!is_extern_mp) {
                ucg_list_shmem_pool_put(&ctx->shmem_mp_list, algo_args->shmem_pool);
            }
        } else {
            status = ucg_shmem_segment_put(&ctx->shmem_segment_list, algo_args->shmem_segment);
        }
    }
out:
    ucx_op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_gatherw_ddt_sm_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_coll_gatherv_args_t *gatherv_args = &ucx_op->super.super.args.gatherv;

    ucg_planc_ucx_op_reset(ucx_op);
    ucx_op->flags = UCG_GATHERV_SM_FLAGS;

    int is_root = ucg_planc_ucx_gatherv_sm_is_root(vgroup, gatherv_args);
    if (is_root) {
        status = ucg_planc_ucx_gatherv_sm_check_mp(ucx_op);
        UCG_CHECK_STATUS(status, status);
    }

    status = ucg_planc_ucx_gatherw_ddt_sm_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_gatherw_ddt_sm_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                        ucg_vgroup_t *vgroup,
                                                        const ucg_coll_args_t *coll_args,
                                                        const ucg_ddt_args_t *gatherw_ddt_args)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, coll_args);

    ucg_planc_ucx_op_t *ucx_op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (ucx_op == NULL) {
        goto err;
    }

    ucg_status_t status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &ucx_op->super, vgroup,
                                              ucg_planc_ucx_gatherw_ddt_sm_op_trigger,
                                              ucg_planc_ucx_gatherw_ddt_sm_op_progress,
                                              ucg_planc_ucx_op_discard,
                                              coll_args);
    UCG_CHECK_GOTO(status, err_free_op);

    ucg_planc_ucx_op_init(ucx_op, ucx_group);
    ucg_planc_ucx_gatherv_sm_init(ucx_op, coll_args);
    ucg_planc_ucx_gatherw_ddt_sm_init(ucx_op, gatherw_ddt_args);
    return ucx_op;

err_free_op:
    ucg_mpool_put(ucx_op);
err:
    return NULL;
}