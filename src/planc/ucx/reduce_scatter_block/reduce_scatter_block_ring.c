/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 */

#include "reduce_scatter_block.h"
#include "planc_ucx_plan.h"
// #include "reduce_scatter_block_meta.h"

enum {
    UCG_REDUCE_SCATTER_BLOCK_RING_START = UCG_BIT(0),
    UCG_REDUCE_SCATTER_BLOCK_RING_SEND = UCG_BIT(1),
    UCG_REDUCE_SCATTER_BLOCK_RING_RECV = UCG_BIT(2),
    UCG_REDUCE_SCATTER_BLOCK_RING_PARAMS = UCG_BIT(3),
    UCG_REDUCE_SCATTER_BLOCK_RING_END = UCG_BIT(4),
};


#define UCG_GATHER_RING_FLAGS UCG_REDUCE_SCATTER_BLOCK_RING_END | UCG_REDUCE_SCATTER_BLOCK_RING_PARAMS | UCG_REDUCE_SCATTER_BLOCK_RING_START | UCG_REDUCE_SCATTER_BLOCK_RING_SEND | UCG_REDUCE_SCATTER_BLOCK_RING_RECV
#define UCG_GATHER_RING_ITER_FLAGS UCG_REDUCE_SCATTER_BLOCK_RING_SEND | UCG_REDUCE_SCATTER_BLOCK_RING_RECV


static inline ucg_status_t ucg_planc_ucx_reduce_scatter_block_ring_op_discard(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    
    if (op->reduce_scatter_block.ring.inbuf != NULL) {
        ucg_free(op->reduce_scatter_block.ring.inbuf);
        op->reduce_scatter_block.ring.inbuf = NULL;
    }
    return ucg_planc_ucx_op_discard(ucg_op);
}


ucg_status_t ucg_planc_ucx_reduce_scatter_block_ring_op_progress(ucg_plan_op_t *ucg_op)
{
    
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = op->super.vgroup->myrank;
    uint32_t group_size = vgroup->size;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    
    ucg_coll_reduce_scatter_block_args_t *args = &op->super.super.args.reduce_scatter_block;
    int64_t dt_extent = ucg_dt_extent(args->dt);
    ucg_rank_t send_to_rank = (myrank + 1) % group_size;
    ucg_rank_t recv_from_rank = (myrank + group_size - 1) % group_size;

    if (ucg_test_and_clear_flags(&op->flags, UCG_REDUCE_SCATTER_BLOCK_RING_PARAMS)) {
        if (args->sendbuf != UCG_IN_PLACE) {
            status = ucg_dt_memcpy(op->staging_area, op->reduce_scatter_block.ring.total_count, args->dt,
                                    args->sendbuf, op->reduce_scatter_block.ring.total_count, args->dt);
        } else {
            status = ucg_dt_memcpy(op->staging_area, op->reduce_scatter_block.ring.total_count, args->dt,
                                    args->recvbuf, op->reduce_scatter_block.ring.total_count, args->dt);
        }
        UCG_CHECK_GOTO(status, out);  
    }

    if (ucg_test_flags(op->flags, UCG_REDUCE_SCATTER_BLOCK_RING_START)) {
        int64_t offset = args->recvcount * recv_from_rank * dt_extent;
        status = ucg_planc_ucx_p2p_isend(op->staging_area + offset, args->recvcount,
                                            args->dt, send_to_rank, op->tag,
                                            vgroup, &params);
        UCG_CHECK_GOTO(status, out);
        status = ucg_planc_ucx_p2p_irecv(op->reduce_scatter_block.ring.inbuf, args->recvcount,
                                         args->dt,
                                         recv_from_rank, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_REDUCE_SCATTER_BLOCK_RING_START);
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
    UCG_CHECK_GOTO(status, out);
    
    while(op->reduce_scatter_block.ring.iter_idx != group_size) {
        
        if (ucg_test_and_clear_flags(&op->flags, UCG_REDUCE_SCATTER_BLOCK_RING_SEND)) {
            int block_idx = (myrank - op->reduce_scatter_block.ring.iter_idx + group_size) % group_size;
            void *sendbuf = op->staging_area + args->recvcount * block_idx * dt_extent;
            void *inbuf = op->reduce_scatter_block.ring.inbuf + op->reduce_scatter_block.ring.inbi * args->recvcount * op->reduce_scatter_block.ring.dtype_size; 
            status = ucg_op_reduce(args->op, inbuf, sendbuf, args->recvcount, args->dt);
            UCG_CHECK_GOTO(status, out);
            status = ucg_planc_ucx_p2p_isend(sendbuf, args->recvcount,
                                             args->dt, send_to_rank, op->tag,
                                             vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }
        
        if (ucg_test_and_clear_flags(&op->flags, UCG_REDUCE_SCATTER_BLOCK_RING_RECV)) {
            op->reduce_scatter_block.ring.inbi = op->reduce_scatter_block.ring.inbi ^ 0x1;
            // int block_idx = (myrank - op->reduce_scatter_block.ring.iter_idx - 1 + group_size) % group_size;
            void *recvbuf = op->reduce_scatter_block.ring.inbuf + op->reduce_scatter_block.ring.inbi * args->recvcount * op->reduce_scatter_block.ring.dtype_size;
            status = ucg_planc_ucx_p2p_irecv(recvbuf, args->recvcount,
                                            args->dt, recv_from_rank, op->tag,
                                             vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }

        status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
        UCG_CHECK_GOTO(status, out);
        op->reduce_scatter_block.ring.iter_idx++;
        op->flags |= UCG_GATHER_RING_ITER_FLAGS;
    }

    if (ucg_test_and_clear_flags(&op->flags, UCG_REDUCE_SCATTER_BLOCK_RING_END)) {
        void *recvbuf = op->staging_area + args->recvcount * myrank * dt_extent;
        void *inbuf = op->reduce_scatter_block.ring.inbuf + op->reduce_scatter_block.ring.inbi * args->recvcount * op->reduce_scatter_block.ring.dtype_size;
        status = ucg_op_reduce(args->op, inbuf, recvbuf, args->recvcount, args->dt);
        UCG_CHECK_GOTO(status, out);
        
        status = ucg_dt_memcpy(args->recvbuf, args->recvcount, args->dt,
                                recvbuf, args->recvcount, args->dt);
        UCG_CHECK_GOTO(status, out);  
    }


out:
    op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_reduce_scatter_block_ring_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_op_reset(op);
    op->reduce_scatter_block.ring.inbi = 0;
    op->reduce_scatter_block.ring.iter_idx = 2;
    op->flags = UCG_GATHER_RING_FLAGS;
    status = ucg_planc_ucx_reduce_scatter_block_ring_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}


static inline
ucg_status_t ucg_planc_ucx_reduce_scatter_block_ring_op_init(ucg_planc_ucx_op_t *op,
                                                   ucg_planc_ucx_group_t *ucx_group)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_init(op, ucx_group);
    ucg_vgroup_t *vgroup = op->super.vgroup;
    int32_t group_size = vgroup->size;

    ucg_coll_reduce_scatter_block_args_t *args = &op->super.super.args.reduce_scatter_block;

    op->reduce_scatter_block.ring.dtype_size = ucg_dt_size(args->dt);
    int64_t total_count = args->recvcount * group_size;

    op->reduce_scatter_block.ring.total_count = total_count;
    op->staging_area = ucg_malloc(total_count * op->reduce_scatter_block.ring.dtype_size, "reduce_scatter_block op staging area");
    if (op->staging_area == NULL) {
        status = UCG_ERR_NO_MEMORY;
        goto err;
    }
    
    op->reduce_scatter_block.ring.inbuf = ucg_malloc(2 * args->recvcount * op->reduce_scatter_block.ring.dtype_size, "reduce_scatter_block inbuf");
    if (op->reduce_scatter_block.ring.inbuf == NULL) {
        status = UCG_ERR_NO_MEMORY;
        goto err_free_staging_area;
    }

    return status;

err_free_staging_area:
    ucg_free(op->staging_area);
err:
    return status;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_reduce_scatter_block_ring_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                        ucg_vgroup_t *vgroup,
                                                        const ucg_coll_args_t *args)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args);

    ucg_planc_ucx_op_t *ucx_op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (ucx_op == NULL) {
        goto err;
    }

    ucg_status_t status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &ucx_op->super, vgroup,
                                              ucg_planc_ucx_reduce_scatter_block_ring_op_trigger,
                                              ucg_planc_ucx_reduce_scatter_block_ring_op_progress,
                                              ucg_planc_ucx_reduce_scatter_block_ring_op_discard,
                                              args);

    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }
    ucg_planc_ucx_reduce_scatter_block_ring_op_init(ucx_op, ucx_group);
    return ucx_op;

err_free_op:
    ucg_mpool_put(ucx_op);
err:
    return NULL;
}

static ucg_status_t ucg_planc_ucx_reduce_scatter_block_ring_check(ucg_vgroup_t *vgroup, const ucg_coll_args_t *args)
{
    if (args->reduce_scatter_block.op->type != UCG_OP_TYPE_SUM) {
        ucg_info("Reduce_scatter ring op type is not MPI_SUM, so roll back to OpenMPI");
        return UCG_ERR_UNSUPPORTED;
    }
    return UCG_OK;
}

ucg_status_t ucg_planc_ucx_reduce_scatter_block_ring_prepare(ucg_vgroup_t *vgroup,
                                                  const ucg_coll_args_t *args,
                                                  ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);
    ucg_status_t status;
    status = ucg_planc_ucx_reduce_scatter_block_ring_check(vgroup, args);
    if (status != UCG_OK) {
        return status;
    }
    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_op_t *ring_op = ucg_planc_ucx_reduce_scatter_block_ring_op_new(ucx_group, vgroup, args);
    if (ring_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &ring_op->super;
    return UCG_OK;
}
