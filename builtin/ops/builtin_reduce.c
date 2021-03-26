/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "builtin_ops.h"

#include "ucp/dt/dt_contig.h" /* no braces since that header isn't installed */

static void UCS_F_ALWAYS_INLINE
ucg_builtin_mpi_reduce(void *mpi_op, void *src, void *dst,
                       int dcount, void* mpi_datatype)
{
    UCS_PROFILE_CALL_VOID(ucg_global_params.reduce_op.reduce_cb_f, mpi_op,
                          (char*)src, (char*)dst, (unsigned)dcount, mpi_datatype);
}

static void ucg_builtin_mpi_reduce_single(uint8_t *dst, uint8_t *src,
                                          ucg_op_t *op)
{
    const ucg_collective_params_t *params = &op->params;

    ucg_builtin_mpi_reduce(UCG_PARAM_OP(params), src, dst,
                           params->recv.count, params->recv.dtype);
}

static void ucg_builtin_mpi_reduce_fragment(uint8_t *dst, uint8_t *src,
                                            size_t frag_len, ucg_op_t *op)
{
    const ucg_collective_params_t *params = &op->params;
    ucg_builtin_op_t *builtin_op          = ucs_derived_of(op,
                                                           ucg_builtin_op_t);
    ucp_datatype_t dtype                  = builtin_op->recv_dt;
    unsigned dtype_count                  = frag_len /
                                            ucp_contig_dt_length(dtype, 1);

    ucg_builtin_mpi_reduce(UCG_PARAM_OP(params), src, dst, dtype_count,
                           params->recv.dtype);
}


static void ucg_builtin_step_full_sum_float_1(uint8_t *dst, uint8_t *src,
                                              ucg_op_t *op)
{
    *(float*)dst += *(float*)src;
}

static void ucg_builtin_step_full_sum_float_2(uint8_t *dst, uint8_t *src,
                                              ucg_op_t *op)
{
    ((float*)dst)[0] += ((float*)src)[0];
    ((float*)dst)[1] += ((float*)src)[1];
}

static void ucg_builtin_step_frag_sum_float(uint8_t *dst, uint8_t *src,
                                            size_t frag_len, ucg_op_t *op)
{
    float *f_src = (float*)src;
    float *f_dst = (float*)dst;
    float *limit = (float*)(dst + frag_len);

    do {
        *f_dst += *f_src;
        f_dst++;
    } while (f_dst < limit);
}


ucs_status_t ucg_builtin_step_select_reducers(void *dtype, void *reduce_op,
                                              int is_contig, size_t dtype_len,
                                              int64_t dtype_cnt,
                                              ucg_builtin_config_t *config,
                                              ucg_op_reduce_full_f
                                              *selected_reduce_full_f,
                                              ucg_op_reduce_frag_f
                                              *selected_reduce_frag_f)
{
    ucg_op_reduce_full_f reduce_full_chosen = ucg_builtin_mpi_reduce_single;
    ucg_op_reduce_frag_f reduce_frag_chosen = ucg_builtin_mpi_reduce_fragment;

    /* This is just an example... */
    if (is_contig && (dtype_len == sizeof(float)) &&
        ucg_global_params.reduce_op.is_sum_f(reduce_op) &&
        ucg_global_params.datatype.is_floating_point_f(dtype)) {
        reduce_frag_chosen = ucg_builtin_step_frag_sum_float;

        switch (dtype_cnt) {
        case 1:
            reduce_full_chosen = ucg_builtin_step_full_sum_float_1;
            break;
        case 2:
            reduce_full_chosen = ucg_builtin_step_full_sum_float_2;
            break;
        default:
            break;
        }
    }

    // TODO: implement more specific reducers... maybe use uct_mm_incast_ep_am_short_func_arr?

    *selected_reduce_full_f = reduce_full_chosen;
    *selected_reduce_frag_f = reduce_frag_chosen;

     ucs_assert((selected_reduce_full_f != NULL) &&
                ((*selected_reduce_full_f == NULL) ||
                 (*selected_reduce_full_f == reduce_full_chosen)));

     ucs_assert((selected_reduce_frag_f != NULL) &&
                ((*selected_reduce_frag_f == NULL) ||
                 (*selected_reduce_frag_f == reduce_frag_chosen)));

    return UCS_OK;
}
