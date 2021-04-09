/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "builtin_ops.h"
#include "builtin_comp_step.inl"

#include <ucs/arch/atomic.h>

#ifndef HAVE_UCT_COLLECTIVES
#define UCT_PACK_CALLBACK_REDUCE ((uintptr_t)-1)
#endif

#define UCG_BUILTIN_PACKER_NAME(_modifier, _mode) \
    ucg_builtin_step_am_bcopy_pack ## _modifier ## _mode

#define UCG_BUILTIN_PACKER_DECLARE(_modifier, _mode) \
    size_t UCG_BUILTIN_PACKER_NAME(_modifier, _mode) (void *dest, void *arg)

static int UCS_F_ALWAYS_INLINE
ucg_builtin_atomic_reduce_full(ucg_builtin_request_t *req,
                                   void *src, void *dst, size_t length)
{
    ucg_op_t *op = &req->op->super;

    op->reduce_full_f(dst, src, op);

    return length;
}

static int UCS_F_ALWAYS_INLINE
ucg_builtin_atomic_reduce_part(ucg_builtin_request_t *req,
                               void *src, void *dst, size_t length)
{
    ucg_op_t *op = &req->op->super;

    op->reduce_frag_f(dst, src, length, op);

    return length;
}

#define UCG_BUILTIN_PACK_CB(_offset, _length) { \
    ucg_builtin_header_t *header = (ucg_builtin_header_t*)dest; \
    ucg_builtin_request_t *req   = (ucg_builtin_request_t*)arg; \
    ucg_builtin_op_step_t *step  = req->step; \
    size_t buffer_length         = (_length); \
    header->header               = step->am_header.header; \
    \
    ucs_assert(header->header != 0); \
    ucs_assert(((uintptr_t)arg & UCT_PACK_CALLBACK_REDUCE) == 0); \
    ucs_assert((_offset) + buffer_length <= step->buffer_length); \
    \
    memcpy(header + 1, step->send_buffer + (_offset), buffer_length); \
    \
    return sizeof(*header) + buffer_length; \
}

UCG_BUILTIN_PACKER_DECLARE(_, single)
UCG_BUILTIN_PACK_CB(0,                 step->buffer_length)

UCG_BUILTIN_PACKER_DECLARE(_, full)
UCG_BUILTIN_PACK_CB(step->iter_offset, step->fragment_length)

UCG_BUILTIN_PACKER_DECLARE(_, part)
UCG_BUILTIN_PACK_CB(step->iter_offset, step->buffer_length - step->iter_offset)

#define UCG_BUILTIN_REDUCING_PACK_CB(_offset, _length, _part) { \
    if ((uintptr_t)arg & UCT_PACK_CALLBACK_REDUCE) { \
        ucg_builtin_request_t *req   = (ucg_builtin_request_t*)((uintptr_t)arg \
                                        ^ UCT_PACK_CALLBACK_REDUCE); \
        ucg_builtin_op_step_t *step  = req->step; \
        ucg_builtin_header_t *header = (ucg_builtin_header_t*)dest; \
        \
        ucs_assert(header->header != 0); \
        ucs_assert(header->header == step->am_header.header); \
        \
        return sizeof(*header) + ucg_builtin_atomic_reduce_ ## _part \
                (req, step->send_buffer + (_offset), header + 1, (_length)); \
    } else { \
        UCG_BUILTIN_PACK_CB((_offset), (_length)) \
    } \
}

UCG_BUILTIN_PACKER_DECLARE(_reducing_, single)
UCG_BUILTIN_REDUCING_PACK_CB(0,                 step->buffer_length, full)

UCG_BUILTIN_PACKER_DECLARE(_reducing_, full)
UCG_BUILTIN_REDUCING_PACK_CB(step->iter_offset, step->fragment_length, part)

UCG_BUILTIN_PACKER_DECLARE(_reducing_, part)
UCG_BUILTIN_REDUCING_PACK_CB(step->iter_offset, step->buffer_length -
                                                step->iter_offset, part)

#define UCG_BUILTIN_VARIADIC_PACK_CB(_offset, _length, _part) { \
        { /* Separate scope for a separate name-space (e.g. "step" conflict)*/ \
            ucg_builtin_request_t *req  = (ucg_builtin_request_t*)arg; \
            ucg_builtin_op_step_t *step = req->step; \
            \
            uint8_t *buffer; \
            size_t length; \
            \
            ucg_builtin_step_get_local_address(step + 1, 1, &buffer, &length); \
            ucg_builtin_step_set_remote_address(step, &buffer); \
        } \
        { \
            UCG_BUILTIN_PACK_CB((_offset), (_length)) \
        } \
}

UCG_BUILTIN_PACKER_DECLARE(_variadic_, single)
UCG_BUILTIN_VARIADIC_PACK_CB(0,                 step->buffer_length, full)

UCG_BUILTIN_PACKER_DECLARE(_variadic_, full)
UCG_BUILTIN_VARIADIC_PACK_CB(step->iter_offset, step->fragment_length, part)

UCG_BUILTIN_PACKER_DECLARE(_variadic_, part)
UCG_BUILTIN_VARIADIC_PACK_CB(step->iter_offset, step->buffer_length -
                                                step->iter_offset, part)

#define UCG_BUILTIN_ATOMIC_SINGLE_PACK_CB(_integer_bits) { \
    ucg_builtin_header_t *header = (ucg_builtin_header_t*)dest; \
    ucg_builtin_request_t *req   = (ucg_builtin_request_t*)arg; \
    ucg_builtin_op_step_t *step  = req->step; \
    uint##_integer_bits##_t *ptr = (uint##_integer_bits##_t *)(header + 1); \
    \
    ucs_atomic_add##_integer_bits (ptr, \
            *(uint##_integer_bits##_t *)step->send_buffer); \
    \
    return sizeof(uint##_integer_bits##_t); \
}

#define UCG_BUILTIN_ATOMIC_MULTIPLE_PACK_CB(_integer_bits) { \
    ucg_builtin_header_t *header = (ucg_builtin_header_t*)dest; \
    ucg_builtin_request_t *req   = (ucg_builtin_request_t*)arg; \
    ucg_builtin_op_step_t *step  = req->step; \
    uint##_integer_bits##_t *ptr = (uint##_integer_bits##_t *)(header + 1); \
    size_t length                = step->buffer_length; \
    unsigned index, count        = length / sizeof(*ptr); \
    \
    ucs_assert((step->buffer_length % sizeof(*ptr)) == 0); \
    \
    for (index = 0; index < count; ptr++, index++) { \
        ucs_atomic_add##_integer_bits (ptr, \
                *(uint##_integer_bits##_t *)step->send_buffer); \
    } \
    \
    return length; \
}

UCG_BUILTIN_PACKER_DECLARE(_atomic_single_, 8)
UCG_BUILTIN_ATOMIC_SINGLE_PACK_CB(8)

UCG_BUILTIN_PACKER_DECLARE(_atomic_multiple_, 8)
UCG_BUILTIN_ATOMIC_MULTIPLE_PACK_CB(8)

UCG_BUILTIN_PACKER_DECLARE(_atomic_single_, 16)
UCG_BUILTIN_ATOMIC_SINGLE_PACK_CB(16)

UCG_BUILTIN_PACKER_DECLARE(_atomic_multiple_, 16)
UCG_BUILTIN_ATOMIC_MULTIPLE_PACK_CB(16)

UCG_BUILTIN_PACKER_DECLARE(_atomic_single_, 32)
UCG_BUILTIN_ATOMIC_SINGLE_PACK_CB(32)

UCG_BUILTIN_PACKER_DECLARE(_atomic_multiple_, 32)
UCG_BUILTIN_ATOMIC_MULTIPLE_PACK_CB(32)

UCG_BUILTIN_PACKER_DECLARE(_atomic_single_, 64)
UCG_BUILTIN_ATOMIC_SINGLE_PACK_CB(64)

UCG_BUILTIN_PACKER_DECLARE(_atomic_multiple_, 64)
UCG_BUILTIN_ATOMIC_MULTIPLE_PACK_CB(64)

#define UCG_BUILTIN_DATATYPE_PACK_CB(_offset, _length) { \
    ucg_builtin_header_t *header = (ucg_builtin_header_t*)dest; \
    ucg_builtin_request_t *req   = (ucg_builtin_request_t*)arg; \
    ucg_builtin_op_t *op         = req->op; \
    ucp_dt_generic_t *dt_gen     = ucp_dt_to_generic(op->send_dt); \
    void *dt_state               = op->send_pack; \
    ucg_builtin_op_step_t *step  = req->step; \
    size_t buffer_length         = (_length); \
    header->header               = step->am_header.header; \
    \
    ucs_assert(((uintptr_t)arg & UCT_PACK_CALLBACK_REDUCE) == 0); \
    \
    dt_gen->ops.pack(dt_state, (_offset), header + 1, buffer_length); \
    \
    return sizeof(*header) + buffer_length; \
}

UCG_BUILTIN_PACKER_DECLARE(_datatype_, single)
UCG_BUILTIN_DATATYPE_PACK_CB(0,                 step->buffer_length)

UCG_BUILTIN_PACKER_DECLARE(_datatype_, full)
UCG_BUILTIN_DATATYPE_PACK_CB(step->iter_offset, step->fragment_length)

UCG_BUILTIN_PACKER_DECLARE(_datatype_, part)
UCG_BUILTIN_DATATYPE_PACK_CB(step->iter_offset, step->buffer_length -
                                                step->iter_offset)

ucs_status_t
ucg_builtin_step_select_packers(const ucg_collective_params_t *params,
                                size_t send_dt_len, int is_send_dt_contig,
                                ucg_builtin_op_step_t *step)
{
    int is_signed;
    uint16_t modifiers = UCG_PARAM_TYPE(params).modifiers;
    int is_sm_reduce   = ((step->phase->method == UCG_PLAN_METHOD_SEND_TO_SM_ROOT) &&
                          (modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE));

    if ((is_sm_reduce) &&
        (ucg_global_params.field_mask & UCG_PARAM_FIELD_DATATYPE_CB) &&
        (ucg_global_params.datatype.is_integer_f(params->send.dtype, &is_signed)) &&
        (!is_signed) &&
        (ucg_global_params.field_mask & UCG_PARAM_FIELD_REDUCE_OP_CB) &&
        (ucg_global_params.reduce_op.is_sum_f != NULL) &&
        (ucg_global_params.reduce_op.is_sum_f(UCG_PARAM_OP(params)))) {
        int is_single = (params->send.count == 1);
        // TODO: (un)set UCG_BUILTIN_OP_STEP_FLAG_BCOPY_PACK_LOCK...
        switch (send_dt_len) {
        case 1:
            step->bcopy.pack_single_cb = is_single ?
                    UCG_BUILTIN_PACKER_NAME(_atomic_single_, 8) :
                    UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 8);
            return UCS_OK;

        case 2:
            step->bcopy.pack_single_cb = is_single ?
                    UCG_BUILTIN_PACKER_NAME(_atomic_single_, 16) :
                    UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 16);
            return UCS_OK;

        case 4:
            step->bcopy.pack_single_cb = is_single ?
                    UCG_BUILTIN_PACKER_NAME(_atomic_single_, 32) :
                    UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 32);
            return UCS_OK;

        case 8:
            step->bcopy.pack_single_cb = is_single ?
                    UCG_BUILTIN_PACKER_NAME(_atomic_single_, 64) :
                    UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 64);
            return UCS_OK;

        default:
            ucs_error("unsupported unsigned integer datatype length: %lu",
                      send_dt_len);
            break; /* fall-back to the MPI reduction callback */
        }
    }

    int is_variadic   = (UCG_PARAM_TYPE(params).modifiers &
                         UCG_GROUP_COLLECTIVE_MODIFIER_VARIADIC);

    if (!is_send_dt_contig) {
        step->bcopy.pack_full_cb   = UCG_BUILTIN_PACKER_NAME(_datatype_, full);
        step->bcopy.pack_part_cb   = UCG_BUILTIN_PACKER_NAME(_datatype_, part);
        step->bcopy.pack_single_cb = UCG_BUILTIN_PACKER_NAME(_datatype_, single);
    } else if (is_variadic) {
        step->bcopy.pack_full_cb   = UCG_BUILTIN_PACKER_NAME(_variadic_, full);
        step->bcopy.pack_part_cb   = UCG_BUILTIN_PACKER_NAME(_variadic_, part);
        step->bcopy.pack_single_cb = UCG_BUILTIN_PACKER_NAME(_variadic_, single);
    } else if (is_sm_reduce) {
        step->bcopy.pack_full_cb   = UCG_BUILTIN_PACKER_NAME(_reducing_, full);
        step->bcopy.pack_part_cb   = UCG_BUILTIN_PACKER_NAME(_reducing_, part);
        step->bcopy.pack_single_cb = UCG_BUILTIN_PACKER_NAME(_reducing_, single);
    } else {
        step->bcopy.pack_full_cb   = UCG_BUILTIN_PACKER_NAME(_, full);
        step->bcopy.pack_part_cb   = UCG_BUILTIN_PACKER_NAME(_, part);
        step->bcopy.pack_single_cb = UCG_BUILTIN_PACKER_NAME(_, single);
    }

    return UCS_OK;
}

void ucg_builtin_print_pack_cb_name(uct_pack_callback_t pack_single_cb)
{
    if (pack_single_cb == NULL) {
        printf("NONE");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_single_, 8)) {
        printf("atomic (8 bytes, single integer)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 8)) {
        printf("atomic (8 bytes, multiple integers)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_single_, 16)) {
        printf("atomic (16 bytes, single integer)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 16)) {
        printf("atomic (16 bytes, multiple integers)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_single_, 32)) {
        printf("atomic (32 bytes, single integer)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 32)) {
        printf("atomic (32 bytes, multiple integers)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_single_, 64)) {
        printf("atomic (64 bytes, single integer)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 64)) {
        printf("atomic (64 bytes, multiple integers)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_reducing_, single)) {
        printf("reduction callback");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_, single)) {
        printf("memory copy");
    }
}
