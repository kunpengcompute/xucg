/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2021.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <string.h>
#include <ucp/core/ucp_ep.inl>
#include <ucg/api/ucg_mpi.h>
#include <ucg/base/ucg_group.h>
#include <ucs/datastruct/queue.h>
#include <ucs/datastruct/list.h>
#include <ucs/profile/profile.h>
#include <ucs/debug/memtrack.h>
#include <ucs/debug/assert.h>

#include "builtin_cb.inl"

/*
* rank id, used in the phase step calculate algorithm
*/
ucg_group_member_index_t g_myidx = 0;
unsigned num_procs = 0;

/* in order to keep the interface ucg_builtin_step_create no change, use the global para to pass the value */
short g_myposition = 0;
int g_reduce_coinsidency = 0;
/******************************************************************************
 *                                                                            *
 *                            Operation Execution                             *
 *                                                                            *
 ******************************************************************************/
static void ucg_builtin_step_assert(ucg_builtin_op_step_t *step, enum ucg_builtin_op_step_flags step_flag)
{
    ucs_assert((step->flags & (UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT |
                               UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY |
                               UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY)) ==
                               step_flag);
    ucs_assert(step->iter_offset != UCG_BUILTIN_OFFSET_PIPELINE_READY);
    ucs_assert(step->iter_offset != UCG_BUILTIN_OFFSET_PIPELINE_PENDING);
}

/*
 * calculate the fragments for current buffer length
 * INPUT: length, dt_len, ep_thresh
 * OUTPUT: fragment_length, fragments, flag
 */
static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_step_calc_fragment(unsigned length, size_t dt_len,
                                                                       ucg_builtin_tl_threshold_t *ep_thresh,
                                                                        size_t *fragment_length,
                                                                        uint32_t *fragments,
                                                                        uint16_t *flag)
{
    ucs_assert(dt_len > 0);
    size_t max_short_one = ep_thresh->max_short_one;
    size_t max_short_max = ep_thresh->max_short_max;
    size_t max_bcopy_one = ep_thresh->max_bcopy_one;
    size_t max_bcopy_max = ep_thresh->max_bcopy_max;
    size_t max_zcopy_one = ep_thresh->max_zcopy_one;
    size_t md_attr_cap_max_reg = ep_thresh->md_attr_cap_max_reg;
    size_t extra_frag = 0;
   /*
     * Short messages (e.g. RDMA "inline")
     */
    if ((length <= max_short_one) && (max_short_one != 0)) {
        /* Short send - single message */
        *fragments = 1;
        *flag = UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT;
    } else if ((length <= max_short_max) && (max_short_max != 0)) {
        if (dt_len <= max_short_one) {
            /* Short send - multiple messages */
            *fragment_length = max_short_one - (max_short_one % dt_len);
        } else {
            *fragment_length = max_short_one;
        }
        ucs_assert(*fragment_length > 0);
        extra_frag = (length % (*fragment_length)) > 0;
        *fragments = ((*fragment_length) ? (length / (*fragment_length)) : 0) + extra_frag;
        *flag = UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED | UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT;
    /*
     * Large messages, if supported (e.g. RDMA "zero-copy")
     */
    } else if ((length >  max_bcopy_max) && (length <= md_attr_cap_max_reg)) {
        if ((length < max_zcopy_one) && (max_zcopy_one != 0)) {
            /* ZCopy send - single message */
            *fragments = 1;
            *flag = UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY;
        } else {
            /* ZCopy send - multiple message */
            *fragment_length = (dt_len > max_zcopy_one) ? max_zcopy_one : (max_zcopy_one - (max_zcopy_one % dt_len)) ;
            ucs_assert(*fragment_length > 0);
            extra_frag = (length % (*fragment_length)) > 0;
            *fragments = length / (*fragment_length) + extra_frag;
            *flag = UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED | UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY;
        }
    /*
     * Medium messages
     */
    } else if ((length <= max_bcopy_one) && (max_bcopy_one != 0)) {
        /* BCopy send - single message */
        *fragments       = 1;
        *flag = UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY;
    } else if ((length <= max_bcopy_max) && (max_bcopy_max != 0)) {
        /* BCopy send - multiple messages */
        if (dt_len > max_bcopy_one) {
        *fragment_length =  max_bcopy_one;
        } else {
        *fragment_length =  max_bcopy_one - (max_bcopy_one % dt_len);
        }
        ucs_assert(*fragment_length > 0);
        extra_frag = (length % (*fragment_length)) > 0;
        *fragments = ((*fragment_length) ? (length / (*fragment_length)) : 0) + extra_frag;
        *flag = UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED | UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY;
    } else {
        return UCS_ERR_INVALID_PARAM;
    }
    return UCS_OK;
}

/* Add rank id to the front of variable-length operations. */
static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_ep_am_short_pack_rank(uct_ep_h ep, uint8_t id, uint64_t header,
                                                                          const void *payload, unsigned length,
                                                                          ucg_builtin_op_step_t *step)
{
    ucg_builtin_pack_rank_cb_t pack_rank_func = step->variable_length.pack_rank_func;
    if (pack_rank_func != NULL) {
        size_t new_length = 0;
        void *packed_rank_payload = pack_rank_func(step, payload, length, &new_length);
        if (packed_rank_payload == NULL) {
            return UCS_ERR_NO_MEMORY;
        }
        payload = packed_rank_payload;
        length = new_length;
    }
    return uct_ep_am_short(ep, id, header, payload, length);
}

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_step_dummy_send(ucg_builtin_request_t *req,
                                                                    ucg_builtin_op_step_t *step,
                                                                    uct_ep_h ep, int is_single_send)
{
    ucg_builtin_step_assert(step, UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND);
    return UCS_OK;
}

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_step_am_short_one(ucg_builtin_request_t *req,
                                                                      ucg_builtin_op_step_t *step,
                                                                      uct_ep_h ep, int is_single_send)
{
    ucg_builtin_step_assert(step, UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT);
    ucs_info("am_short_one step %u length %zu", step->am_header.step_idx, step->buffer_length);

    int8_t *send_buffer          = step->send_buffer;
    void *dt_state               = step->non_contig.pack_state;
    if (dt_state != NULL) {
        req->op->send_dt->ops.pack(dt_state, 0, step->non_contig.contig_buffer, step->buffer_length);
        send_buffer              = step->non_contig.contig_buffer;
    }
    return ucg_builtin_ep_am_short_pack_rank(ep, step->am_id,
                                            step->am_header.header, send_buffer, step->buffer_length, step);
}

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_step_am_short_max(ucg_builtin_request_t *req,
                                                                      ucg_builtin_op_step_t *step,
                                                                      uct_ep_h ep, int is_single_send)
{
    ucs_status_t status;
    unsigned am_id               = step->am_id;
    ucg_offset_t frag_size       = step->fragment_length;
    int8_t *send_buffer          = step->send_buffer;
    void *dt_state               = step->non_contig.pack_state;

    if (dt_state != NULL) {
        req->op->send_dt->ops.pack(dt_state, 0, step->non_contig.contig_buffer, step->buffer_length);
        send_buffer              = step->non_contig.contig_buffer;
    }

    int8_t *buffer_iter          = send_buffer + step->iter_offset;
    int8_t *buffer_iter_limit    = send_buffer + step->buffer_length - frag_size;
    ucg_builtin_header_t am_iter = { .header = step->am_header.header };
    am_iter.remote_offset        = (is_single_send) ? step->iter_offset :
                                   am_iter.remote_offset + step->iter_offset;

    ucg_builtin_step_assert(step, UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT);

    /* send every fragment but the last */
    if (ucs_likely(buffer_iter < buffer_iter_limit)) {
        do {
            ucs_debug("am_short_max step %u offset %" PRIu32 " length %u",
                step->am_header.step_idx, am_iter.remote_offset, frag_size);
            status = ucg_builtin_ep_am_short_pack_rank(ep, am_id, am_iter.header, buffer_iter, frag_size, step);

            if (is_single_send) {
                return status;
            }

            buffer_iter           += frag_size;
            am_iter.remote_offset += frag_size;
        } while ((status == UCS_OK) && (buffer_iter < buffer_iter_limit));

        /* send last fragment of the message */
        if (ucs_unlikely(status != UCS_OK)) {
            /* assuming UCS_ERR_NO_RESOURCE, restore the state for re-entry */
            step->iter_offset = (!is_single_send) ? buffer_iter - frag_size - send_buffer :
                                step->iter_offset;
            return status;
        }
    }

    ucs_debug("am_short_max step: %u; offset: %" PRIu32 "", step->am_header.step_idx, am_iter.remote_offset);
    status = ucg_builtin_ep_am_short_pack_rank(ep, am_id, am_iter.header, buffer_iter,
        send_buffer + step->buffer_length - buffer_iter, step);
    /* iter_offset can not set to be zero for pipelining */
    if (!is_single_send) {
        step->iter_offset = (status == UCS_OK) ? 0 : buffer_iter - send_buffer;
    }

    return status;
}

static size_t ucg_builtin_step_fill_bcopy_header(void *dest, ucg_builtin_request_t *req)
{
    ucg_builtin_op_step_t *step = req->step;
    unsigned is_rank_tx = step->phase->ex_attr.is_variable_len;
    if (is_rank_tx) {
        ucg_builtin_header_ext_t *header_ext_ptr = (ucg_builtin_header_ext_t *)dest;
        header_ext_ptr->header = step->am_header;
        header_ext_ptr->src_rank = step->phase->ex_attr.packed_rank;
        return sizeof(ucg_builtin_header_ext_t);
    } else {
        ucg_builtin_header_t *header_ptr = (ucg_builtin_header_t *)dest;
        header_ptr->header = step->am_header.header;
        return sizeof(ucg_builtin_header_t);
    }
}

static size_t ucg_builtin_step_am_bcopy_single_frag_packer(void *dest, void *arg)
{
    ucg_builtin_request_t *req       = (ucg_builtin_request_t*)arg;
    ucg_builtin_op_step_t *step      = req->step;
    size_t header_len = ucg_builtin_step_fill_bcopy_header(dest, req);
    int8_t *header_ptr = (int8_t*)dest;
    void *dt_state                   = step->non_contig.pack_state;

    if (dt_state != NULL) {
        req->op->send_dt->ops.pack(dt_state, 0, header_ptr + header_len, step->buffer_length);
    } else {
        memcpy(header_ptr + header_len, step->send_buffer, step->buffer_length);
    }
    return header_len + step->buffer_length;
}

static size_t ucg_builtin_step_am_bcopy_full_frag_packer(void *dest, void *arg)
{
    ucg_builtin_request_t *req       = (ucg_builtin_request_t*)arg;
    ucg_builtin_op_step_t *step      = req->step;
    size_t header_len = ucg_builtin_step_fill_bcopy_header(dest, req);
    int8_t *header_ptr = (int8_t*)dest;
    void *dt_state                   = step->non_contig.pack_state;

    if (dt_state != NULL) {
        req->op->send_dt->ops.pack(dt_state, step->iter_offset, header_ptr + header_len, step->fragment_length);
    } else {
        memcpy(header_ptr + header_len, step->send_buffer + step->iter_offset, step->fragment_length);
    }
    return header_len + step->fragment_length;
}

static size_t ucg_builtin_step_am_bcopy_partial_frag_packer(void *dest, void *arg)
{
    ucg_builtin_request_t *req       = (ucg_builtin_request_t*)arg;
    ucg_builtin_op_step_t *step      = req->step;
    ucg_offset_t last_frag_length    = step->buffer_length - step->iter_offset;
    size_t header_len = ucg_builtin_step_fill_bcopy_header(dest, req);
    int8_t *header_ptr = (int8_t*)dest;
    void *dt_state                   = step->non_contig.pack_state;

    if (dt_state != NULL) {
        req->op->send_dt->ops.pack(dt_state, step->iter_offset, header_ptr + header_len, last_frag_length);
    } else {
        memcpy(header_ptr + header_len, step->send_buffer + step->iter_offset, last_frag_length);
    }
    return header_len + last_frag_length;
}

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_step_am_bcopy_one(ucg_builtin_request_t *req,
                                                                      ucg_builtin_op_step_t *step,
                                                                      uct_ep_h ep, int is_single_send)
{
    ucg_builtin_step_assert(step, UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY);

    /* send active message to remote endpoint */
    ucs_debug("am_bcopy_one step %u length %zu", step->am_header.step_idx, step->buffer_length);
    ssize_t len = uct_ep_am_bcopy(ep, step->am_id,
                                  ucg_builtin_step_am_bcopy_single_frag_packer, req, 0);
    return (ucs_unlikely(len < 0)) ? (ucs_status_t)len : UCS_OK;
}

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_step_am_bcopy_max(ucg_builtin_request_t *req,
                                                                      ucg_builtin_op_step_t *step,
                                                                      uct_ep_h ep, int is_single_send)
{
    ssize_t len;
    unsigned am_id                = step->am_id;
    ucg_offset_t frag_size        = step->fragment_length;
    ucg_offset_t iter_limit       = step->buffer_length - frag_size;
    step->am_header.remote_offset = (is_single_send) ? step->iter_offset :
                                    step->am_header.remote_offset;


    ucg_builtin_step_assert(step, UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY);

    /* check if this is not, by any chance, the last fragment */
    if (ucs_likely(step->iter_offset < iter_limit)) {
        /* send every fragment but the last */
        do {
            ucs_debug("am_bcopy_max step %u offset %" PRIu32 " length %u",
                step->am_header.step_idx, step->am_header.remote_offset, frag_size);
            len = uct_ep_am_bcopy(ep, am_id, ucg_builtin_step_am_bcopy_full_frag_packer, req, 0);

            if (is_single_send) {
                return ucs_unlikely(len < 0) ? (ucs_status_t)len : UCS_OK;
            }

            step->am_header.remote_offset += frag_size;
            step->iter_offset             += frag_size;
        } while ((len >= 0) && (step->iter_offset < iter_limit));

        if (ucs_unlikely(len < 0)) {
            step->iter_offset -= frag_size;
            step->am_header.remote_offset -= frag_size;
            return (ucs_status_t)len;
        }
    }

    /* Send last fragment of the message */
    ucs_debug("am_bcopy_max step: %u; offset: %" PRIu32 "", step->am_header.step_idx, step->am_header.remote_offset);
    len = uct_ep_am_bcopy(ep, am_id, ucg_builtin_step_am_bcopy_partial_frag_packer, req, 0);
    if (ucs_unlikely(len < 0)) {
        return (ucs_status_t)len;
    }

    step->am_header.remote_offset = 0;
    /* iter_offset can not set to be zero for pipelining */
    step->iter_offset = (!is_single_send) ? 0 : step->iter_offset;

    return UCS_OK;
}

static ucs_status_t ucg_builtin_step_am_zcopy_pack_rank(uct_ep_h ep,
                                                        ucg_builtin_op_step_t *step,
                                                        const uct_iov_t *iov, size_t iovcnt,
                                                        unsigned flags,
                                                        uct_completion_t *comp)
{
    ucs_status_t status;
    unsigned is_rank_tx = step->phase->ex_attr.is_variable_len;
    if (is_rank_tx) {
        step->am_header_ext.header = step->am_header;
        step->am_header_ext.src_rank = step->phase->ex_attr.packed_rank;
        status = uct_ep_am_zcopy(ep, step->am_id,
                                &step->am_header_ext, sizeof(step->am_header_ext),
                                iov, iovcnt, flags, comp);
    } else {
        status = uct_ep_am_zcopy(ep, step->am_id,
                                &step->am_header, sizeof(step->am_header),
                                iov, iovcnt, flags, comp);
    }
    return status;
}

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_step_am_zcopy_one(ucg_builtin_request_t *req,
                                                                      ucg_builtin_op_step_t *step,
                                                                      uct_ep_h ep, int is_single_send)
{
    ucs_status_t status = UCS_OK;
    int8_t *send_buffer          = step->send_buffer;
    void *dt_state               = step->non_contig.pack_state;

    if (dt_state != NULL) {
        req->op->send_dt->ops.pack(dt_state, 0, step->non_contig.contig_buffer, step->buffer_length);
        send_buffer              = step->non_contig.contig_buffer;
    }

    uct_iov_t iov = {
        .buffer = send_buffer,
        .length = step->buffer_length,
        .memh   = step->zcopy.memh,
        .stride = 0,
        .count  = 1
    };

    ucg_builtin_step_assert(step, UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY);
    ucg_builtin_zcomp_t *zcomp = &step->zcopy.zcomp[step->iter_ep];
    zcomp->req = req;

    ucs_debug("am_zcopy_one step %u length %zu", step->am_header.step_idx, step->buffer_length);
    status = ucg_builtin_step_am_zcopy_pack_rank(ep, step, &iov, 1, 0, &zcomp->comp);
    return ucs_unlikely(status != UCS_INPROGRESS) ? status : UCS_OK;
}

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_step_am_zcopy_max(
    ucg_builtin_request_t *req,
    ucg_builtin_op_step_t *step,
    uct_ep_h ep, int is_single_send)
{
    ucs_status_t status;
    step->am_header.remote_offset = (is_single_send) ? step->iter_offset :
                                    step->am_header.remote_offset;
    int8_t *send_buffer           = step->send_buffer;
    void *dt_state                = step->non_contig.pack_state;
    if (dt_state != NULL) {
        req->op->send_dt->ops.pack(dt_state, 0, step->non_contig.contig_buffer, step->buffer_length);
        send_buffer               = step->non_contig.contig_buffer;
    }

    ucg_offset_t frag_size      = step->fragment_length;
    void* iov_buffer_limit      = send_buffer + step->buffer_length - frag_size;
    unsigned zcomp_index;
    if (step->phase->ex_attr.is_variable_len) {
        step->iter_offset = step->am_header.remote_offset;
        zcomp_index = step->iter_offset / step->fragment_length;
    } else {
        zcomp_index = step->iter_ep * step->fragments +
                      step->iter_offset / step->fragment_length;
    }
    ucg_builtin_zcomp_t *zcomp = &step->zcopy.zcomp[zcomp_index];

    uct_iov_t iov = {
        .buffer = send_buffer + step->iter_offset,
        .length = frag_size,
        .memh   = step->zcopy.memh,
        .stride = 0,
        .count  = 1
    };

    ucg_builtin_step_assert(step, UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY);

    /* check if this is not, by any chance, the last fragment */
    if (ucs_likely(iov.buffer < iov_buffer_limit)) {
        /* send every fragment but the last */
        do {
            ucs_debug("am_zcopy_max step %u offset %" PRIu32 " length %u",
                step->am_header.step_idx, step->am_header.remote_offset, frag_size);
            status = ucg_builtin_step_am_zcopy_pack_rank(ep, step, &iov, 1, 0, &zcomp->comp);
            (zcomp++)->req = req;

            if (is_single_send) {
                return status;
            }

            step->am_header.remote_offset += frag_size;
            iov.buffer = (void*)((int8_t*)iov.buffer + frag_size);
        } while ((status == UCS_INPROGRESS) && (iov.buffer < iov_buffer_limit));

        if (ucs_unlikely(status != UCS_INPROGRESS)) {
            step->iter_offset = (int8_t*)iov.buffer - send_buffer - frag_size;
            step->am_header.remote_offset -= frag_size;
            zcomp--;
            step->resend_flag = UCG_BUILTIN_OP_STEP_RESEND;
            return status;
        }
    }

    /* Send last fragment of the message */
    zcomp->req = req;
    iov.length = send_buffer + step->buffer_length - (int8_t*)iov.buffer;
    ucs_debug("am_zcopy_max step %u offset %" PRIu32 " length %zu",
        step->am_header.step_idx, step->am_header.remote_offset, iov.length);
    status     = ucg_builtin_step_am_zcopy_pack_rank(ep, step, &iov, 1, 0, &zcomp->comp);

    if (ucs_unlikely(status != UCS_INPROGRESS)) {
        step->iter_offset = (!is_single_send) ? (int8_t*)iov.buffer - send_buffer :
                            step->iter_offset;
        step->resend_flag = UCG_BUILTIN_OP_STEP_RESEND;
        return status;
    }

    step->am_header.remote_offset = 0;
    /* iter_offset can not set to be zero for pipelining */
    step->iter_offset = (!is_single_send) ? 0 : step->iter_offset;

    return UCS_OK;
}

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_step_am_dynamic(ucg_builtin_request_t *req,
                                                                    ucg_builtin_op_step_t *step,
                                                                    uct_ep_h ep,
                                                                    unsigned is_single_send)
{
    ucs_status_t status = UCS_OK;
    int is_short      = step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT;
    int is_bcopy      = step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY;
    int is_zcopy      = step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY;
    int is_fragmented = step->flags & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED;

    if (step->resend_flag & UCG_BUILTIN_OP_STEP_FIRST_SEND) {
        step->am_header.remote_offset = 0;
        step->iter_offset = 0;
    }

    /* Single-send operations (only one fragment passed to UCT) */
    if (!is_fragmented) {
        if (is_short) {
            status = ucg_builtin_step_am_short_one(req, step, ep, is_single_send);
        } else if (is_bcopy) {
            status = ucg_builtin_step_am_bcopy_one(req, step, ep, is_single_send);
        } else if (is_zcopy) {
            status = ucg_builtin_step_am_zcopy_one(req, step, ep, is_single_send);
        }
    } else {
        if (is_short) {
            status = ucg_builtin_step_am_short_max(req, step, ep, is_single_send);
        } else if (is_bcopy) {
            status = ucg_builtin_step_am_bcopy_max(req, step, ep, is_single_send);
        } else if (is_zcopy) {
            status = ucg_builtin_step_am_zcopy_max(req, step, ep, is_single_send);
        }
    }
    return status;
}

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_step_update_pending(ucg_builtin_request_t *req,
                                                                        unsigned recv_ep_index)
{
    ucs_status_t status = UCS_OK;
    ucg_collective_params_t *params = &(req->op->super.params);
    ucg_builtin_op_step_t *step = req->step;
    ucg_builtin_plan_phase_t *phase = step->phase;
    unsigned recv_buffer_length = step->buffer_length_recv;
    ucg_builtin_tl_threshold_t *ep_thresh = &phase->ep_thresh[recv_ep_index];

    size_t fragment_length  = 0;
    uint32_t fragments = 0;
    uint16_t recv_flag = 0;

    status = ucg_builtin_step_calc_fragment(recv_buffer_length, params->recv.dt_len, ep_thresh,
                                            &fragment_length, &fragments, &recv_flag);
    if (status != UCS_OK) {
        return status;
    }

    /* update the pending related to receive count */
    req->pending += fragments;
    return status;
}

static void ucg_builtin_dynamic_calc_pending(ucg_builtin_request_t *req, ucg_request_t **user_req)
{
    ucg_collective_params_t *params = &(req->op->super.params);
    ucg_builtin_op_step_t *step = req->step;
    ucg_builtin_plan_phase_t *phase = step->phase;
    ucg_builtin_coll_params_t *recv_coll_params = step->recv_coll_params;

    unsigned recv_start_block = phase->ex_attr.recv_start_block;
    unsigned local_member_cnt = phase->ex_attr.member_cnt;

    unsigned block_idx;
    while (step->iter_ep < phase->ep_cnt) {
        uct_ep_h *ep_iter = phase->multi_eps + step->iter_ep;
        if (*ep_iter) {
            block_idx = (recv_start_block + local_member_cnt + step->iter_ep - phase->send_ep_cnt) % local_member_cnt;
            if (recv_coll_params->counts[block_idx] > 0) {
                step->buffer_length_recv = recv_coll_params->counts[block_idx] * params->recv.dt_len;
                ucg_builtin_step_update_pending(req, step->iter_ep);
            }
        }
        step->iter_ep++;
    }
}

static ucs_status_t ucg_builtin_dynamic_send_recv(ucg_builtin_request_t *req, ucg_request_t **user_req)
{
    ucs_status_t status = UCS_OK;

    ucg_builtin_op_step_t *step = req->step;
    ucg_builtin_plan_phase_t *phase = step->phase;
    ucg_collective_params_t *params = &(req->op->super.params);
    ucg_builtin_coll_params_t *send_coll_params = step->send_coll_params;

    /* step dynamic send flag */
    step->flags |= UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_DYNAMIC;

    /* save the original step->flags */
    uint16_t orig_flags = step->flags;

    unsigned send_start_block = phase->ex_attr.start_block;
    unsigned local_member_cnt = phase->ex_attr.member_cnt;

    /* initialize the pendings before both sending/receiving */
    if (step->resend_flag & UCG_BUILTIN_OP_STEP_FIRST_SEND) {
        req->pending = 0;
    }

    int block_idx;
    while (step->iter_ep < phase->send_ep_cnt) {
        uct_ep_h *ep_iter = phase->multi_eps + step->iter_ep;
        if (*ep_iter) {
            block_idx = (send_start_block + step->iter_ep) % local_member_cnt;
            if (step->send_coll_params->counts[block_idx] > 0) {
                int send_buffer_length = send_coll_params->counts[block_idx] * params->send.dt_len;
                int send_buffer_displ = send_coll_params->displs[block_idx] * params->send.dt_len;
                step->send_buffer = send_coll_params->init_buf + send_buffer_displ;
                step->buffer_length = send_buffer_length;
                uint16_t send_flag = 0;
                /* calculate the flag and fragments for sender */
                status = ucg_builtin_step_calc_fragment(send_buffer_length, params->send.dt_len,
                        &step->phase->ep_thresh[step->iter_ep], &step->fragment_length, &step->fragments, &send_flag);
                if (status !=UCS_OK) {
                    step->resend_flag = UCG_BUILTIN_OP_STEP_RESEND;
                    step->flags = orig_flags;
                    return status;
                }

                step->flags |= send_flag;
                /* register memory for zero-copy */
                if (step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY) {
                    status = ucg_builtin_dynamic_zcopy_prep(step, step->iter_ep);
                    if (status != UCS_OK) {
                        step->resend_flag = UCG_BUILTIN_OP_STEP_RESEND;
                        step->flags = orig_flags;
                        return status;
                    }
                }

                status = ucg_builtin_step_am_dynamic(req, step, *ep_iter, 0);
                if (status != UCS_OK) {
                    step->resend_flag = UCG_BUILTIN_OP_STEP_RESEND;
                    step->flags = orig_flags;
                    return status;
                }

                /* restor the original state of step->flag */
                step->flags = orig_flags;
            }
        }
        step->iter_ep++;
    }
    return status;
}

/*
 * Below is a set of macros, generating most bit-field combinations of
 * step->flags inside @ref ucg_builtin_step_execute() .
 */
#define case_send(req, ureq, step, phase, _send_func) do {                       \
        if ((is_rs1 || is_r1s) && ((step)->iter_ep == 0)) {                      \
            uint32_t new_cnt = (step)->iter_ep = is_r1s ? 1 : (phase)->ep_cnt - 1; \
            ucs_assert(new_cnt > 0);                                             \
            if (is_pipelined) {                                                  \
                memset((void*)(step)->fragment_pending,                          \
                       new_cnt, (step)->fragments);                              \
            }                                                                    \
            (req)->pending = new_cnt * (step)->fragments_recv;                   \
            /* Beyond the case we fall-back to receiving */                      \
            goto finish_send;                                                    \
        }                                                                        \
                                                                                 \
        if (is_recv && is_zcopy && !is_resend) {                                 \
            /* Both zcopy callbacks and incoming messages use pending, so ... */ \
            (req)->pending = (step)->fragments_recv * (phase)->ep_cnt +          \
                    (step)->fragments * (phase)->ep_cnt;                         \
        }                                                                        \
                                                                                 \
        /* Perform one or many send operations, unless an error occurs */        \
        /* for waypoint, reset the req->pending to complete zcomp cb */          \
        if ((is_rs1 || is_r1s) && is_zcopy && !is_resend) {                      \
            uint32_t new_cnt = is_rs1 ? 1 : (phase)->ep_cnt - 1;                 \
            ucs_assert(new_cnt > 0);                                             \
            (req)->pending = new_cnt * (step)->fragments;                        \
        }                                                                        \
        if (is_one_ep) {                                                         \
            ucs_assert(!is_pipelined); /* makes no sense in single-ep case */    \
            status = _send_func (req, step, (phase)->single_ep, 0);              \
            if (ucs_unlikely(UCS_STATUS_IS_ERR(status))) {                       \
                goto step_execute_error;                                         \
            }                                                                    \
        } else {                                                                 \
            if ((is_pipelined) && (ucs_unlikely((step)->iter_offset ==           \
                    UCG_BUILTIN_OFFSET_PIPELINE_PENDING))) {                     \
                /* find a pending offset to progress */                          \
                unsigned frag_idx = 0;                                           \
                while ((frag_idx < (step)->fragments) &&                         \
                       ((step)->fragment_pending[frag_idx] ==                    \
                               UCG_BUILTIN_FRAG_PENDING)) {                      \
                    frag_idx++;                                                  \
                }                                                                \
                ucs_assert(frag_idx < (step)->fragments);                        \
                (step)->iter_offset = frag_idx * (step)->fragment_length;        \
            }                                                                    \
                                                                                 \
            uct_ep_h *ep_iter, *ep_last;                                         \
            ep_iter = ep_last = (phase)->multi_eps;                              \
            ep_iter += (step)->iter_ep;                                          \
            ep_last += (phase)->ep_cnt;                                          \
            do {                                                                 \
                status = _send_func (req, step, *ep_iter, is_pipelined);         \
                if (ucs_unlikely(UCS_STATUS_IS_ERR(status))) {                   \
                    /* Store the pointer, e.g. for UCS_ERR_NO_RESOURCE */        \
                    (step)->iter_ep = ep_iter - (phase)->multi_eps;              \
                    goto step_execute_error;                                     \
                }                                                                \
                                                                                 \
                if (is_scatter) {                                                \
                    (step)->send_buffer += (step)->buffer_length;                \
                }                                                                \
            } while (++ep_iter < ep_last);                                       \
                                                                                 \
            if (is_scatter) { /* restore after a temporary pointer change */     \
                (step)->send_buffer -= (phase)->ep_cnt * (step)->buffer_length;  \
            }                                                                    \
                                                                                 \
            if (is_pipelined) {                                                  \
                /* Reset the iterator for the next pipelined incoming packet */  \
                (step)->iter_ep = is_r1s ? 1 : (phase)->ep_cnt - 1;              \
                ucs_assert(is_r1s + is_rs1 > 0);                                 \
                                                                                 \
                /* Check if this invocation is a result of a resend attempt */   \
                unsigned idx = (step)->iter_offset / (step)->fragment_length;    \
                if (ucs_unlikely((step)->fragment_pending[idx] ==                \
                        UCG_BUILTIN_FRAG_PENDING)) {                             \
                    (step)->fragment_pending[idx] = 0;                           \
                                                                                 \
                    /* Look for other packets in need of resending */            \
                    for (idx = 0; idx < (step)->fragments; idx++) {              \
                        if ((step)->fragment_pending[idx] ==                     \
                                UCG_BUILTIN_FRAG_PENDING) {                      \
                            /* Found such packets - mark for next resend */      \
                            (step)->iter_offset = idx * (step)->fragment_length; \
                            status            = UCS_ERR_NO_RESOURCE;             \
                            goto step_execute_error;                             \
                        }                                                        \
                    }                                                            \
                } else {                                                         \
                    ucs_assert((step)->fragment_pending[idx] == 0);              \
                }                                                                \
                (step)->iter_offset = UCG_BUILTIN_OFFSET_PIPELINE_READY;         \
            } else {                                                             \
                (step)->iter_ep = 0; /* Reset the per-step endpoint iterator */  \
                ucs_assert((step)->iter_offset == 0);                            \
            }                                                                    \
        }                                                                        \
                                                                                 \
        /* avoid to enter directly into comp_step_cb without finish pipeline */  \
        if (is_pipelined && (step)->fragment_pending[(step)->fragments-1] != 0)  \
            goto finish_send;                                                    \
                                                                                 \
        /* when pipelining is finished, set iter_offset & iter_ep to be 0! */    \
        if (is_pipelined && (step)->fragment_pending[(step)->fragments-1] == 0)  \
        {                                                                        \
            (step)->iter_offset = 0;                                             \
            (step)->iter_ep     = 0;                                             \
        }                                                                        \
                                                                                 \
        /* Potential completions (the operation may have finished by now) */     \
        if ((!is_recv && !is_zcopy) || ((req)->pending == 0)) {                  \
            /* Nothing else to do - complete this step */                        \
            if (is_last) {                                                       \
                if (!(ureq)) {                                                   \
                    ucg_builtin_comp_last_step_cb(req, UCS_OK);                  \
                    if ((step)->buffer_length == 0) { /* speciallly for barrier */ \
                        ucg_collective_release_barrier(                          \
                        (req)->op->super.plan->group);                           \
                    }                                                            \
                }                                                                \
                return UCS_OK;                                                   \
            } else {                                                             \
                return ucg_builtin_comp_step_cb(req, ureq);                      \
            }                                                                    \
        }                                                                        \
    } while (0)                                                                            \

#define INIT_USER_REQUEST_IF_GIVEN(user_req, req) {                              \
    if (ucs_unlikely((user_req) != NULL)) {                                      \
        /* Initialize user's request part (checked for completion) */            \
        if (*(user_req)) {                                                       \
            (req)->comp_req = *(user_req) - 1;                                   \
        } else {                                                                 \
            (req)->comp_req = &(req)->super;                                     \
            *(user_req)     = &(req)->super + 1;                                 \
        }                                                                        \
        (req)->comp_req->flags = 0;                                              \
        user_req = NULL;                                                         \
    }                                                                            \
}
/*
 * Executing a single step is the heart of the Builtin planner.
 * This function advances to the next step (some invocations negate that...),
 * sends and then receives according to the instructions of this step.
 * The function returns the status, typically one of the following:
 * > UCS_OK - collective operation (not just this step) has been completed.
 * > UCS_INPROGRESS - sends complete, waiting on some messages to be receieved.
 * > otherwise - an error has occurred.
 *
 * For example, a "complex" case is when the message is fragmented, and requires
 * both receiveing and sending in a single step, like in REDUCE_WAYPOINT. The
 * first call, coming from @ref ucg_builtin_op_trigger() , will enter the first
 * branch ("step_ep" is zero when a new step is starting), will process some
 * potential incoming messages (arriving beforehand) - returning UCS_INPROGRESS.
 * Subsequent calls to "progress()" will handle the rest of the incoming
 * messages for this step, and eventually call this function again from within
 * @ref ucg_builtin_comp_step_cb() . This call will choose the second branch,
 * the switch-case, which will send the message and
 */
UCS_PROFILE_FUNC(ucs_status_t, ucg_builtin_step_execute, (req, user_req),
                 ucg_builtin_request_t *req, ucg_request_t **user_req)
{
    /* UCT level communication operations */
    int is_dummy, is_short, is_bcopy, is_zcopy;
    /* Receive-related indicators, for non-send-only steps */
    int is_recv, is_rs1, is_r1s, is_pipelined;
    /* Step-completion-related indicators */
    int is_last, is_one_ep, is_resend;
    /* Send-related  parameters */
    int is_scatter, is_fragmented;

    uint16_t local_id;
    ucs_status_t status;
    ucg_builtin_op_step_t *step     = req->step;
    ucg_builtin_plan_phase_t *phase = step->phase;
    ucg_builtin_comp_slot_t *slot   = ucs_container_of(req, ucg_builtin_comp_slot_t, req);
    step->am_header.coll_id         = slot->coll_id;
    ucs_assert(slot->step_idx == step->am_header.step_idx);

    ucs_debug("step_execute, coll_id:%u, step_idx:%u, step->flags:0x%x, send_buffer:%p, recv_buffer:%p",
              slot->coll_id, slot->step_idx, step->flags, step->send_buffer, step->recv_buffer);

    /*
     * For some operations, like MPI_Alltoall, the
     * discrete data should be packed then send (e.g. Bruck algorithms).
     */
    if (req->step->send_cb != NULL && !req->is_send_cb_called) {
        req->step->send_cb(req);
        req->is_send_cb_called = 1;
        if (req->inc_req_status != UCS_OK) {
            status = req->inc_req_status;
            goto step_execute_error;
        }
        if (req->ladd_req_status != UCS_OK) {
            status = req->ladd_req_status;
            goto step_execute_error;
        }
        if (req->plummer_req_status != UCS_OK) {
            status = req->plummer_req_status;
            goto step_execute_error;
        }
    }

    is_scatter    = step->flags & UCG_BUILTIN_OP_STEP_FLAG_LENGTH_PER_REQUEST;
    is_one_ep     = step->flags & UCG_BUILTIN_OP_STEP_FLAG_SINGLE_ENDPOINT;
    is_last       = step->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP;
    is_pipelined  = step->flags & UCG_BUILTIN_OP_STEP_FLAG_PIPELINED;
    is_r1s        = step->flags & UCG_BUILTIN_OP_STEP_FLAG_RECV1_BEFORE_SEND;
    is_rs1        = step->flags & UCG_BUILTIN_OP_STEP_FLAG_RECV_BEFORE_SEND1;
    is_recv       = step->flags & UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND;
    is_resend     = step->resend_flag & UCG_BUILTIN_OP_STEP_RESEND;

    is_dummy      = (step->flags == 0);
    is_short      = step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT;
    is_bcopy      = step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY;
    is_zcopy      = step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY;
    is_fragmented = step->flags & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED;

    if (phase->ex_attr.is_variable_len) {
        status = ucg_builtin_dynamic_send_recv(req, user_req);
        if (status != UCS_OK) {
            goto step_execute_error;
        }
        /* calculate zcopy pending */
        unsigned ep_iter;
        for (ep_iter = 0; ep_iter < phase->send_ep_cnt; ep_iter++) {
            req->pending += step->zcopys[ep_iter].zcopy_pending;
        }

        ucg_builtin_dynamic_calc_pending(req, user_req);
        if (req->pending == 0) {
            if (is_last) {
                if (!user_req) {
                    ucg_builtin_comp_last_step_cb(req, UCS_OK);
                }
                return UCS_OK;
            } else {
                return ucg_builtin_comp_step_cb(req, user_req);
            }
        } else {
            ucg_builtin_step_var_callbacks(req->pending, &step->recv_cb);
        }
    } else {
        ucs_debug("is_dummy:%d is_fragmented:%d is_short:%d is_bcopy:%d is_zcopy:%d", is_dummy, is_fragmented, is_short,
            is_bcopy, is_zcopy);
        if (is_dummy) case_send(req, user_req, step, phase, ucg_builtin_step_dummy_send);

        if (!is_fragmented) {
            if (is_short) {
                case_send(req, user_req, step, phase, ucg_builtin_step_am_short_one);
            } else if (is_bcopy) {
                case_send(req, user_req, step, phase, ucg_builtin_step_am_bcopy_one);
            } else if (is_zcopy) {
                case_send(req, user_req, step, phase, ucg_builtin_step_am_zcopy_one);
            }
        } else {
            if (is_short) {
                case_send(req, user_req, step, phase, ucg_builtin_step_am_short_max);
            } else if (is_bcopy) {
                case_send(req, user_req, step, phase, ucg_builtin_step_am_bcopy_max);
            } else if (is_zcopy) {
                case_send(req, user_req, step, phase, ucg_builtin_step_am_zcopy_max);
            }
        }
    }
finish_send:

    /* Initialize the users' request object, if applicable */
    INIT_USER_REQUEST_IF_GIVEN(user_req, req);
    slot->cb = step->recv_cb;
    step->resend_flag = UCG_BUILTIN_OP_STEP_FIRST_SEND;

    /* Check pending incoming messages - invoke the callback on each one */
    if (ucs_likely(ucs_list_is_empty(&slot->msg_head))) {
        return UCS_INPROGRESS;
    }

    if (is_zcopy && is_recv) {
        /* Count pre-arrived zcopy msg to req->step->zcopy.num_store */
        local_id = slot->local_id;
        /* receive from "multiple" EPs with "multiple" fragments */
        unsigned recv_zcopy_cnt = step->fragments_recv * step->phase->ep_cnt;
        ucg_builtin_comp_desc_t *desc = NULL;
        ucg_builtin_comp_desc_t *iter = NULL;
        ucs_list_for_each_safe(desc, iter, &slot->msg_head, super.tag_list[0]) {
            if (ucs_likely(desc->header.local_id == local_id)) {
                /* The number of store will not bigger than recv fragments */
                if (++step->zcopy.num_store >= recv_zcopy_cnt) {
                    break;
                }
            }
        }
        return UCS_INPROGRESS;
    }

    return (is_r1s && req->recv_comp) ? UCS_INPROGRESS : ucg_builtin_msg_process(slot, req);

    /************************** Error flows ***********************************/
step_execute_error:
    if (status == UCS_ERR_NO_RESOURCE) {
        /* Special case: send incomplete - enqueue for resend upon progress */
        INIT_USER_REQUEST_IF_GIVEN(user_req, req);

        if (step->flags & UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) {
            step->fragment_pending[step->iter_offset / step->fragment_length] =
                    UCG_BUILTIN_FRAG_PENDING;
            step->iter_offset = UCG_BUILTIN_OFFSET_PIPELINE_PENDING;
        }

        ucs_list_add_tail(req->op->resend, &req->send_list);
        return UCS_INPROGRESS;
    }

    /* Generic error - reset the collective and mark the request as completed */
    ucg_builtin_comp_last_step_cb(req, status);
    return status;
}

ucs_status_t ucg_builtin_msg_process(ucg_builtin_comp_slot_t *slot, ucg_builtin_request_t *req)
{
    static unsigned loop_cnt = 0;
    static unsigned is_return = 0;
    unsigned max_msg_list_size = ((ucg_builtin_config_t*) req->op->super.plan->planner->plan_config)->max_msg_list_size;

    /* Look for matches in list of packets waiting on this slot */
    uint16_t local_id = slot->local_id;
    ucg_builtin_op_step_t *step = req->step;

    ucg_builtin_comp_desc_t *desc = NULL;
    ucg_builtin_comp_desc_t *iter = NULL;

    ucs_list_for_each_safe(desc, iter, &slot->msg_head, super.tag_list[0]) {
        /*
         * Note: stored message coll_id can be either larger or smaller than
         * the one currently handled - due to coll_id wrap-around.
         */
        if (ucs_likely(desc->header.local_id == local_id)) {
            /* Check loop count - return in_progress if attach max size */
            if (++loop_cnt > max_msg_list_size) {
                is_return = 1;
                loop_cnt--;
                return UCS_INPROGRESS;
            }

            /* Remove the packet (next call may lead here recursively) */
            ucs_list_del(&desc->super.tag_list[0]);

            if (req->step->phase->is_swap) {
                ucg_builtin_swap_net_recv(&desc->data[0], desc->super.length,
                                          desc->header.remote_offset, &slot->req);
            }

            /* Handle this "waiting" packet, possibly completing the step */
            int is_step_done = step->recv_cb(&slot->req,
                                             desc->header.remote_offset, &desc->data[0],
                                             desc->super.length);
            desc->release(desc);
            desc = NULL;
            loop_cnt--;

            /* If the step has indeed completed - check the entire op */
            if (is_step_done) {
                /* Continue msg processing if return by loop check */
                if (loop_cnt == 0 && is_return == 1) {
                    is_return = 0;
                    return ucg_builtin_msg_process(slot, req);
                } else {
                    return (req->comp_req->flags & UCP_REQUEST_FLAG_COMPLETED) ?
                           req->comp_req->status : UCS_INPROGRESS;
                }
            }
        }
    }

    return UCS_INPROGRESS;
}

void *ucg_builtin_pack_rank(void *step, const void *send_buffer, size_t buffer_len, size_t *new_buffer_len)
{
    ucg_builtin_op_step_t *temp_step = (ucg_builtin_op_step_t *)step;
    ucg_group_member_index_t my_idx = temp_step->phase->ex_attr.packed_rank;
    int8_t *temp_buffer = (int8_t *)temp_step->variable_length.pack_rank_buffer;

    memcpy(temp_buffer, (int8_t *)&my_idx, sizeof(ucg_group_member_index_t));
    memcpy(temp_buffer + sizeof(ucg_group_member_index_t), (int8_t *)send_buffer, buffer_len);
    *new_buffer_len = buffer_len + sizeof(ucg_group_member_index_t);
    return temp_buffer;
}

ucg_group_member_index_t ucg_builtin_unpack_rank(const void *send_buffer, size_t buffer_length)
{
    ucs_assert(buffer_length >= sizeof(ucg_group_member_index_t));
    return *(ucg_group_member_index_t *)send_buffer;
}

ucs_status_t ucg_builtin_step_alloc_pack_rank_buffer(ucg_builtin_op_step_t *step, size_t buffer_length)
{
    if (step->variable_length.pack_rank_buffer == NULL) {
        step->variable_length.pack_rank_buffer =
           (int8_t *)UCS_ALLOC_CHECK(buffer_length + sizeof(ucg_group_member_index_t) * step->phase->send_ep_cnt,
                                    "pack rank buffer");
        step->variable_length.pack_rank_func = ucg_builtin_pack_rank;
        step->variable_length.unpack_rank_func = ucg_builtin_unpack_rank;
    }
    return UCS_OK;
}

void ucg_builtin_step_free_pack_rank_buffer(ucg_builtin_op_step_t *step)
{
    ucg_builtin_free((void **)&step->variable_length.pack_rank_buffer);
    step->variable_length.pack_rank_func = NULL;
    step->variable_length.unpack_rank_func = NULL;
}

ucs_status_t ucg_builtin_step_set_contig(ucg_builtin_op_step_t *step,
                                         int is_contig)
{
    ucs_status_t status = UCS_OK;
    if (is_contig) {
        return status;
    }

    /* only non-contig dt will malloc contig_buffer and malloc only once. */
    if (!is_contig && step->non_contig.contig_buffer == NULL) {
        step->non_contig.contig_buffer = (int8_t *)UCS_ALLOC_CHECK(step->buffer_length, "contig_buffer");
    }

    if (step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY) {
        /* The send buffer changed, reregister it */
        uct_md_mem_dereg(step->uct_md, step->zcopy.memh);
        status = uct_md_mem_reg(step->uct_md, step->non_contig.contig_buffer,
                                step->buffer_length, UCT_MD_MEM_ACCESS_ALL, &step->zcopy.memh);
        if (status != UCS_OK) {
            if (step->zcopy.zcomp != NULL) {
                ucs_free(step->zcopy.zcomp);
                step->zcopy.zcomp = NULL;
            }
            ucs_info("contig_buffer md mem register failed.");
            return status;
        }
    }

    return status;
}

void ucg_builtin_step_release_contig(ucg_builtin_op_step_t *step)
{
    ucg_builtin_free((void **)&step->non_contig.contig_buffer);
}

static void free_zcomp(ucg_builtin_op_step_t *step)
{
    ucg_builtin_free((void **)&step->zcopy.zcomp);
}

static void free_zcopy_info(ucg_builtin_op_step_t *step)
{
    if (step->zcopys != NULL) {
        unsigned i;
        for (i = 0; i < step->phase->send_ep_cnt; i++) {
            if (step->zcopys[i].zcomp != NULL) {
                uct_md_mem_dereg(step->zcopys[i].uct_md, step->zcopys[i].memh);
                ucs_free(step->zcopys[i].zcomp);
            }
        }
        ucg_builtin_free((void **)&step->zcopys);
    }
}

static void free_fragment_pending(ucg_builtin_op_step_t *step)
{
    if (step->zcopy.zcomp != NULL) {
        ucg_builtin_free((void **)&step->fragment_pending);
    }
}

void ucg_builtin_op_discard(ucg_op_t *op)
{
    ucg_builtin_op_t *builtin_op = (ucg_builtin_op_t*)op;
    ucg_builtin_op_step_t *step = &builtin_op->steps[0];
    do {
        if ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY) && (step->phase->ex_attr.is_variable_len == 0)) {
            uct_md_mem_dereg(step->uct_md, step->zcopy.memh);
            free_zcomp(step);
        } else {
            free_zcopy_info(step); //for dynamic sending, dereg for all zcopys
        }

        if (step->flags & UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) {
            free_fragment_pending(step);
        }
        /* Free the allreduce buffer */
        if (step->reduce_buff != NULL) {
            free(step->reduce_buff);
            step->rbuf_count = 0;
            step->reduce_buff = NULL;
        }
        ucg_builtin_step_release_contig(step);
    } while (!((step++)->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP));
    ucg_builtin_free((void **)&builtin_op->temp_data_buffer);
    ucs_mpool_put_inline(op);
}

ucs_status_t ucg_builtin_op_trigger(ucg_op_t *op, ucg_coll_id_t coll_id, ucg_request_t **request)
{
    /* Allocate a "slot" for this operation, from a per-group array of slots */
    ucg_builtin_op_t *builtin_op  = (ucg_builtin_op_t*)op;
    ucg_builtin_comp_slot_t *slot = &builtin_op->slots[coll_id % UCG_BUILTIN_MAX_CONCURRENT_OPS];
    slot->coll_id                 = coll_id;
    if (ucs_unlikely(slot->cb != NULL)) {
        ucs_error("UCG Builtin planner exceeded the max concurrent collectives.");
        return UCS_ERR_NO_RESOURCE;
    }

    /* Initialize the request structure, located inside the selected slot s */
    ucg_builtin_request_t *builtin_req = &slot->req;
    builtin_req->op                    = builtin_op;
    ucg_builtin_op_step_t *first_step  = builtin_op->steps;
    first_step->iter_ep                = 0;
    builtin_req->step                  = first_step;
    builtin_req->pending               = first_step->fragments_recv *
                                         first_step->phase->ep_cnt;
    builtin_req->recv_comp             = 0;
    builtin_req->is_send_cb_called = 0;
    builtin_req->inc_req_status = UCS_OK;
    builtin_req->ladd_req_status = UCS_OK;
    builtin_req->plummer_req_status = UCS_OK;
    slot->step_idx                     = first_step->am_header.step_idx;
    ucs_debug("op trigger: step idx %u coll id %u", slot->step_idx, coll_id);

    /* Sanity checks */
    ucs_assert(first_step->iter_offset == 0);
    ucs_assert(first_step->iter_ep == 0);
    ucs_assert(request != NULL);

    /*
     * For some operations, like MPI_Reduce, MPI_Allreduce or MPI_Gather, the
     * local data has to be aggregated along with the incoming data. In others,
     * some shuffle is required once before starting (e.g. Bruck algorithms).
     */
    if (builtin_op->init_cb != NULL) {
#if ENABLE_UCG_HICOLL
        builtin_op->inc_init_status = UCS_OK;
#endif
        builtin_op->init_cb(builtin_op);
#if ENABLE_UCG_HICOLL
        if (builtin_op->inc_init_status != UCS_OK) {
            return builtin_op->inc_init_status;
        }
#endif
    }

    /* Consider optimization, if this operation is used often enough */
    if (ucs_unlikely(--builtin_op->opt_cnt == 0)) {
        ucs_status_t optm_status = builtin_op->optm_cb(builtin_op);
        if (ucs_unlikely(UCS_STATUS_IS_ERR(optm_status))) {
            return optm_status;
        }
        /* Need to return original status, because it can be OK or INPROGRESS */
    }

    /* Start the first step, which may actually complete the entire operation */
    return ucg_builtin_step_execute(builtin_req, request);
}

static size_t ucg_builtin_get_inc_data_length(const ucg_collective_params_t *params)
{
    unsigned inc_header_size = 0;
#if ENABLE_UCG_HICOLL
    inc_header_size += inc_get_header_size();
#endif
    enum ucg_collective_modifiers modifiers = params->type.modifiers;
    if (modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_ALLREDUCE] ||
       modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_BCAST]) {
        unsigned data_size = params->send.count * params->send.dt_len;
        return data_size + inc_header_size;
    } else if (modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_BARRIER]) {
        return 1 + inc_header_size;
    }
    return params->send.count * params->send.dt_len;
}

/******************************************************************************
 *                                                                            *
 *                            Operation Creation                              *
 *                                                                            *
 ******************************************************************************/
static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_step_send_flags(ucg_builtin_op_step_t *step,
                                                                    ucg_builtin_plan_phase_t *phase,
                                                                    const ucg_collective_params_t *params,
                                                                    size_t dt_len,
                                                                    enum ucg_builtin_op_step_flags *send_flag)
{
    size_t length = phase->method == UCG_PLAN_METHOD_INC ? ucg_builtin_get_inc_data_length(params) :
                                                    step->buffer_length;
    unsigned partial_length = 0;

    /* Flag whether to go error and resend data */
    step->resend_flag = UCG_BUILTIN_OP_STEP_FIRST_SEND;

    /*
     * Short messages (e.g. RDMA "inline")
     */
    if (ucs_likely((length <= phase->send_thresh.max_short_one) &&
                   (phase->send_thresh.max_short_one != 0))) {
        /* Short send - single message */
        *send_flag = UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT;
        step->fragments = 1;
    } else if (ucs_likely((length <= phase->send_thresh.max_short_max) &&
                          (phase->send_thresh.max_short_max != 0))) {
        if (ucs_likely(dt_len <= phase->send_thresh.max_short_one)) {
            /* Short send - multiple messages */
            step->fragment_length = phase->send_thresh.max_short_one - (phase->send_thresh.max_short_one % dt_len);
        } else {
            step->fragment_length = phase->send_thresh.max_short_one;
        }
        ucs_assert(step->fragment_length > 0);
        *send_flag = (enum ucg_builtin_op_step_flags)(UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT |
                UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED);
        partial_length = (length % step->fragment_length) > 0;
        step->fragments = length / step->fragment_length + partial_length;

    /*
     * Large messages, if supported (e.g. RDMA "zero-copy")
     */
    } else if (ucs_unlikely((length >  phase->send_thresh.max_bcopy_max) &&
                            (length <= phase->send_thresh.md_attr_cap_max_reg))) {
        if (ucs_likely(length < phase->send_thresh.max_zcopy_one)) {
            /* ZCopy send - single message */
            *send_flag            = UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY;
            step->fragments       = 1;
        } else {
            /* ZCopy send - multiple message */
            step->fragment_length = (ucs_likely((dt_len <= phase->send_thresh.max_zcopy_one) && (dt_len != 0))) ?
                                    phase->send_thresh.max_zcopy_one - (phase->send_thresh.max_zcopy_one % dt_len) :
                                    phase->send_thresh.max_zcopy_one;
            ucs_assert(step->fragment_length > 0);
            *send_flag = (enum ucg_builtin_op_step_flags)(UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY |
                    UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED);
            partial_length = (length % step->fragment_length) > 0;
            step->fragments = length / step->fragment_length + partial_length;
        }

    /*
     * Medium messages
     */
    } else if (ucs_likely(length <= phase->send_thresh.max_bcopy_one)) {
        /* BCopy send - single message */
        *send_flag = UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY;
        step->fragment_length = length;
        step->fragments       = 1;
    } else {
        /* BCopy send - multiple messages */
        step->fragment_length = (ucs_likely((dt_len <= phase->send_thresh.max_bcopy_one) && (dt_len != 0))) ?
                                phase->send_thresh.max_bcopy_one - (phase->send_thresh.max_bcopy_one % dt_len) :
                                phase->send_thresh.max_bcopy_one;
        ucs_assert(step->fragment_length > 0);
        *send_flag = (enum ucg_builtin_op_step_flags)(UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY |
                UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED);
        partial_length = (length % step->fragment_length) > 0;
        step->fragments = length / step->fragment_length + partial_length;
    }

    ucs_debug("step send_flags:0x%x, length:%lu, fragments:%u, fragment_length:%lu, partial_length:%u, dt_len:%lu",
              *send_flag, length, step->fragments, step->fragment_length, partial_length, dt_len);

    return UCS_OK;
}


static UCS_F_ALWAYS_INLINE void ucg_builtin_step_fragment_flags(size_t thresh_one,
                                                                size_t dt_len,
                                                                size_t length,
                                                                ucg_builtin_op_step_t *step,
                                                                ucg_builtin_plan_phase_t *phase,
                                                                enum ucg_builtin_op_step_flags *recv_flag)
{
    unsigned partial_length = 0;
    size_t fragment_length = 0;
    if (ucs_unlikely(dt_len > thresh_one)) {
        phase->segmented = 1;
        fragment_length = thresh_one;
    } else {
        fragment_length = thresh_one - (thresh_one % dt_len);
    }

    if (fragment_length == 0) {
        return;
    }
    *recv_flag = UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED;
    partial_length = (length % fragment_length) > 0;
    step->fragments_recv = length / fragment_length + partial_length;
}

/*
 * For some algorithms (e.g. Bruck, Ring), the thresholds of sender and receiver
 * are not same!
 * So, receiver should set fragment_recv according to phase->max_XXX_recv and
 * recv_flag should also be set to distinguish with send_flag to choose correct recv_cb.
 */
static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_step_recv_flags(ucg_builtin_op_step_t *step,
                                                                    ucg_builtin_plan_phase_t *phase,
                                                                    const ucg_collective_params_t *params,
                                                                    size_t dt_len,
                                                                    enum ucg_builtin_op_step_flags *recv_flag)
{
    *recv_flag = (enum ucg_builtin_op_step_flags)0;
    size_t length = phase->ex_attr.is_inequal ? step->buffer_length_recv : step->buffer_length;
    length = phase->method == UCG_PLAN_METHOD_INC ? ucg_builtin_get_inc_data_length(params) :
                                                    length;
    size_t fragment_length = 0;
    unsigned partial_length = 0;

    /* for ring, the length of send_buffer and recv_buffer may be different */
    if (phase->method == UCG_PLAN_METHOD_REDUCE_SCATTER_RING ||
        phase->method == UCG_PLAN_METHOD_ALLGATHER_RING) {
        length = step->buffer_length_recv;
    }
    /*
     * Short messages (e.g. RDMA "inline")
     */
    if (length <= phase->recv_thresh.max_short_one) {
        /* Short send - single message */
        step->fragments_recv = 1;
    } else if (length <= phase->recv_thresh.max_short_max) {
        /* Short send - multiple messages */
        ucg_builtin_step_fragment_flags(phase->recv_thresh.max_short_one, dt_len, length,
                                        step, phase, recv_flag);
    /*
     * Large messages, if supported (e.g. RDMA "zero-copy")
     */
    } else if ((length > phase->recv_thresh.max_bcopy_max) &&
               (length <= phase->recv_thresh.md_attr_cap_max_reg)) {
        if (length < phase->recv_thresh.max_zcopy_one) {
            /* ZCopy send - single message */
            step->fragments_recv = 1;
        } else {
            /* ZCopy send - multiple message */
            ucg_builtin_step_fragment_flags(phase->recv_thresh.max_zcopy_one, dt_len, length,
                                            step, phase, recv_flag);
        }
    /*
     * Medium messages
     */
    } else if (length <= phase->recv_thresh.max_bcopy_one) {
        /* BCopy send - single message */
        step->fragments_recv = 1;
    } else {
        /* BCopy send - multiple messages */
        if (ucs_unlikely(dt_len > phase->recv_thresh.max_bcopy_one)) {
            phase->segmented = 1;
            fragment_length = phase->recv_thresh.max_bcopy_one;
        } else {
            fragment_length = phase->recv_thresh.max_bcopy_one - (phase->recv_thresh.max_bcopy_one % dt_len);
        }

        *recv_flag = UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED;
        if (phase->recv_thresh.max_bcopy_one > 0) {
            partial_length = (length % fragment_length) > 0;
            step->fragments_recv = length / fragment_length + partial_length;
        } else {
            ucs_info("phase->recv_thresh.max_bcopy_one is negative or zero");
            partial_length = 0;
            step->fragments_recv = length;
        }
    }

    ucs_debug("step recv_flags:0x%x, length:%lu, fragments:%u, fragment_length:%lu, partial_length:%u, dt_len:%lu",
              *recv_flag, length, step->fragments_recv, fragment_length, partial_length, dt_len);

    return UCS_OK;
}

size_t ucg_builtin_get_dt_len(ucp_dt_generic_t *dt_gen)
{
    /* need to generate a one-time state to figure out the packed size */
    if(dt_gen == NULL) {
        return 0;
    }
    void *state_gen = dt_gen->ops.start_pack(dt_gen->context, NULL, 1);
    size_t len = dt_gen->ops.packed_size(state_gen);
    dt_gen->ops.finish(state_gen);
    return len;
}

/* ucg_builtin_coll_params_t init_buf malloc elsewhere and release elsewhere */
ucg_builtin_coll_params_t *ucg_builtin_allocate_coll_params(unsigned local_member_cnt)
{
    ucg_builtin_coll_params_t *params =
        ucs_malloc(sizeof(ucg_builtin_coll_params_t), "allocate variable length params");
    if (params == NULL) {
        return NULL;
    }

    params->counts = ucs_malloc(local_member_cnt * sizeof(int), "allocate variable length counts");
    if (params->counts == NULL) {
        ucg_builtin_free((void **)&params);
        return NULL;
    }
    memset(params->counts, 0, local_member_cnt * sizeof(int));

    params->displs = ucs_malloc(local_member_cnt * sizeof(int), "allocate variable length displs");
    if (params->displs == NULL) {
        ucg_builtin_free((void **)&params->counts);
        ucg_builtin_free((void **)&params);
        return NULL;
    }
    memset(params->displs, 0, local_member_cnt * sizeof(int));
    return params;
}

void ucg_builtin_free_coll_params(ucg_builtin_coll_params_t **params)
{
    if (*params != NULL) {
        ucg_builtin_free((void **)&(*params)->displs);
        ucg_builtin_free((void **)&(*params)->counts);
        ucg_builtin_free((void **)params);
    }
}

ucs_status_t ucg_builtin_step_create(ucg_builtin_op_t *op,
                                     ucg_builtin_plan_phase_t *phase,
                                     ucp_datatype_t send_dtype,
                                     ucp_datatype_t recv_dtype,
                                     unsigned extra_flags,
                                     unsigned base_am_id,
                                     ucg_group_id_t group_id,
                                     const ucg_collective_params_t *params,
                                     int8_t **current_data_buffer,
                                     ucg_builtin_op_step_t *step)
{
    ucs_status_t status;
    /* Set the parameters determining the send-flags later on */
    int is_send_contig       = UCG_DT_IS_CONTIG(params, send_dtype);
    int is_recv_contig       = UCG_DT_IS_CONTIG(params, recv_dtype);
    size_t send_dt_len       = is_send_contig ? params->send.dt_len :
                               ucg_builtin_get_dt_len(ucp_dt_generic(send_dtype));
    size_t recv_dt_len       = is_recv_contig ? params->recv.dt_len :
                               ucg_builtin_get_dt_len(ucp_dt_generic(recv_dtype));
    memset(step, 0, sizeof(ucg_builtin_op_step_t));
    step->buffer_length      = send_dt_len * params->send.count;
    step->uct_md             = phase->md;

    /* Note: we assume all the UCT endpoints have the same interface */
    step->phase              = phase;
    step->am_id              = base_am_id;
    step->am_header.group_id = group_id;
    step->am_header.step_idx = (ucg_step_idx_t)phase->step_index;
    step->iter_ep            = 0;
    step->iter_offset        = 0;
    step->fragment_pending   = NULL;

    step->send_coll_params = NULL;
    step->recv_coll_params = NULL;
    step->variable_length.pack_rank_buffer = NULL;
    step->variable_length.pack_rank_func = NULL;
    step->variable_length.unpack_rank_func = NULL;
    /* allocate the zcopy_info array for dynamic sending */
    step->zcopys = (ucg_builtin_zcopy_info_t *)UCS_ALLOC_CHECK(phase->ep_cnt * sizeof(ucg_builtin_zcopy_info_t),
    "ucg_zcopys_info");
    memset(step->zcopys, 0, phase->ep_cnt * sizeof(ucg_builtin_zcopy_info_t));
    step->recv_buffer        = (int8_t*)params->recv.buf;
    step->send_buffer        = ((params->send.buf == MPI_IN_PLACE) ||
            !(extra_flags & UCG_BUILTIN_OP_STEP_FLAG_FIRST_STEP)) ?
                    (int8_t*)params->recv.buf : (int8_t*)params->send.buf;
    step->send_cb            = NULL;
    if (phase->init_phase_cb != NULL) {
        status = phase->init_phase_cb(phase, params);
        if (status != UCS_OK) {
            return status;
        }
    }
    step->non_contig.contig_buffer = NULL;
    step->non_contig.pack_state   = NULL;
    step->non_contig.unpack_state = NULL;
    step->non_contig.pack_state_recv = NULL;
    step->reduce_buff = NULL;

    ucg_builtin_plan_t *builtin_plan = (ucg_builtin_plan_t*)op->super.plan;

    if (phase->method == UCG_PLAN_METHOD_ALLTOALLV_LADD) {
        step->send_coll_params =
            (ucg_builtin_coll_params_t *)ucs_malloc(sizeof(ucg_builtin_coll_params_t), "allocate var_len_params");
        if (step->send_coll_params == NULL) {
            return UCS_ERR_NO_MEMORY;
        }

        step->recv_coll_params =
            (ucg_builtin_coll_params_t *)ucs_malloc(sizeof(ucg_builtin_coll_params_t), "allocate var_len_params");
        if (step->recv_coll_params == NULL) {
            ucg_builtin_free((void **)&step->send_coll_params);
            return UCS_ERR_NO_MEMORY;
        }

        step->flags                     |= extra_flags;
        step->resend_flag               = UCG_BUILTIN_OP_STEP_FIRST_SEND;
        step->am_header.remote_offset   = 0;
        step->remote_offset             = step->am_header.remote_offset;
        step->send_cb                   = ucg_builtin_throttled_scatter_alltoallv_cb;

        return UCS_OK;
    }

    if (phase->ex_attr.is_plummer) {
        if (phase->ex_attr.is_variable_len == 0) {
            step->buf_len_unit = phase->ex_attr.member_cnt * sizeof(int);
            step->buffer_length = step->buf_len_unit;
            step->am_header.remote_offset = phase->ex_attr.packed_rank * step->buffer_length;
            /* The remote offset is an absolute value */
            step->remote_offset = step->am_header.remote_offset;
            if (phase->step_index == UCG_PLAN_PLUMMER_STEP_INTRA_GATHER_SEND_COUNTS) {
                step->send_buffer = (int8_t *)params->send.counts;
                step->send_cb = ucg_builtin_plummer_gather_send_counts_cb;
            } else {
                step->send_buffer = (int8_t *)params->recv.counts;
                step->send_cb = ucg_builtin_plummer_gather_recv_counts_cb;
            }
        } else {
            if (phase->send_ep_cnt > 0) {
                step->send_coll_params = ucg_builtin_allocate_coll_params(phase->ep_cnt);
                if (step->send_coll_params == NULL) {
                    return UCS_ERR_NO_MEMORY;
                }
            }
            if (phase->recv_ep_cnt > 0) {
                step->recv_coll_params = ucg_builtin_allocate_coll_params(phase->ep_cnt);
                if (step->recv_coll_params == NULL) {
                    if (step->send_coll_params) {
                        ucg_builtin_free_coll_params(&(step->send_coll_params));
                    }
                    return UCS_ERR_NO_MEMORY;
                }
            }
            step->flags |= extra_flags;
            step->resend_flag = UCG_BUILTIN_OP_STEP_FIRST_SEND;
            step->am_header.remote_offset = 0;
            step->remote_offset = step->am_header.remote_offset;

            int is_plummer_phase = 0;
#if ENABLE_UCG_HICOLL
            is_plummer_phase = (phase->method == UCG_PLAN_METHOD_ALLTOALLV_PLUMMER);
#endif
            if (phase->step_index == UCG_PLAN_PLUMMER_STEP_INTRA_GATHER_SEND_BUFFERS) {
                step->send_cb = ucg_builtin_plummer_gather_send_buffers_cb;
            } else if (is_plummer_phase) {
                step->send_cb = ucg_builtin_plummer_inter_alltoallv_cb;
            } else {
                step->send_cb = ucg_builtin_plummer_scatter_recv_buffers_cb;
            }
            return UCS_OK;
        }
    }

    /* special parameter of buffer length should be set for allgather with bruck plan */
    if (phase->method == UCG_PLAN_METHOD_ALLGATHER_BRUCK) {
        step->buf_len_unit = step->buffer_length;
        size_t special_offset = 1UL << phase->step_index;
        step->buffer_length *= (extra_flags == UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP) ?
                               (num_procs - special_offset) : special_offset;
    }

    /* for alltoall bruck, buffer_length should be changed! */
    if (phase->method == UCG_PLAN_METHOD_ALLTOALL_BRUCK) {
        step->displs_rule = UCG_BUILTIN_OP_STEP_DISPLS_RULE_BRUCK_ALLTOALL;
        unsigned i, k;
        size_t buffer_length_discrete = 0;
        if (step->displs_rule == UCG_BUILTIN_OP_STEP_DISPLS_RULE_BRUCK_ALLTOALL) {
            k = (unsigned)step->am_header.step_idx;
            for (i = 0; i < num_procs; i++) {
                if ((i >> k) & 1) { // kth bit is 1
                    buffer_length_discrete++;
                }
            }
        }

        step->buf_len_unit   = step->buffer_length;
        step->buffer_length *= buffer_length_discrete;
        /* set send cb for alltoall only, should be move to proper place */
        step->send_cb = ucg_builtin_send_alltoall;
    }

    if (phase->method == UCG_PLAN_METHOD_REDUCE_SCATTER_RING ||
        phase->method == UCG_PLAN_METHOD_ALLGATHER_RING) {
        int num_offset_blocks;
        int send_position;
        int recv_position;
        int quotient = params->send.count / num_procs;
        int remainder = params->send.count % num_procs;

        step->buf_len_unit   = step->buffer_length; // for ring init
        step->buffer_length = params->send.dt_len * quotient;
        num_offset_blocks = (g_myidx - phase->step_index + UCG_BUILTIN_NUM_PROCS_DOUBLE * num_procs) % num_procs;
        send_position = num_offset_blocks + 1;
        recv_position = (num_offset_blocks - 1 + num_procs) % num_procs + 1;

        step->buffer_length_recv = (recv_position <= remainder) ? step->buffer_length + params->send.dt_len :
                                   step->buffer_length;
        step->buffer_length += (send_position <= remainder) ? params->send.dt_len : 0;

        step->am_header.remote_offset = params->send.dt_len * (num_offset_blocks * quotient +
                               (num_offset_blocks <= remainder ? num_offset_blocks : remainder));

        step->remote_offset = step->am_header.remote_offset;
        step->send_buffer +=  step->am_header.remote_offset;
    }

    if (phase->method == UCG_PLAN_METHOD_ALLGATHER_RECURSIVE) {
        size_t power = 1UL << (phase->step_index - 1);
        size_t base_index;
        base_index = (g_myidx / power) * power;

        step->am_header.remote_offset = base_index * params->send.count * params->send.dt_len;
        /* need set the send offset if it's not the first step */
        if (!(extra_flags & UCG_BUILTIN_OP_STEP_FLAG_FIRST_STEP)) {
            step->send_buffer += step->am_header.remote_offset;
        }
        step->buffer_length *= power;
    }
    if (phase->ex_attr.is_partial) {
        if (builtin_plan->ucg_algo.binary_block == 1) {
            step->buffer_length             = phase->ex_attr.num_blocks * params->send.dt_len;
            step->buf_len_unit              = step->buffer_length;
            step->am_header.remote_offset   = phase->ex_attr.start_block * params->send.dt_len;
            step->send_buffer              += step->am_header.remote_offset;
            step->buffer_length_recv        = phase->ex_attr.peer_block * params->send.dt_len;
            step->remote_offset             = step->am_header.remote_offset;
        }
    }

    ucs_assert(base_am_id < UCP_AM_ID_MAX);

    /* Decide how the messages are sent (regardless of my role) */
    enum ucg_builtin_op_step_flags send_flag, recv_flag;
    recv_flag = (enum ucg_builtin_op_step_flags) 0;
    send_flag = (enum ucg_builtin_op_step_flags) 0;
    /* Note: in principle, step->send_buffer should not be changed after this function */
    status = ucg_builtin_step_send_flags(step, phase, params, send_dt_len, &send_flag);
    extra_flags |= (send_flag & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED);
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }

    /* Set the actual step-related parameters */
    switch (phase->method) {
        /* Send-only */
        case UCG_PLAN_METHOD_SCATTER_TERMINAL:
            extra_flags      |= UCG_BUILTIN_OP_STEP_FLAG_LENGTH_PER_REQUEST;
            /* no break */
        case UCG_PLAN_METHOD_SEND_TERMINAL:
            step->flags       = send_flag | extra_flags;
            break;

        /* Recv-only */
        case UCG_PLAN_METHOD_RECV_TERMINAL:
        case UCG_PLAN_METHOD_REDUCE_TERMINAL:
            extra_flags      |= UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND;
            step->flags       = extra_flags;
            break;

        /* Recv-all, Send-one */
        case UCG_PLAN_METHOD_GATHER_WAYPOINT:
            extra_flags      |= UCG_BUILTIN_OP_STEP_FLAG_LENGTH_PER_REQUEST;
            /* no break */
        case UCG_PLAN_METHOD_REDUCE_WAYPOINT:
            if (phase->ex_attr.is_partial) {
                step->buffer_length = params->send.dt_len * params->send.count;
            }
            extra_flags  = ((send_flag & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED) && builtin_plan->ucg_algo.pipeline) ?
                           (extra_flags | UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) : extra_flags;
            extra_flags |= UCG_BUILTIN_OP_STEP_FLAG_RECV_BEFORE_SEND1;
            step->flags  = send_flag | extra_flags;
            ucs_assert(params->type.modifiers != ucg_predefined_modifiers[UCG_PRIMITIVE_REDUCE]);
            step->send_buffer = step->recv_buffer;
            if (phase->ex_attr.is_partial) {
                unsigned block_length = params->send.count * params->send.dt_len / phase->ex_attr.total_num_blocks;
                step->buffer_length = phase->ex_attr.num_blocks * block_length;
                step->send_buffer += step->am_header.remote_offset;
            }
            break;

        /* Recv-one, Send-all */
        case UCG_PLAN_METHOD_BCAST_WAYPOINT:
            extra_flags  = ((send_flag & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED) && builtin_plan->ucg_algo.pipeline) ?
                           (extra_flags | UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) : extra_flags;
            extra_flags |= UCG_BUILTIN_OP_STEP_FLAG_RECV1_BEFORE_SEND;
            step->flags  = send_flag | extra_flags;
            break;

        case UCG_PLAN_METHOD_SCATTER_WAYPOINT:
            extra_flags  = ((send_flag & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED) &&builtin_plan->ucg_algo.pipeline) ?
                           (extra_flags | UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) : extra_flags;

            extra_flags |= UCG_BUILTIN_OP_STEP_FLAG_RECV1_BEFORE_SEND;
            extra_flags |= UCG_BUILTIN_OP_STEP_FLAG_LENGTH_PER_REQUEST;
            step->flags  = send_flag | extra_flags;
            if (*current_data_buffer == NULL) {
                *current_data_buffer = (int8_t *)ucs_calloc(1, step->buffer_length, "ucg_fanout_waypoint_buffer");
                if (*current_data_buffer == NULL) {
                    return UCS_ERR_NO_MEMORY;
                }
            }
            step->send_buffer = *current_data_buffer;
            step->recv_buffer = step->send_buffer;
            if (!step->recv_buffer) {
                return UCS_ERR_NO_MEMORY;
            }
            break;

        /* Recursive patterns */
        case UCG_PLAN_METHOD_REDUCE_RECURSIVE:
        case UCG_PLAN_METHOD_ALLGATHER_RECURSIVE:
        case UCG_PLAN_METHOD_REDUCE_SCATTER_RECURSIVE:
            extra_flags      |= UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND;
            step->flags       = send_flag | extra_flags;
            break;

        /* Bruck patterns for allgather */
        case UCG_PLAN_METHOD_ALLGATHER_BRUCK:
            extra_flags |= UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND;
            step->flags = send_flag | extra_flags;
            break;

        /* Bruck patterns for alltoall */
        case UCG_PLAN_METHOD_ALLTOALL_BRUCK:
            extra_flags |= UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND;
            step->flags = send_flag | extra_flags;
            // should malloc a new buffer to handle ucg_alltoall_step_buffer_discrete
            step->send_buffer = (int8_t*)params->send.buf;
            // bellow does not work
            /* max buffer size for alltoall at every step is num_procs/2 !!!! */
            break;

        case UCG_PLAN_METHOD_REDUCE_SCATTER_RING:
        case UCG_PLAN_METHOD_ALLGATHER_RING:
        case UCG_PLAN_METHOD_EXCHANGE:
            extra_flags |= UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND;
            step->flags = send_flag | extra_flags;
            break;
#if ENABLE_UCG_HICOLL
        case UCG_PLAN_METHOD_INC:
            extra_flags |= UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND;
            step->flags = send_flag | extra_flags;
            step->send_cb = ucg_builtin_send_inc;
            break;
#endif

        default:
            ucs_error("Invalid method for a collective operation.");
            return UCS_ERR_INVALID_PARAM;
    }

    if (send_flag & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY) {
        if (phase->method != UCG_PLAN_METHOD_RECV_TERMINAL &&
            phase->method != UCG_PLAN_METHOD_REDUCE_TERMINAL ) {
                /* memory registration (using the memory registration cache)*/
                status = ucg_builtin_step_zcopy_prep(step);
                if (ucs_unlikely(status != UCS_OK)) {
                    ucs_error("Failed to register the buffer in zcopy");
                    return status;
                }
            }
    }


    status = ucg_builtin_step_recv_flags(step, phase, params, recv_dt_len, &recv_flag);
    if (status != UCS_OK) {
        return status;
    }

    /* fill in additional data before finishing this step */
    step->flags = (phase->ep_cnt == 1) ?
                  (step->flags | UCG_BUILTIN_OP_STEP_FLAG_SINGLE_ENDPOINT) : step->flags;

    if (step->flags & send_flag) {
        if (phase->method != UCG_PLAN_METHOD_ALLGATHER_RECURSIVE &&
            phase->method != UCG_PLAN_METHOD_REDUCE_SCATTER_RING &&
            phase->method != UCG_PLAN_METHOD_ALLGATHER_RING &&
            !phase->ex_attr.is_partial) {
            step->am_header.remote_offset = 0;
        }
    }
    /* create allreduce buffer */
    if ((phase->method == UCG_PLAN_METHOD_REDUCE_TERMINAL || phase->method == UCG_PLAN_METHOD_REDUCE_WAYPOINT)
        && !(send_flag & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED) && g_reduce_coinsidency
        && (extra_flags & UCG_BUILTIN_OP_STEP_FLAG_FIRST_STEP)) {
            step->rbuf_count = step->fragments_recv
            * (phase->ep_cnt - ((phase->method == UCG_PLAN_METHOD_REDUCE_TERMINAL) ? 0 : 1));
            step->reduce_buff = (void *)UCS_ALLOC_CHECK(step->buffer_length * step->rbuf_count, "reduce buffer for child");
            ucs_debug("rb count:%d, ep count:%u", step->rbuf_count, phase->ep_cnt);
    }
    if (!(send_flag & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED)
         && ((phase->method == UCG_PLAN_METHOD_REDUCE_WAYPOINT) || (phase->method == UCG_PLAN_METHOD_SEND_TERMINAL))
         && g_reduce_coinsidency) {
        unsigned is_allreduce = (ucg_builtin_get_coll_type(&params->type) == COLL_TYPE_ALLREDUCE);
        if (is_allreduce) {
            ucs_debug("my position%d", g_myposition);
            step->am_header.remote_offset = g_myposition;
        }
    }


    /* Pipelining preparation */
    if ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) && builtin_plan->ucg_algo.pipeline) {
        step->fragment_pending = (uint8_t*)UCS_ALLOC_CHECK(step->fragments *
                sizeof(uint8_t*), "ucg_builtin_step_pipelining");
    }

    if (phase->method != UCG_PLAN_METHOD_ALLGATHER_BRUCK &&
        phase->method != UCG_PLAN_METHOD_ALLTOALL_BRUCK &&
        phase->method != UCG_PLAN_METHOD_REDUCE_SCATTER_RING &&
        phase->method != UCG_PLAN_METHOD_ALLGATHER_RING &&
        !phase->ex_attr.is_inequal) {
        recv_flag = (enum ucg_builtin_op_step_flags)step->flags;
        step->fragments_recv = step->fragments;
    }

    if (phase->segmented) {
        phase->recv_cache_buffer = (int8_t *)UCS_ALLOC_CHECK(params->send.count * send_dt_len, "recv_cache_buffer");
        ucs_debug("segmented phase %p fragments %" PRIu32 "", phase, step->fragments_recv);
    } else {
        phase->recv_cache_buffer = NULL;
    }

    ucg_builtin_step_set_contig(step, (is_send_contig || is_recv_contig));

    /* Select the right completion callback */
    return ucg_builtin_step_select_callbacks(phase, is_recv_contig, &step->recv_cb,
                                             params->send.count > 0, recv_flag);
}

static inline ucs_status_t ucg_builtin_convert_datatype(ucg_builtin_plan_t *builtin_plan,
                                               void *param_datatype,
                                               ucp_datatype_t *ucp_datatype)
{
    int ret = builtin_plan->convert_f(param_datatype, ucp_datatype);
    if (ucs_unlikely(ret != 0)) {
        ucs_error("Datatype conversion callback failed");
        return UCS_ERR_INVALID_PARAM;
    }

    return UCS_OK;
}

void ucg_builtin_swap_net_recv(char *netdata, size_t length, size_t offset,
                               ucg_builtin_request_t *req)
{
    ucg_builtin_op_step_t *step = req->step;
    ucp_dt_generic_t *gen_dt = req->op->recv_dt;
    void *state_pack = step->non_contig.pack_state_recv;
    void *state_unpack = step->non_contig.unpack_state;
    char *recv_buffer = (char *)step->recv_buffer;
    char *tmp_buffer = NULL;

    ucs_debug("swap netdata:%p length:%lu and recv_buffer:%p offset:%lu",
              netdata, length, recv_buffer, offset);

    if (length == 0) {
        return;
    }

    tmp_buffer = (char *)ucs_malloc(length, "temp swap buffer");
    if (tmp_buffer == NULL) {
        ucs_fatal("no memory for malloc, length:%lu", length);
    }

    memcpy(tmp_buffer, netdata, length);
    if (gen_dt != NULL) {
        if (step->recv_cb == ucg_builtin_comp_reduce_full_cb) {
            ucs_debug("large non-contiguous datatype can not swap here");
        } else {
            gen_dt->ops.pack(state_pack, offset, netdata, length);
            gen_dt->ops.unpack(state_unpack, offset, tmp_buffer, length);
        }
    } else {
        memcpy(netdata, recv_buffer + offset, length);
        memcpy(recv_buffer + offset, tmp_buffer, length);
    }

    free(tmp_buffer);
}

ucs_status_t ucg_builtin_op_create(ucg_plan_t *plan,
                                   const ucg_collective_params_t *params,
                                   ucg_op_t **new_op)
{
    ucs_status_t status;
    ucp_datatype_t send_dtype = 0;
    ucp_datatype_t recv_dtype = 0;
    ucg_builtin_plan_t *builtin_plan     = (ucg_builtin_plan_t*)plan;
    ucg_builtin_plan_phase_t *next_phase = &builtin_plan->phss[0];
    unsigned phase_count                 = builtin_plan->phs_cnt;

    ucg_builtin_op_t *op                 = (ucg_builtin_op_t*)
            ucs_mpool_get_inline(&builtin_plan->op_mp);
    ucg_builtin_group_ctx_t *builtin_ctx = UCG_GROUP_TO_COMPONENT_CTX(ucg_builtin_component, plan->group);
    if (op == NULL) {
        return UCS_ERR_NO_MEMORY;
    }

    ucg_builtin_op_step_t *next_step     = &op->steps[0];
    unsigned am_id                       = builtin_plan->am_id;
    op->super.plan = plan;

    /* obtain UCX datatypes corresponding to the extenral datatypes passed */
    op->dtspan_f = builtin_plan->dtspan_f;
    op->send_dt = NULL;
    op->recv_dt = NULL;
    op->temp_data_buffer = NULL;
    op->temp_data_buffer1 = NULL;
    op->temp_exchange_buffer = NULL;
    op->temp_exchange_buffer1 = NULL;
    if (params->send.count > 0 && params->send.dt_len > 0) {
        status = ucg_builtin_convert_datatype(builtin_plan, params->send.dt_ext, &send_dtype);
        if (ucs_unlikely(status != UCS_OK)) {
            return status;
        }
        op->send_dt = (!UCG_DT_IS_CONTIG(params, send_dtype)) ? ucp_dt_generic(send_dtype) :
                      op->send_dt;
    }

    if (params->recv.count > 0 && params->recv.dt_len > 0) {
        status = ucg_builtin_convert_datatype(builtin_plan, params->recv.dt_ext, &recv_dtype);
        if (ucs_unlikely(status != UCS_OK)) {
            return status;
        }
        op->recv_dt = (!UCG_DT_IS_CONTIG(params, recv_dtype)) ? ucp_dt_generic(recv_dtype) :
                      op->recv_dt;
    }

    /* get number of processes */
    num_procs = (unsigned)(ucg_group_get_params(plan->group))->member_count;
    g_myidx = plan->my_index;
    g_myposition = plan->up_offset;
    g_reduce_coinsidency = ucg_is_allreduce_consistency(builtin_ctx);
    ucs_debug("ucg rank: %" PRIu64 " phase cnt %u", g_myidx, phase_count);
    /* Select the right initialization callback */
    status = ucg_builtin_op_select_callback(builtin_plan,
                                            UCG_DT_IS_CONTIG(params, send_dtype),
                                            UCG_DT_IS_CONTIG(params, recv_dtype),
                                            &op->init_cb, &op->final_cb);
    if (status != UCS_OK) {
        goto op_cleanup;
    }

    /* Create a step in the op for each phase in the topology */
    if (phase_count == 1) {
        /* The only step in the plan */
        status = ucg_builtin_step_create(op, next_phase, send_dtype, recv_dtype,
                                         UCG_BUILTIN_OP_STEP_FLAG_FIRST_STEP | UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP,
                                         am_id, plan->group_id, params,
                                         &(op->temp_data_buffer), next_step);
    } else {
        /* First step of many */
        status = ucg_builtin_step_create(op, next_phase, send_dtype, recv_dtype,
                                         UCG_BUILTIN_OP_STEP_FLAG_FIRST_STEP, am_id, plan->group_id,
                                         params, &(op->temp_data_buffer), next_step);
        if (ucs_unlikely(status != UCS_OK)) {
            goto op_cleanup;
        }

        ucg_step_idx_ext_t step_cnt;
        for (step_cnt = 1; step_cnt < phase_count - 1; step_cnt++) {
            status = ucg_builtin_step_create(op, ++next_phase, send_dtype, recv_dtype, 0, am_id,
                                             plan->group_id, params, &(op->temp_data_buffer), ++next_step);
            if (ucs_unlikely(status != UCS_OK)) {
                goto op_cleanup;
            }
        }

        /* Last step gets a special flag */
        status = ucg_builtin_step_create(op, ++next_phase, send_dtype, recv_dtype,
                                         UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP, am_id, plan->group_id,
                                         params, &(op->temp_data_buffer), ++next_step);
    }
    if (ucs_unlikely(status != UCS_OK)) {
        goto op_cleanup;
    }

    /* Select the right optimization callback */
    status = ucg_builtin_op_consider_optimization(op, (ucg_builtin_config_t*)plan->planner->plan_config);
    if (status != UCS_OK) {
        goto op_cleanup;
    }

    UCS_STATIC_ASSERT(sizeof(ucg_builtin_header_t) <= UCP_WORKER_HEADROOM_PRIV_SIZE);
    UCS_STATIC_ASSERT(sizeof(ucg_builtin_header_t) == sizeof(uint64_t));

    op->slots  = (ucg_builtin_comp_slot_t*)builtin_plan->slots;
    op->resend = builtin_plan->resend;
    *new_op    = &op->super;
    return UCS_OK;

op_cleanup:
    ucs_mpool_put_inline(op);
    return status;
}
