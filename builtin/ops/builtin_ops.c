/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <string.h>

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

/******************************************************************************
 *                                                                            *
 *                            Operation Execution                             *
 *                                                                            *
 ******************************************************************************/
void ucg_builtin_step_assert(ucg_builtin_op_step_t *step, enum ucg_builtin_op_step_flags step_flag)
{
    ucs_assert((step->flags & (UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT |
                               UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY |
                               UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY)) ==
                               step_flag);
    ucs_assert(step->iter_offset != UCG_BUILTIN_OFFSET_PIPELINE_READY);
    ucs_assert(step->iter_offset != UCG_BUILTIN_OFFSET_PIPELINE_PENDING);
}

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_step_dummy_send(ucg_builtin_request_t *req,
                                                                    ucg_builtin_op_step_t *step,
                                                                    uct_ep_h ep, int is_single_send)
{
    ucg_builtin_step_assert(step, 0);
    return UCS_OK;
}

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_step_am_short_one(ucg_builtin_request_t *req,
                                                                      ucg_builtin_op_step_t *step,
                                                                      uct_ep_h ep, int is_single_send)
{
    ucg_builtin_step_assert(step, UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT);
    ucs_debug("am_short_one step %u length %zu", step->am_header.step_idx, step->buffer_length);

    int8_t *send_buffer          = step->send_buffer;
    void *dt_state               = step->non_contig.pack_state;
    if (dt_state != NULL) {
        req->op->send_dt->ops.pack(dt_state, 0, step->non_contig.contig_buffer, step->buffer_length);
        send_buffer              = step->non_contig.contig_buffer;
    }
    return step->uct_iface->ops.ep_am_short(ep, step->am_id,
                                            step->am_header.header, send_buffer, step->buffer_length);
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

    ucs_status_t (*ep_am_short)(uct_ep_h, uint8_t, uint64_t, const void*, unsigned) =
            step->uct_iface->ops.ep_am_short;
    ucg_builtin_step_assert(step, UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT);

    /* send every fragment but the last */
    if (ucs_likely(buffer_iter < buffer_iter_limit)) {
        do {
            ucs_debug("am_short_max step %u offset %" PRIu32 " length %u", step->am_header.step_idx, am_iter.remote_offset, frag_size);
            status = ep_am_short(ep, am_id, am_iter.header, buffer_iter, frag_size);

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
    status = ep_am_short(ep, am_id, am_iter.header, buffer_iter, send_buffer + step->buffer_length - buffer_iter);
    /* iter_offset can not set to be zero for pipelining */
    if (!is_single_send) {
        step->iter_offset = (status == UCS_OK) ? 0 : buffer_iter - send_buffer;
    }

    return status;
}

static size_t ucg_builtin_step_am_bcopy_single_frag_packer(void *dest, void *arg)
{
    ucg_builtin_request_t *req       = (ucg_builtin_request_t*)arg;
    ucg_builtin_op_step_t *step      = req->step;
    ucg_builtin_header_t *header_ptr = (ucg_builtin_header_t*)dest;
    void *dt_state                   = step->non_contig.pack_state;
    header_ptr->header               = step->am_header.header;

    if (dt_state != NULL) {
        req->op->send_dt->ops.pack(dt_state, 0, header_ptr + 1, step->buffer_length);
    } else {
        memcpy(header_ptr + 1, step->send_buffer, step->buffer_length);
    }
    return sizeof(*header_ptr) + step->buffer_length;
}

static size_t ucg_builtin_step_am_bcopy_full_frag_packer(void *dest, void *arg)
{
    ucg_builtin_request_t *req       = (ucg_builtin_request_t*)arg;
    ucg_builtin_op_step_t *step      = req->step;
    ucg_builtin_header_t *header_ptr = (ucg_builtin_header_t*)dest;
    void *dt_state                   = step->non_contig.pack_state;
    header_ptr->header               = step->am_header.header;

    if (dt_state != NULL) {
        req->op->send_dt->ops.pack(dt_state, step->iter_offset, header_ptr + 1, step->fragment_length);
    } else {
        memcpy(header_ptr + 1, step->send_buffer + step->iter_offset, step->fragment_length);
    }
    return sizeof(*header_ptr) + step->fragment_length;
}

static size_t ucg_builtin_step_am_bcopy_partial_frag_packer(void *dest, void *arg)
{
    ucg_builtin_request_t *req       = (ucg_builtin_request_t*)arg;
    ucg_builtin_op_step_t *step      = req->step;
    ucg_offset_t last_frag_length    = step->buffer_length - step->iter_offset;
    ucg_builtin_header_t *header_ptr = (ucg_builtin_header_t*)dest;
    void *dt_state                   = step->non_contig.pack_state;
    header_ptr->header               = step->am_header.header;

    if (dt_state != NULL) {
        req->op->send_dt->ops.pack(dt_state, step->iter_offset, header_ptr + 1, last_frag_length);
    } else {
        memcpy(header_ptr + 1, step->send_buffer + step->iter_offset, last_frag_length);
    }
    return sizeof(*header_ptr) + last_frag_length;
}

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_step_am_bcopy_one(ucg_builtin_request_t *req,
                                                                      ucg_builtin_op_step_t *step,
                                                                      uct_ep_h ep, int is_single_send)
{
    ucg_builtin_step_assert(step, UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY);

    /* send active message to remote endpoint */
    ucs_debug("am_bcopy_one step %u length %zu", step->am_header.step_idx, step->buffer_length);
    ssize_t len = step->uct_iface->ops.ep_am_bcopy(ep, step->am_id,
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

    ssize_t (*ep_am_bcopy)(uct_ep_h, uint8_t, uct_pack_callback_t, void*, unsigned) =
            step->uct_iface->ops.ep_am_bcopy;

    ucg_builtin_step_assert(step, UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY);

    /* check if this is not, by any chance, the last fragment */
    if (ucs_likely(step->iter_offset < iter_limit)) {
        /* send every fragment but the last */
        do {
            ucs_debug("am_bcopy_max step %u offset %" PRIu32 " length %u", step->am_header.step_idx, step->am_header.remote_offset, frag_size);
            len = ep_am_bcopy(ep, am_id, ucg_builtin_step_am_bcopy_full_frag_packer, req, 0);

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
    len = ep_am_bcopy(ep, am_id, ucg_builtin_step_am_bcopy_partial_frag_packer, req, 0);
    if (ucs_unlikely(len < 0)) {
        return (ucs_status_t)len;
    }

    step->am_header.remote_offset = 0;
    /* iter_offset can not set to be zero for pipelining */
    step->iter_offset = (!is_single_send) ? 0 : step->iter_offset;

    return UCS_OK;
}

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_step_am_zcopy_one(ucg_builtin_request_t *req,
                                                                      ucg_builtin_op_step_t *step,
                                                                      uct_ep_h ep, int is_single_send)
{
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
    ucs_status_t status = step->uct_iface->ops.ep_am_zcopy(ep, step->am_id,
                                                           &step->am_header, sizeof(step->am_header),
                                                           &iov, 1, 0, &zcomp->comp);
    return ucs_unlikely(status != UCS_INPROGRESS) ? status : UCS_OK;
}

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_step_am_zcopy_max(ucg_builtin_request_t *req,
                                                                      ucg_builtin_op_step_t *step,
                                                                      uct_ep_h ep, int is_single_send)
{
    ucs_status_t status;
    unsigned am_id                = step->am_id;
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
    unsigned zcomp_index        = step->iter_ep * step->fragments +
                                  step->iter_offset / step->fragment_length;
    ucg_builtin_zcomp_t *zcomp  = &step->zcopy.zcomp[zcomp_index];
    ucs_status_t (*ep_am_zcopy)(uct_ep_h, uint8_t, const void*, unsigned,
            const uct_iov_t*, size_t, unsigned, uct_completion_t*) =
                    step->uct_iface->ops.ep_am_zcopy;

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
            ucs_debug("am_zcopy_max step %u offset %" PRIu32 " length %u", step->am_header.step_idx, step->am_header.remote_offset, frag_size);
            status = ep_am_zcopy(ep, am_id, &step->am_header,
                                 sizeof(step->am_header), &iov,
                                 1, 0, &zcomp->comp);
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
    ucs_debug("am_zcopy_max step %u offset %" PRIu32 " length %zu", step->am_header.step_idx, step->am_header.remote_offset, iov.length);
    status     = ep_am_zcopy(ep, am_id, &step->am_header,
                             sizeof(step->am_header),
                             &iov, 1, 0, &zcomp->comp);
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

/*
 * Below is a set of macros, generating most bit-field combinations of
 * step->flags inside @ref ucg_builtin_step_execute() .
 */
#define case_send(req, ureq, step, phase, _send_func) {                          \
        if ((is_rs1 || is_r1s) && ((step)->iter_ep == 0)) {                      \
            uint32_t new_cnt = (step)->iter_ep = is_r1s ? 1 : (phase)->ep_cnt - 1; \
            ucs_assert(new_cnt > 0);                                             \
            if (is_pipelined) {                                                  \
                memset((void*)(step)->fragment_pending, new_cnt, (step)->fragments); \
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
    }                                                                            \

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
 * sends and then recieves according to the instructions of this step.
 * The function returns the status, typically one of the following:
 * > UCS_OK - collective operation (not just this step) has been completed.
 * > UCS_INPROGRESS - sends complete, waiting on some messages to be recieved.
 * > otherwise - an error has occurred.
 *
 * For example, a "complex" case is when the message is fragmented, and requires
 * both recieveing and sending in a single step, like in REDUCE_WAYPOINT. The
 * first call, coming from @ref ucg_builtin_op_trigger() , will enter the first
 * branch ("step_ep" is zero when a new step is starting), will process some
 * potential incoming messages (arriving beforehand) - returning UCS_INPROGRESS.
 * Subsequent calls to "progress()" will handle the rest of the incoming
 * messages for this step, and eventually call this function again from within
 * @ref ucg_builtin_comp_step_cb() . This call will choose the second branch,
 * the swith-case, which will send the message and
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
    if (req->step->send_cb != NULL) {
        req->step->send_cb(req);
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

    /* for recv-only step */
    if (is_dummy) case_send(req, user_req, step, phase, ucg_builtin_step_dummy_send);

    if (!is_fragmented) { /* Single-send operations (only one fragment passed to UCT) */
        if (is_short) {
            case_send(req, user_req, step, phase, ucg_builtin_step_am_short_one);
        } else if (is_bcopy) {
            case_send(req, user_req, step, phase, ucg_builtin_step_am_bcopy_one);
        } else if (is_zcopy) {
            case_send(req, user_req, step, phase, ucg_builtin_step_am_zcopy_one);
        }
    } else { /* Multi-send operations (using iter_ep and iter_offset for context) */
        if (is_short) {
            case_send(req, user_req, step, phase, ucg_builtin_step_am_short_max);
        } else if (is_bcopy) {
            case_send(req, user_req, step, phase, ucg_builtin_step_am_bcopy_max);
        } else if (is_zcopy) {
            case_send(req, user_req, step, phase, ucg_builtin_step_am_zcopy_max);
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
        ucg_builtin_comp_desc_t *desc = NULL;
        ucg_builtin_comp_desc_t *iter = NULL;
        ucs_list_for_each_safe(desc, iter, &slot->msg_head, super.tag_list[0]) {
            if (ucs_likely(desc->header.local_id == local_id)) {
                /* The number of store will not bigger than recv fragments */
                if (++step->zcopy.num_store >= step->fragments_recv) {
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

void ucg_builtin_dispose_packet(ucg_builtin_comp_desc_t *desc)
{
    /* Dispose of the packet, according to its allocation */
    if (desc->super.flags == UCT_CB_PARAM_FLAG_DESC) {
        uct_iface_release_desc(desc);
    } else {
        ucs_mpool_put_inline(desc);
    }
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
            ucg_builtin_dispose_packet(desc);

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
    if (step->non_contig.contig_buffer != NULL) {
        ucs_free(step->non_contig.contig_buffer);
        step->non_contig.contig_buffer = NULL;
    }
}

void ucg_builtin_op_discard(ucg_op_t *op)
{
    ucg_builtin_op_t *builtin_op = (ucg_builtin_op_t*)op;
    ucg_builtin_op_step_t *step = &builtin_op->steps[0];
    do {
        if (step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY) {
            uct_md_mem_dereg(step->uct_md, step->zcopy.memh);
            if (step->zcopy.zcomp != NULL) {
                ucs_free(step->zcopy.zcomp);
                step->zcopy.zcomp = NULL;
            }
        }

        if (step->flags & UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) {
            if (step->zcopy.zcomp != NULL) {
                ucs_free((void*)step->fragment_pending);
                step->fragment_pending = NULL;
            }
        }

        ucg_builtin_step_release_contig(step);
    } while (!((step++)->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP));

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
        builtin_op->init_cb(builtin_op);
    }

    /* Consider optimization, if this operation is used often enough */
    if (ucs_unlikely(--builtin_op->opt_cnt == 0)) {
        ucs_status_t optm_status = builtin_op->optm_cb(builtin_op);
        if (ucs_unlikely(UCS_STATUS_IS_ERR(optm_status))) {
            return optm_status;
        }
        /* Need to return original status, becuase it can be OK or INPROGRESS */
    }

    /* Start the first step, which may actually complete the entire operation */
    return ucg_builtin_step_execute(builtin_req, request);
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
    size_t length = step->buffer_length;
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

        if (phase->method != UCG_PLAN_METHOD_RECV_TERMINAL && phase->method != UCG_PLAN_METHOD_REDUCE_TERMINAL) {
            /* memory registration (using the memory registration cache) */
            ucs_status_t status = ucg_builtin_step_zcopy_prep(step);
            if (ucs_unlikely(status != UCS_OK)) {
                return status;
            }
        } else {
            /* recv only method */
            return UCS_OK;
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
    size_t length = step->buffer_length;
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

ucs_status_t ucg_builtin_step_create(ucg_builtin_plan_phase_t *phase,
                                     ucp_datatype_t send_dtype,
                                     ucp_datatype_t recv_dtype,
                                     unsigned extra_flags,
                                     unsigned base_am_id,
                                     ucg_group_id_t group_id,
                                     const ucg_collective_params_t *params,
                                     int8_t **current_data_buffer,
                                     ucg_builtin_op_step_t *step)
{
    /* Set the parameters determining the send-flags later on */
    int is_send_contig       = UCG_DT_IS_CONTIG(params, send_dtype);
    int is_recv_contig       = UCG_DT_IS_CONTIG(params, recv_dtype);
    size_t send_dt_len       = is_send_contig ? params->send.dt_len :
                               ucg_builtin_get_dt_len(ucp_dt_generic(send_dtype));
    size_t recv_dt_len       = is_recv_contig ? params->recv.dt_len :
                               ucg_builtin_get_dt_len(ucp_dt_generic(recv_dtype));
    step->buffer_length      = send_dt_len * params->send.count;
    step->uct_md             = phase->md;
    if (phase->md) {
        step->uct_iface      = (phase->ep_cnt == 1) ? phase->single_ep->iface :
                                                      phase->multi_eps[0]->iface;
    }
    /* Note: we assume all the UCT endpoints have the same interface */
    step->phase              = phase;
    step->am_id              = base_am_id;
    step->am_header.group_id = group_id;
    step->am_header.step_idx = (ucg_step_idx_t)phase->step_index;
    step->iter_ep            = 0;
    step->iter_offset        = 0;
    step->fragment_pending   = NULL;
    step->recv_buffer        = (int8_t*)params->recv.buf;
    step->send_buffer        = ((params->send.buf == MPI_IN_PLACE) ||
            !(extra_flags & UCG_BUILTIN_OP_STEP_FLAG_FIRST_STEP)) ?
                    (int8_t*)params->recv.buf : (int8_t*)params->send.buf;
    step->send_cb            = NULL;

    step->non_contig.contig_buffer = NULL;
    step->non_contig.pack_state   = NULL;
    step->non_contig.unpack_state = NULL;
    step->non_contig.pack_state_recv = NULL;

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

    if (phase->method != UCG_PLAN_METHOD_BCAST_WAYPOINT) {
        if (*current_data_buffer) {
            step->send_buffer = *current_data_buffer;
        } else {
            *current_data_buffer = step->recv_buffer;
        }
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
        size_t base_index = 0;
        base_index = (g_myidx / power) * power;

        step->am_header.remote_offset = base_index * params->send.count * params->send.dt_len;
        /* need set the send offset if it's not the first step */
        if (!(extra_flags & UCG_BUILTIN_OP_STEP_FLAG_FIRST_STEP)) {
            step->send_buffer += step->am_header.remote_offset;
        }
        step->buffer_length *= power;
    }
    ucs_assert(base_am_id < UCP_AM_ID_MAX);

    /* Decide how the messages are sent (regardless of my role) */
    enum ucg_builtin_op_step_flags send_flag, recv_flag;
    recv_flag = (enum ucg_builtin_op_step_flags) 0;
    send_flag = (enum ucg_builtin_op_step_flags) 0;
    /* Note: in principle, step->send_buffer should not be changed after this function */
    ucs_status_t status = ucg_builtin_step_send_flags(step, phase, params, send_dt_len, &send_flag);
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
            extra_flags  = ((send_flag & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED) && ucg_algo.pipeline) ?
                           (extra_flags | UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) : extra_flags;
            extra_flags |= UCG_BUILTIN_OP_STEP_FLAG_RECV_BEFORE_SEND1;
            step->flags  = send_flag | extra_flags;
            *current_data_buffer = (int8_t*)ucs_calloc(1, step->buffer_length, "ucg_fanin_waypoint_buffer");
            if (*current_data_buffer == NULL) {
                return UCS_ERR_NO_MEMORY;
            }
            step->send_buffer = *current_data_buffer;
            step->recv_buffer = step->send_buffer;

            if (params->send.buf == MPI_IN_PLACE) {
                memcpy(step->send_buffer, params->recv.buf, step->buffer_length);
            } else {
                memcpy(step->send_buffer, params->send.buf, step->buffer_length);
            }

            if (send_flag & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY) {
                /* The send buffer changed, reregister it */
                uct_md_mem_dereg(step->uct_md, step->zcopy.memh);
                status = uct_md_mem_reg(step->uct_md, step->send_buffer,
                                        step->buffer_length, UCT_MD_MEM_ACCESS_ALL, &step->zcopy.memh);
                if (status != UCS_OK) {
                    if (step->zcopy.zcomp != NULL) {
                        ucs_free(step->zcopy.zcomp);
                        step->zcopy.zcomp = NULL;
                    }
                    return status;
                }
            }

            if (!step->recv_buffer) {
                return UCS_ERR_NO_MEMORY;
            }
            break;

        /* Recv-one, Send-all */
        case UCG_PLAN_METHOD_BCAST_WAYPOINT:
            extra_flags  = ((send_flag & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED) && ucg_algo.pipeline) ?
                           (extra_flags | UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) : extra_flags;
            extra_flags |= UCG_BUILTIN_OP_STEP_FLAG_RECV1_BEFORE_SEND;
            step->flags  = send_flag | extra_flags;
            break;

        case UCG_PLAN_METHOD_SCATTER_WAYPOINT:
            extra_flags  = ((send_flag & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED) && ucg_algo.pipeline) ?
                           (extra_flags | UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) : extra_flags;

            extra_flags |= UCG_BUILTIN_OP_STEP_FLAG_RECV1_BEFORE_SEND;
            extra_flags |= UCG_BUILTIN_OP_STEP_FLAG_LENGTH_PER_REQUEST;
            step->flags  = send_flag | extra_flags;
            *current_data_buffer = (int8_t*)ucs_calloc(1, step->buffer_length, "ucg_fanout_waypoint_buffer");
            if (*current_data_buffer == NULL) {
                return UCS_ERR_NO_MEMORY;
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
            extra_flags |= UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND;
            step->flags = send_flag | extra_flags;
            break;

        default:
            ucs_error("Invalid method for a collective operation.");
            return UCS_ERR_INVALID_PARAM;
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
            phase->method != UCG_PLAN_METHOD_ALLGATHER_RING) {
            step->am_header.remote_offset = 0;
        }
    }

    /* Pipelining preparation */
    if ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) && ucg_algo.pipeline) {
        step->fragment_pending = (uint8_t*)UCS_ALLOC_CHECK(step->fragments *
                sizeof(uint8_t*), "ucg_builtin_step_pipelining");
    }

    if (phase->method != UCG_PLAN_METHOD_ALLGATHER_BRUCK &&
        phase->method != UCG_PLAN_METHOD_ALLTOALL_BRUCK &&
        phase->method != UCG_PLAN_METHOD_REDUCE_SCATTER_RING &&
        phase->method != UCG_PLAN_METHOD_ALLGATHER_RING) {
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

static inline int ucg_builtin_convert_datatype(ucg_builtin_plan_t *builtin_plan,
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
    if (op == NULL) {
        return UCS_ERR_NO_MEMORY;
    }

    ucg_builtin_op_step_t *next_step     = &op->steps[0];
    unsigned am_id                       = builtin_plan->am_id;
    int8_t *current_data_buffer          = NULL;

    /* obtain UCX datatypes corresponding to the extenral datatypes passed */
    op->dtspan_f = builtin_plan->dtspan_f;
    op->send_dt = NULL;
    op->recv_dt = NULL;
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
    ucs_debug("ucg rank: %" PRIu64 " phase cnt %u", g_myidx, phase_count);
    /* Select the right initialization callback */
    status = ucg_builtin_op_select_callback(builtin_plan, UCG_DT_IS_CONTIG(params, send_dtype), UCG_DT_IS_CONTIG(params, recv_dtype), &op->init_cb, &op->final_cb);
    if (status != UCS_OK) {
        goto op_cleanup;
    }

    /* Create a step in the op for each phase in the topology */
    if (phase_count == 1) {
        /* The only step in the plan */
        status = ucg_builtin_step_create(next_phase, send_dtype, recv_dtype,
                                         UCG_BUILTIN_OP_STEP_FLAG_FIRST_STEP | UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP,
                                         am_id, plan->group_id, params,
                                         &current_data_buffer, next_step);
    } else {
        /* First step of many */
        status = ucg_builtin_step_create(next_phase, send_dtype, recv_dtype,
                                         UCG_BUILTIN_OP_STEP_FLAG_FIRST_STEP, am_id, plan->group_id,
                                         params, &current_data_buffer, next_step);
        if (ucs_unlikely(status != UCS_OK)) {
            goto op_cleanup;
        }

        ucg_step_idx_ext_t step_cnt;
        for (step_cnt = 1; step_cnt < phase_count - 1; step_cnt++) {
            status = ucg_builtin_step_create(++next_phase, send_dtype, recv_dtype, 0, am_id,
                                             plan->group_id, params, &current_data_buffer, ++next_step);
            if (ucs_unlikely(status != UCS_OK)) {
                goto op_cleanup;
            }
        }

        /* Last step gets a special flag */
        status = ucg_builtin_step_create(++next_phase, send_dtype, recv_dtype,
                                         UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP, am_id, plan->group_id,
                                         params, &current_data_buffer, ++next_step);
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
