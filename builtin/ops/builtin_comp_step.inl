/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <ucp/core/ucp_request.inl> /* For @ref ucp_recv_desc_release */

/******************************************************************************
 *                                                                            *
 *                         Operation Step Completion                          *
 *                                                                            *
 ******************************************************************************/

static void UCS_F_ALWAYS_INLINE
ucg_builtin_comp_last_step_cb(ucg_builtin_request_t *req, ucs_status_t status)
{
    ucg_builtin_op_t *op = req->op;

    if (ucs_unlikely(op->flags & UCG_BUILTIN_OP_FLAG_FINALIZE_MASK)) {
        ucg_builtin_op_finalize_by_flags(op);
    }

    /* Mark (per-group) slot as available */
    req->expecting.local_id = 0;

    op->super.compreq_f(req->comp_req, status);
    ucs_assert(status != UCS_INPROGRESS);

    UCS_PROFILE_REQUEST_EVENT(req->comp_req, "complete_coll", 0);
    ucs_trace_req("collective returning completed request=%p (status: %s)",
                  req->comp_req, ucs_status_string(status));
}

ucs_status_t static UCS_F_ALWAYS_INLINE
ucg_builtin_comp_ft_end_step(ucg_builtin_op_step_t *step)
{
#if ENABLE_FAULT_TOLERANCE
    if (ucs_unlikely(step->op->flags & UCG_BUILTIN_OP_STEP_FLAG_FT_ONGOING))) {
        ucg_builtin_plan_phase_t *phase = step->phase;
        if (phase->ep_cnt == 1) {
            ucg_ft_end(phase->handles[0], phase->indexes[0]);
        } else {
            /* Removing in reverse order is good for FT's timerq performance */
            unsigned peer_idx = phase->ep_cnt;
            while (peer_idx--) {
                ucg_ft_end(phase->handles[peer_idx], phase->indexes[peer_idx]);
            }
        }
    }
#endif
    return UCS_OK;
}

ucs_status_t static UCS_F_ALWAYS_INLINE
ucg_builtin_comp_step_cb(ucg_builtin_request_t *req)
{
    /* Sanity checks */
    ucs_assert((req->step->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP) == 0);
    if (req->step->flags & UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) {
        unsigned frag_idx;
        unsigned frag_per_ep = req->step->fragments_total / req->step->ep_cnt;
        ucs_assert(req->step->fragments_total % req->step->ep_cnt == 0);
        ucs_assert(req->step->fragment_pending != NULL);
        for (frag_idx = 0; frag_idx < frag_per_ep; frag_idx++) {
            ucs_assert(req->step->fragment_pending[frag_idx] == 0);
        }
    }

    /* Start on the next step for this collective operation */
    ucg_builtin_op_step_t *prev_step  = req->step;
    ucg_coll_id_t coll_id             = prev_step->am_header.msg.coll_id;
    ucg_builtin_op_step_t *next_step  = req->step = prev_step + 1;
    req->pending                      = next_step->fragments_total;
    ucg_builtin_header_t header       = next_step->am_header;
    header.msg.coll_id                = coll_id;

    ucg_builtin_comp_ft_end_step(next_step - 1);

    ucs_assert(next_step->iter_offset == 0);
    ucs_assert(next_step->iter_ep == 0);

    return ucg_builtin_step_execute(req, header);
}

static void UCS_F_ALWAYS_INLINE
ucg_builtin_comp_gather(uint8_t *recv_buffer, uint64_t offset, uint8_t *data,
                        size_t per_rank_length, size_t length,
                        ucg_group_member_index_t root)
{
    size_t my_offset = per_rank_length * root; // TODO: fix... likely broken
    if ((offset > my_offset) ||
        (offset + length <= my_offset)) {
        memcpy(recv_buffer + offset, data, length);
        return;
    }

    /* The write would overlap with my own contribution to gather */
    size_t first_part = offset + length - my_offset;
    recv_buffer += offset;
    memcpy(recv_buffer, data, first_part);
    data += first_part;
    memcpy(recv_buffer + first_part + length, data, length - first_part);

    // TODO: replace with SIMD like _mm256_i64gather_epi64 whenever possible
    // TODO: for reduction - combine a reduce SIMD, e.g. _mm512_reduce_add_pd
}

static ucs_status_t UCS_F_ALWAYS_INLINE
ucg_builtin_comp_unpack_rkey(ucg_builtin_op_step_t *step, uint64_t remote_addr,
                             uint8_t *packed_remote_key)
{
    /* Unpack the information from the payload to feed the next (0-copy) step */
    step->zcopy.raddr           = remote_addr;

    return uct_rkey_unpack(step->zcopy.cmpt, packed_remote_key, &step->zcopy.rkey);
}

static int UCS_F_ALWAYS_INLINE
ucg_builtin_comp_send_check_frag_by_offset(ucg_builtin_request_t *req,
                                           uint64_t offset, uint8_t batch_cnt)
{
    ucg_builtin_op_step_t *step = req->step;
    unsigned frag_idx = offset / step->fragment_length;
    ucs_assert(step->fragment_pending[frag_idx] >= batch_cnt);
    step->fragment_pending[frag_idx] -= batch_cnt;

    if (step->fragment_pending[frag_idx] == 0) {
        if (ucs_unlikely(step->iter_offset == UCG_BUILTIN_OFFSET_PIPELINE_PENDING)) {
            step->fragment_pending[frag_idx] = UCG_BUILTIN_FRAG_PENDING;
        } else {
            step->iter_offset = offset;
            return 1;
        }
    }

    return step->iter_offset != UCG_BUILTIN_OFFSET_PIPELINE_READY;
}

static ucg_builtin_op_step_t*
ucg_builtin_find_step_by_header(ucg_builtin_request_t *req,
                                ucg_builtin_header_t header)
{
    return NULL; // TODO: implement...
}

static ucs_status_t UCS_F_ALWAYS_INLINE
ucg_builtin_step_recv_handle_chunk(enum ucg_builtin_op_step_comp_aggregation ag,
                                   uint8_t *dst, uint8_t *src, size_t length,
                                   ucg_builtin_header_t header, int is_fragment,
                                   int is_swap, int is_dt_packed,
                                   ucg_builtin_request_t *req)
{
    ucs_status_t status;
    ucg_builtin_op_t *op;
    ucg_collective_params_t *params;
    ucg_builtin_op_step_t *ooo_step;
    ucp_dt_generic_t *gen_dt;
    uint8_t *reduce_buf;
    void *gen_state;
    ptrdiff_t dsize;
    ptrdiff_t gap;
    uint8_t *tmp;
    int ret;

    switch (ag) {
    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_NOP:
        status = UCS_OK;
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_WRITE_OOO:
        ooo_step = ucg_builtin_find_step_by_header(req, header);
        dst      = ooo_step->recv_buffer + header.remote_offset;
        /* no break */
    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_WRITE:
        op = req->op;
        if (is_dt_packed) {
            ucs_assert(ucp_dt_length(op->recv_dt, 0, NULL, op->recv_unpack) ==
                       req->step->dtype_length);

            gen_dt = ucp_dt_to_generic(op->recv_dt);
            status = gen_dt->ops.unpack(op->recv_unpack, header.remote_offset,
                                        src, length);
        } else {
            if (is_fragment) {
                ucs_assert(length <= req->step->fragment_length);
            } else {
                ucs_assert(length + header.remote_offset <=
                           req->step->buffer_length);
            }

            memcpy(dst, src, length);
            status = UCS_OK;
        }
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_GATHER:
        op = req->op;
        ucg_builtin_comp_gather(req->step->recv_buffer, header.remote_offset,
                                src, ucg_builtin_step_length(req->step,
                                                             &op->super.params,
                                                             0), length,
                                UCG_PARAM_TYPE(&req->op->super.params).root);
        status = UCS_OK;
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE:
    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE_SWAP:
        op = req->op;

        if (is_swap) {
            tmp  = src;
            src  = dst;
            dst  = tmp;
        }

        if (is_dt_packed) {
            params    = &op->super.params;
            gen_dt    = ucp_dt_to_generic(op->recv_dt);
            ret       = ucg_global_params.datatype.get_span_f(params->recv.dtype,
                                                              params->recv.count,
                                                              &dsize,
                                                              &gap);

            if (ret) {
                ucs_error("failed to get the span of a datatype");
                return UCS_ERR_INVALID_PARAM;
            }

            reduce_buf = (uint8_t*)ucs_alloca(dsize);
            gen_state  = gen_dt->ops.start_unpack(gen_dt->context,
                                                  reduce_buf - gap,
                                                  params->recv.count);

            gen_dt->ops.unpack(gen_state, 0, params->send.buffer, length);
            gen_dt->ops.finish(gen_state);
            src = reduce_buf - gap;
            // TODO: (alex) offset = (offset / dt_len) * params->recv.dt_len;
        }

        if (is_fragment) {
            op->super.reduce_frag_f(dst, src, length, &op->super);
        } else {
            op->super.reduce_full_f(dst, src, &op->super);
        }

        if (is_swap) {
            memcpy(src, dst, is_fragment ? length : req->step->buffer_length);
        }

        status = UCS_OK;
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REMOTE_KEY:
        /* zero-copy prepares the key for the next step */
        ucs_assert(length == req->step->phase->md_attr->rkey_packed_size);
        status = ucg_builtin_comp_unpack_rkey(req->step + 1,
                                              header.remote_offset, src);
        break;
    }

    return status;
}

#define case_recv_full(aggregation, _is_batched, _is_fragmented,               \
                       _is_len_packed, _is_dt_packed, _is_buf_long)            \
   case ((_is_batched    ? UCG_BUILTIN_OP_STEP_COMP_FLAG_BATCHED_DATA    : 0) |\
         (_is_fragmented ? UCG_BUILTIN_OP_STEP_COMP_FLAG_FRAGMENTED_DATA : 0) |\
         (_is_len_packed ? UCG_BUILTIN_OP_STEP_COMP_FLAG_PACKED_LENGTH   : 0) |\
         (_is_dt_packed  ? UCG_BUILTIN_OP_STEP_COMP_FLAG_PACKED_DATATYPE : 0) |\
         (_is_buf_long   ? UCG_BUILTIN_OP_STEP_COMP_FLAG_LONG_BUFFERS    : 0)):\
                                                                               \
        is_swap = (aggregation ==                                              \
                   UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE_SWAP);            \
                                                                               \
        if (_is_fragmented) {                                                  \
           chunk_size = step->buffer_length - header.remote_offset;            \
           if (_is_len_packed) {                                               \
               chunk_size = ucs_min(chunk_size,                                \
                   UCT_COLL_DTYPE_MODE_UNPACK_VALUE(step->fragment_length));   \
           } else {                                                            \
               chunk_size = ucs_min(chunk_size, step->fragment_length);        \
           }                                                                   \
        } else {                                                               \
           if (_is_len_packed) {                                               \
               chunk_size =                                                    \
                   UCT_COLL_DTYPE_MODE_UNPACK_VALUE(step->buffer_length);      \
           } else {                                                            \
               chunk_size = step->buffer_length;                               \
           }                                                                   \
        }                                                                      \
                                                                               \
        if (_is_batched && (am_flags & UCT_CB_PARAM_FLAG_STRIDE)) {            \
            header.remote_offset *= chunk_size;                                \
            length += sizeof(ucg_builtin_header_t);                            \
                                                                               \
            for (index = 0; index < step->batch_cnt; index++, data += length) {\
                status = ucg_builtin_step_recv_handle_chunk(aggregation,       \
                                                            dest_buffer, data, \
                                                            chunk_size, header,\
                                                            _is_fragmented,    \
                                                            is_swap,           \
                                                            _is_dt_packed,     \
                                                            req);              \
                if (ucs_unlikely(status != UCS_OK)) {                          \
                    goto recv_handle_error;                                    \
                }                                                              \
            }                                                                  \
        } else {                                                               \
            status = ucg_builtin_step_recv_handle_chunk(aggregation,           \
                                                        dest_buffer, data,     \
                                                        length, header,        \
                                                        _is_fragmented,        \
                                                        is_swap, _is_dt_packed,\
                                                        req);                  \
            if (ucs_unlikely(status != UCS_OK)) {                              \
                goto recv_handle_error;                                        \
            }                                                                  \
        }                                                                      \
                                                                               \
        break;

#define case_recv_fragmented(a,    _is_fragmented, _is_len_packed, _is_dt_packed, _is_buf_long) \
              case_recv_full(a, 0, _is_fragmented, _is_len_packed, _is_dt_packed, _is_buf_long) \
              case_recv_full(a, 1, _is_fragmented, _is_len_packed, _is_dt_packed, _is_buf_long)

#define case_recv_len_packed(a,    _is_len_packed, _is_dt_packed, _is_buf_long) \
        case_recv_fragmented(a, 0, _is_len_packed, _is_dt_packed, _is_buf_long) \
        case_recv_fragmented(a, 1, _is_len_packed, _is_dt_packed, _is_buf_long)

#define  case_recv_dt_packed(a,    _is_dt_packed, _is_buf_long) \
        case_recv_len_packed(a, 0, _is_dt_packed, _is_buf_long) \
        case_recv_len_packed(a, 1, _is_dt_packed, _is_buf_long)

#define   case_recv_buf_long(a,    _is_buf_long) \
         case_recv_dt_packed(a, 0, _is_buf_long) \
         case_recv_dt_packed(a, 1, _is_buf_long)

#define case_recv(a) \
        case a: \
            switch (step->comp_flags & UCG_BUILTIN_OP_STEP_COMP_FLAG_MASK) { \
                case_recv_buf_long(a, 1) \
                case_recv_buf_long(a, 0) \
            } \
        break;

static void UCS_F_ALWAYS_INLINE
ucg_builtin_step_recv_handle_data(ucg_builtin_request_t *req,
                                  ucg_builtin_header_t header,
                                  uint8_t *data, size_t length,
                                  unsigned am_flags)
{
    int is_swap;
    uint8_t index;
    size_t chunk_size;
    ucs_status_t status;
    ucg_builtin_op_step_t *step = req->step;
    uint8_t *dest_buffer        = step->recv_buffer + header.remote_offset;

    switch (step->comp_aggregation) {
        case_recv(UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_NOP)
        case_recv(UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_WRITE)
        case_recv(UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_WRITE_OOO)
        case_recv(UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_GATHER)
        case_recv(UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE)
        case_recv(UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE_SWAP)
        case_recv(UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REMOTE_KEY)
    }

    return;

recv_handle_error:
    ucg_builtin_comp_last_step_cb(req, status);
}

#define UCG_IF_STILL_PENDING(req, num, dec) \
    ucs_assert(req->pending > (num)); \
    req->pending -= dec; \
    if (ucs_unlikely(req->pending != (num)))
    /* note: not really likely, but we optimize for the positive case */

static int UCS_F_ALWAYS_INLINE
ucg_builtin_step_recv_handle_comp(ucg_builtin_request_t *req,
                                  ucg_builtin_header_t header)
{
    ucg_builtin_op_step_t *step = req->step;

    /* Check according to the requested completion criteria */
    switch (step->comp_criteria) {
    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_SEND:
        return 0;

    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_SINGLE_MESSAGE:
        break;

    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES:
        UCG_IF_STILL_PENDING(req, 0, 1) {
            return 0;
        }
        break;

    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES_ZCOPY:
        UCG_IF_STILL_PENDING(req, step->fragments_total, 1) {
            return 0;
        }
        break;

    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_BY_FRAGMENT_OFFSET:
        if (!ucg_builtin_comp_send_check_frag_by_offset(req, header.remote_offset, 1)) {
            return 0;
        }
        break;
    }


    /* Act according to the requested completion action */
    switch (step->comp_action) {
    case UCG_BUILTIN_OP_STEP_COMP_OP:
        ucg_builtin_comp_last_step_cb(req, UCS_OK);
        break;

    case UCG_BUILTIN_OP_STEP_COMP_STEP:
        (void) ucg_builtin_comp_step_cb(req);
        break;

    case UCG_BUILTIN_OP_STEP_COMP_SEND:
        (void) ucg_builtin_step_execute(req, step->am_header);
        break;
    }

    return 1;
}

static int UCS_F_ALWAYS_INLINE
ucg_builtin_step_recv_cb(ucg_builtin_request_t *req, ucg_builtin_header_t header,
                         uint8_t *data, size_t length, unsigned am_flags_ext)
{
    ucg_builtin_step_recv_handle_data(req, header, data, length, am_flags_ext);

    if (ucs_unlikely(am_flags_ext & UCT_CB_PARAM_FLAG_LAST)) {
        return 0; /* step is not done */
    }

    return ucg_builtin_step_recv_handle_comp(req, header);
}

ucs_status_t static UCS_F_ALWAYS_INLINE
ucg_builtin_step_check_pending(ucg_builtin_comp_slot_t *slot,
                               ucg_builtin_op_step_t *step,
                               ucg_builtin_header_t expected)
{
    /* Check pending incoming messages - invoke the callback on each one */
    void *msg;
    size_t length;
    int is_batch;
    int is_step_done;
    unsigned msg_index;
    unsigned mock_flag;
    ucp_recv_desc_t *rdesc;
    ucg_builtin_header_t *header;

    uint16_t local_id            = expected.msg.local_id;
    step->am_header.msg.local_id = local_id;
    slot->req.expecting.local_id = local_id;

    ucs_assert(local_id != 0);

    if (ucs_ptr_array_is_empty(&slot->messages)) {
        return UCS_INPROGRESS;
    }

    mock_flag = 0;
    is_batch  = step->comp_flags & UCG_BUILTIN_OP_STEP_COMP_FLAG_BATCHED_DATA;

    ucs_ptr_array_for_each(msg, msg_index, &slot->messages) {
        rdesc  = (ucp_recv_desc_t*)msg;
        header = (ucg_builtin_header_t*)(rdesc + 1);
        ucs_assert((header->msg.coll_id  != slot->req.expecting.coll_id) ||
                   (header->msg.step_idx >= slot->req.expecting.step_idx));
        /*
         * Note: stored message coll_id can be either larger or smaller than
         * the one currently handled - due to coll_id wrap-around.
         */

        if (ucs_likely(header->msg.local_id == local_id)) {
            /* Remove the packet (next call may lead here recursively) */
            ucs_ptr_array_remove(&slot->messages, msg_index);

            /* we need this in place in case we skip to the next step */
            slot->req.expecting.local_id = local_id;

            /* Handle this "waiting" packet, possibly completing the step */
            if (((rdesc->flags & UCT_CB_PARAM_FLAG_DESC) == 0) &&
                 (step->comp_criteria <= UCG_BUILTIN_OP_STEP_COMP_CRITERIA_SINGLE_MESSAGE)) {
                               /* can be UCG_BUILTIN_OP_STEP_COMP_CRITERIA_SEND */
                mock_flag = UCT_CB_PARAM_FLAG_LAST;
            }

            if (is_batch) {
                /* At the time of arrival the length was not known, since the
                 * batches are padded in memory - so we grab the information
                 * from the step (information specified by local invocation) */
                length = ucg_builtin_step_length(step,
                                                 &slot->req.op->super.params,
                                                 0);

                if (step->flags & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED) {
                    if (header->remote_offset + step->fragment_length >= length) {
                        /* Last fragment */
                        ucs_assert(length > header->remote_offset);
                        length = length - header->remote_offset;
                    } else {
                        /* Ordinary fragment */
                        length = step->fragment_length;
                    }
                }

                ucs_assert(length <= (rdesc->length - sizeof(ucg_builtin_header_t)));
            } else {
                length = rdesc->length - sizeof(ucg_builtin_header_t);
            }

            is_step_done = ucg_builtin_step_recv_cb(&slot->req, *header,
                                                    (uint8_t*)(header + 1),
                                                    length, mock_flag);

            /* Dispose of the packet, according to its allocation */
            ucp_recv_desc_release(rdesc
#ifdef HAVE_UCP_EXTENSIONS
#ifdef UCS_MEMUNITS_INF /* Backport to UCX v1.6.0 */
                    , slot->req.step->uct_iface
#endif
#endif
                    );

            /* If the step has indeed completed - check the entire operation */
            if (is_step_done) {
                goto step_done;
            }
        }
    }

    if ((mock_flag != 0) &&
        (ucg_builtin_step_recv_handle_comp(&slot->req, *header))) {
        goto step_done;
    }

    return UCS_INPROGRESS;

step_done:
    return (slot->req.expecting.local_id == 0) ? UCS_OK : UCS_INPROGRESS;
}
