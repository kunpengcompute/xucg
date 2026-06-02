/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "planc_stars_buffer.h"

#define BUF_DESC_IOV_LEN 2

static inline void* ucg_planc_stars_alloc_event_elem(uint32_t elem_num)
{
    return ucg_calloc(elem_num, sizeof(stars_event_elem_t), "event elems");
}

static inline uint32_t ucg_planc_stars_cal_elem_num(stars_comm_dep_h comm_dep)
{
    uint32_t elem_num = 0;
    uint32_t length;
    stars_rank_info_h rank_info;
    for (uint32_t idx = 0; idx < comm_dep->get_rank_num; ++idx) {
        rank_info = &comm_dep->get_ranks[idx];
        length = rank_info->length;
        elem_num += length;
    }
    return elem_num;
}

static ucg_status_t ucg_planc_stars_algo_alloc_event_shared(ucg_planc_stars_op_t *op)
{
    ucg_status_t status;
    stars_comm_plan_t *plan = &op->plan;
    stars_rank_info_h rank_info;
    events_pool_h *events_pool = op->stars_group->context->events_pool;
    int stride = op->plan.eid_shared_stride;

    stars_event_elem_h elem =
            ucg_planc_stars_alloc_event_elem(stride);
    UCG_ASSERT_RET(elem != NULL, UCG_ERR_NO_MEMORY);
    plan->event_elem = elem;

    rank_info = &plan->comm_dep.get_ranks[0];
    for (int i = 0; i < stride; i++) {
        status = scp_ep_alloc_event(rank_info->ep, &elem->event, events_pool);
        UCG_CHECK_GOTO_ERR(status, out, "Failed to alloc event!");
        elem++;
    }

    for (uint32_t idx = 0; idx < plan->comm_dep.get_rank_num; ++idx) {
        rank_info = &plan->comm_dep.get_ranks[idx];
        rank_info->offset = idx % stride;
    }
    return UCG_OK;

out:
    return status;
}

static ucg_status_t ucg_planc_stars_algo_alloc_event_common(ucg_planc_stars_op_t *op)
{
    ucg_status_t status;
    stars_comm_plan_t *plan = &op->plan;
    stars_rank_info_h rank_info;
    events_pool_h *events_pool = op->stars_group->context->events_pool;
    uint32_t length;
    uint32_t elem_offset = 0;

    uint32_t elem_num = ucg_planc_stars_cal_elem_num(&plan->comm_dep);
    stars_event_elem_h elem =
        ucg_planc_stars_alloc_event_elem(elem_num);
    UCG_ASSERT_RET(elem != NULL, UCG_ERR_NO_MEMORY);
    plan->event_elem = elem;

    for (uint32_t idx = 0; idx < plan->comm_dep.get_rank_num; ++idx) {
        rank_info = &plan->comm_dep.get_ranks[idx];
        length = rank_info->length;
        rank_info->offset = elem_offset;
        for (uint32_t elem_idx = 0; elem_idx < length; ++elem_idx) {
            elem->rank = rank_info->peer_id;
            status = scp_ep_alloc_event(rank_info->ep, &elem->event, events_pool);
            UCG_CHECK_GOTO_ERR(status, out, "Failed to alloc event!");
            elem++;
            elem_offset++;
        }
        if (rank_info->barrier_flag == 0) {
            continue;
        }
        rank_info->barrier_event = ucg_calloc(EP_BARRIER_EVENT_NUM, sizeof(scp_event_t), "barrie event");
        UCG_ASSERT_RET(rank_info->barrier_event != NULL, UCG_ERR_NO_MEMORY);
        for (int i = 0; i < EP_BARRIER_EVENT_NUM; i++) {
            status = scp_ep_alloc_event(rank_info->ep, &rank_info->barrier_event[i], events_pool);
            UCG_CHECK_GOTO_ERR(status, out, "Failed to alloc barrier event!");
        }
    }
    return UCG_OK;

out:
    ucg_planc_stars_event_elem_cleanup(elem);
    return status;
}

ucg_status_t ucg_planc_stars_rbuf_init(ucg_planc_stars_op_t *op, void *buffer,
                                       size_t length)
{
    UCG_STATS_GET_TIME(start_tick);
    stars_comm_plan_t *plan = &op->plan;
    plan->lrbuf_desc = ucg_planc_stars_plan_alloc_local_buf_desc(op);
    UCG_STATS_GET_TIME(alloc_buf_desc_tick);
    UCG_ASSERT_RET(plan->lrbuf_desc != NULL, UCG_ERR_NO_MEMORY);

    plan->lrbuf_desc->addr   = (uintptr_t)buffer;
    plan->lrbuf_desc->length = length;

    scp_context_h context = op->stars_group->context->scp_context;
    ucg_status_t status = scp_mem_reg(context, buffer, length, &plan->lrmemh);
    UCG_STATS_GET_TIME(mem_reg_tick);
    UCG_ASSERT_CODE_GOTO(status, free_buf);

    size_t packed_size = 0;
    void *packed_ptr = &plan->lrbuf_desc->key_packed[0];
    status = scp_md_mkey_pack(context, plan->lrmemh, packed_ptr, &packed_size);
    UCG_STATS_GET_TIME(mkey_pack_tick);
    UCG_STATS_COST_TIME(start_tick, alloc_buf_desc_tick,
                        op->stats.init.exch_buf.init_rbuf.lrbuf_desc_alloc);
    UCG_STATS_COST_TIME(alloc_buf_desc_tick, mem_reg_tick,
                        op->stats.init.exch_buf.init_rbuf.mem_reg);
    UCG_STATS_COST_TIME(mem_reg_tick, mkey_pack_tick,
                        op->stats.init.exch_buf.init_rbuf.mkey_pack);
    UCG_ASSERT_CODE_GOTO(status, free_memh);

    if (plan->eid_shared_flag == 0) {
        status = ucg_planc_stars_algo_alloc_event_common(op);
    } else {
        status = ucg_planc_stars_algo_alloc_event_shared(op);
    }

    UCG_ASSERT_CODE_GOTO(status, free_memh);
    plan->buf_desc_flag |= STARS_BUFF_RECV;
    return UCG_OK;

free_memh:
    ucg_free(plan->lrmemh);
    plan->lrmemh = NULL;
free_buf:
    ucg_mpool_put(plan->lrbuf_desc);
    plan->lrbuf_desc = NULL;
    return status;
}

static inline void ucg_planc_stars_local_buf_desc_cleanup(stars_buf_desc_h buf_desc)
{
    if (buf_desc) {
        ucg_mpool_put(buf_desc);
    }
    buf_desc = NULL;
    return;
}

static ucg_status_t ucg_planc_stars_rbuf_deinit(scp_context_h context, stars_comm_plan_t *plan,
                                                ofd_algo_stats_h stats)
{
    if (plan->lrbuf_desc == NULL) {
        return UCG_OK;
    }

    UCG_STATS_GET_TIME(stars_tick);
    ucg_status_t status =
        scp_mem_dereg(context, plan->lrmemh);
    UCG_STATS_GET_TIME(mem_dereg);
    UCG_ASSERT_CODE_RET(status);

    ucg_planc_stars_local_buf_desc_cleanup(plan->lrbuf_desc);
    UCG_STATS_GET_TIME(desc_free);
    UCG_STATS_COST_TIME(stars_tick, mem_dereg,
                        stats->discard.buf_cleanup.rbuf_deinit.mem_dereg);
    UCG_STATS_COST_TIME(mem_dereg, desc_free,
                        stats->discard.buf_cleanup.rbuf_deinit.desc_free);
    return UCG_OK;
}

ucg_status_t ucg_planc_stars_sbuf_init(ucg_planc_stars_op_t *op, void *buffer,
                                       size_t length)
{
    stars_comm_plan_t *plan = &op->plan;
    plan->lsbuf_desc = ucg_planc_stars_plan_alloc_local_buf_desc(op);
    UCG_ASSERT_RET(plan->lsbuf_desc != NULL, UCG_ERR_NO_MEMORY);

    ucg_status_t status =
        scp_mem_reg(op->stars_group->context->scp_context,
                    buffer, length, &plan->lsmemh);
    UCG_ASSERT_CODE_GOTO(status, free_desc);

    plan->lsbuf_desc->addr = (uintptr_t)buffer;
    plan->lsbuf_desc->length = length;
    plan->buf_desc_flag |= STARS_BUFF_SEND;
    return UCG_OK;

free_desc:
    ucg_mpool_put(plan->lsbuf_desc);
    plan->lsbuf_desc = NULL;
    return status;
}

static ucg_status_t ucg_planc_stars_sbuf_deinit(scp_context_h context, stars_comm_plan_t *plan)
{
    if (plan->lsbuf_desc == NULL) {
        return UCG_OK;
    }

    ucg_planc_stars_local_buf_desc_cleanup(plan->lsbuf_desc);
    return scp_mem_dereg(context, plan->lsmemh);
}

static inline void ucg_planc_stars_event_elem_cleanup(stars_event_elem_h *event_elem)
{
    if (*event_elem) {
        ucg_free(*event_elem);
    }
    *event_elem = NULL;
    return;
}

static void ucg_planc_stars_shared_events_release(ucg_planc_stars_op_t *op)
{
    stars_comm_plan_t *plan = &op->plan;
    stars_event_elem_h elem = plan->event_elem;
    stars_rank_info_h rank;
    events_pool_h *events_pool = op->stars_group->context->events_pool;
    int stride = op->plan.eid_shared_stride;
    ucg_status_t status;

    if (op->event_clear_flag == EVENT_CLEARED) {
        return;
    }
    if (plan->comm_dep.get_rank_num <= 0) {
        return;
    }
    rank = &plan->comm_dep.get_ranks[0];
    for (int i = 0; i < stride; i++) {
        scp_event_h scp_event = &elem->event;
        status = scp_ep_free_event(rank->ep, scp_event, events_pool);
        if (ucg_unlikely(status != UCG_OK)) {
            ucg_fatal("Failed to free sct ep event");
        }
        elem++;
    }
    ucg_planc_stars_event_elem_cleanup(&plan->event_elem);
    op->event_clear_flag = EVENT_CLEARED;

    return;
}

static void ucg_planc_stars_events_release(ucg_planc_stars_op_t *op)
{
    stars_comm_plan_t *plan = &op->plan;
    events_pool_h *events_pool = op->stars_group->context->events_pool;
    stars_event_elem_h elem = plan->event_elem;
    stars_rank_info_h rank;
    uint32_t length;
    uint32_t offset;

    if (op->event_clear_flag == EVENT_CLEARED) {
        return;
    }
    if (plan->comm_dep.get_rank_num <= 0) {
        return;
    }

    ucg_status_t status;
    for (uint32_t idx = 0; idx < plan->comm_dep.get_rank_num; ++idx) {
        rank = &plan->comm_dep.get_ranks[idx];
        length = rank->length;
        offset = rank->offset;
        for (uint32_t elem_idx = 0; elem_idx < length; ++elem_idx) {
            scp_event_h scp_event = &elem[offset + elem_idx].event;
            status = scp_ep_free_event(rank->ep, scp_event, events_pool);
            if (ucg_unlikely(status != UCG_OK)) {
                ucg_fatal("Failed to free sct ep event");
            }
        }
        if (rank->barrier_flag == 0) {
            continue;
        }
        ucg_assert(rank->barrier_event != NULL);
        for (int i = 0; i < EP_BARRIER_EVENT_NUM; i++) {
            status = scp_ep_free_event(rank->ep, &rank->barrier_event[i], events_pool);
            if (ucg_unlikely(status != UCG_OK)) {
                ucg_fatal("Failed to free barrier event");
            }
        }
        ucg_free(rank->barrier_event);
    }
    ucg_planc_stars_event_elem_cleanup(&plan->event_elem);
    op->event_clear_flag = EVENT_CLEARED;

    return;
}

void ucg_planc_stars_buf_cleanup(ucg_planc_stars_op_t *op)
{
    stars_comm_plan_t *plan = &op->plan;
    UCG_STATS_GET_TIME(start_tick);
    if (plan->eid_shared_flag == 0) {
        ucg_planc_stars_events_release(op);
    } else {
        ucg_planc_stars_shared_events_release(op);
    }


    UCG_STATS_GET_TIME(event_free);
    if (plan->buf_desc_flag  & STARS_BUFF_SEND) {
        ucg_planc_stars_sbuf_deinit(op->stars_group->context->scp_context, plan);
    }
    UCG_STATS_GET_TIME(sbuf_deinit);
    if (plan->buf_desc_flag  & STARS_BUFF_RECV) {
        ucg_planc_stars_rbuf_deinit(op->stars_group->context->scp_context, plan, &op->stats);
    }
    UCG_STATS_GET_TIME(rbuf_deinit);
    UCG_STATS_COST_TIME(start_tick, event_free,
                        op->stats.discard.buf_cleanup.event_free);
    UCG_STATS_COST_TIME(event_free, sbuf_deinit,
                        op->stats.discard.buf_cleanup.sbuf_deinit);
    UCG_STATS_COST_TIME(sbuf_deinit, rbuf_deinit,
                        op->stats.discard.buf_cleanup.rbuf_deinit.total);
}

static inline void ucg_planc_stars_put_rank_fill_desc_iov(stars_rank_info_h put_rank)
{
    ucp_dt_iov_t *desc_iov_dt = (void*)&put_rank->desc_iov_dt[0];
    void *ptr = (void*)put_rank->rbuf_desc;
    uint32_t elem_length = put_rank->length;

    desc_iov_dt[0].buffer = (void*)ptr;
    desc_iov_dt[0].length = BUF_DESC_HEAD_SIZE;
    desc_iov_dt[1].buffer = (void*)ptr + BUF_DESC_HEAD_SIZE;
    desc_iov_dt[1].length = STARS_EVENT_ELEM_SIZE(elem_length);
    return;
}

static inline void ucg_planc_stars_get_rank_fill_desc_iov(stars_rank_info_h get_rank,
                                                          stars_event_elem_h elem)
{
    ucp_dt_iov_t *desc_iov_dt = (void*)&get_rank->desc_iov_dt[0];
    uint32_t elem_length = get_rank->length;
    uint32_t elem_offset = get_rank->offset;

    desc_iov_dt[0].buffer = (void*)get_rank->rbuf_desc;
    desc_iov_dt[0].length = BUF_DESC_HEAD_SIZE;
    desc_iov_dt[1].buffer = (void*)&elem[elem_offset];
    desc_iov_dt[1].length = STARS_EVENT_ELEM_SIZE(elem_length);
    return;
}

ucg_status_t ucg_planc_stars_exch_addr_msg(ucg_planc_stars_op_t *op)
{
    ucg_status_t status;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    stars_comm_dep_h comm_dep = &op->plan.comm_dep;
    stars_comm_plan_t *plan = &op->plan;
    stars_event_elem_h elem = plan->event_elem;
    ucg_planc_stars_p2p_params_t params;
    ucg_planc_stars_set_p2p_params(op, &params);

    for (uint32_t idx = 0; idx < comm_dep->put_rank_num; ++idx) {
        stars_rank_info_h peer_rank = &comm_dep->put_ranks[idx];
        ucg_planc_stars_put_rank_fill_desc_iov(peer_rank);
        ucp_datatype_t ucp_dt = ucp_dt_make_iov();
        status = ucg_planc_stars_oob_irecv(peer_rank->peer_id,
                                           &peer_rank->desc_iov_dt[0],
                                           BUF_DESC_IOV_LEN, op->tag, vgroup, &params, ucp_dt);
        UCG_ASSERT_CODE_RET(status);
    }

    for (uint32_t idx = 0; idx < comm_dep->get_rank_num; ++idx) {
        stars_rank_info_h peer_rank = &comm_dep->get_ranks[idx];
        ucg_planc_stars_get_rank_fill_desc_iov(peer_rank, elem);
        ucp_datatype_t ucp_dt = ucp_dt_make_iov();
        status = ucg_planc_stars_oob_isend(peer_rank->peer_id,
                                           &peer_rank->desc_iov_dt[0],
                                           BUF_DESC_IOV_LEN, op->tag, vgroup, &params, ucp_dt);
        UCG_ASSERT_CODE_RET(status);
    }

    return ucg_planc_stars_oob_waitall(params.state);
}

ucg_status_t ucg_planc_stars_mkey_unpack(ucg_planc_stars_op_t *op)
{
    ucg_status_t status;
    stars_comm_dep_h comm_dep = &op->plan.comm_dep;
    for (uint32_t index = 0; index < comm_dep->put_rank_num; ++index) {
        stars_rank_info_h peer_rank = &comm_dep->put_ranks[index];
        void *unpack_ptr = (void *)&peer_rank->rbuf_desc->key_packed[0];
        uct_rkey_bundle_t **rkey_bundle = &peer_rank->rkey_bundle;
        status = scp_md_mkey_unpack(peer_rank->ep->worker->context, unpack_ptr,
                                    rkey_bundle, &peer_rank->rkey_packed_size);
        UCG_ASSERT_CODE_RET(status);
    }
    return UCG_OK;
}
