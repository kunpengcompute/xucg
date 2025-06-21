/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "scp_rma.h"


void scp_init_ofd_req(scp_ofd_req_h req)
{
    ucs_queue_head_init(&req->queue_head);
    req->req_elem_cnt    = 0;
    req->last_req_elem   = NULL;
    req->result          = UCG_OK;
    req->sct_req         = NULL;
    req->sct_req_len     = 0;
    req->submit_elem_cnt = 0;
    req->finish_elem_cnt = 0;
    memset(req->flag_array, 0, sizeof(req->flag_array));
}

void scp_release_ofd_req(scp_ofd_req_h req)
{
    if (ucs_queue_is_empty(&req->queue_head)) {
        return;
    }

    ucs_queue_iter_t iter;
    scp_ofd_req_elem_h req_elem;
    ucs_queue_for_each_safe(req_elem, iter, &req->queue_head, queue_elem) {
        ucs_queue_del_iter(&req->queue_head, iter);
        ucg_mpool_put(req_elem);
    }

    if (req->sct_req != NULL) {
        ucg_free(req->sct_req);
        req->sct_req = NULL;
    }

    scp_init_ofd_req(req);
}

static ucg_status_t scp_do_put_with_notify(scp_ofd_req_h scp_req, scp_ofd_req_elem_h elem,
                                           scp_lane_index_t src, scp_lane_index_t dst,
                                           uint32_t offset, uint32_t length)
{
    scp_ep_h ep = elem->ep;
    sct_iov_t iov;
    iov.unnotify    = elem->unnotify;
    iov.buffer      = elem->lbuf + offset;
    iov.length      = length;
    iov.memh        = elem->lmemh[src];
    iov.flag        = (scp_req->flag_array[src] == elem->idx) ? 1 : 0;
    iov.sct_event   = &elem->scp_event->sct_event[src];
    iov.remote_addr = (uint64_t)(elem->rbuf + offset);
    iov.rkey        = elem->rkey_bundle[dst].rkey;
    return ucg_status_s2g(sct_ep_put_with_notify(ep->sct_eps[src],
        &scp_req->sct_req[ep->sct_eps_rsc_idx[src]], &iov));
}

static ucg_status_t scp_put_with_notify(scp_ofd_req_h scp_req, scp_ofd_req_elem_h elem)
{
    scp_ep_h ep = elem->ep;
    uint32_t offset = 0, length = 0;

    for (uint8_t conn_idx = 0; conn_idx < ep->conn_num; ++conn_idx) {
        uint8_t src_idx = ep->local_lanes[conn_idx];
        uint8_t dst_idx = ep->remote_lanes[src_idx];
        if (dst_idx >= SCP_MAX_LANE) {
            continue;
        }

        if (conn_idx == ep->conn_num - 1) {
            /* last lane only send the remain space of buffer */
            length = elem->llen - offset;
        } else {
            length = elem->llen * ep->weights[src_idx];
        }

        UCG_ASSERT_CODE_RET(scp_do_put_with_notify(scp_req, elem,
            src_idx, dst_idx, offset, length));

        offset += length;
    }

    return UCG_OK;
}

static ucg_status_t scp_ep_wait_notify(scp_ofd_req_h scp_req, scp_ofd_req_elem_h req_elem)
{
    ucs_status_t status;
    scp_ep_h ep = req_elem->ep;
    scp_rsc_index_t iface_idx;

    for (uint8_t conn_idx = 0, dst, src; conn_idx < ep->conn_num; ++conn_idx) {
        src = ep->local_lanes[conn_idx];
        if ((dst = ep->remote_lanes[src]) >= SCP_MAX_LANE) {
            continue;
        }

        sct_wait_elem_t wait_elem;
        wait_elem.sct_event = &req_elem->scp_event->sct_event[dst];
        wait_elem.flag = (scp_req->flag_array[src] == req_elem->idx) ? 1 : 0;

        iface_idx = ep->sct_eps_rsc_idx[src];
        status = sct_ep_wait_notify(ep->sct_eps[src],
                                    &scp_req->sct_req[iface_idx],
                                    &wait_elem);
        if (status != UCS_OK) {
            ucg_error("Failed to do uct wait notify: %s", ucs_status_string(status));
            return ucg_status_s2g(status);
        }
    }

    return UCG_OK;
}

/*
 * Handlers barrier in a stream.
 * Implemented by two rounds of ring-notify. Four handlers example:
 * Round 1:         notify    notify    notify
 *                0 ------> 1 ------> 2 ------> 3
 *                         wait      wait      wait
 * Round 1 ensures that all handlers' previous requests completed, then round 2 notify handlers to continue.
 * Round 2:          notify    notify    notify
 *                0 <------ 1 <------ 2 <------ 3
 *               wait      wait      wait
 */
static ucg_status_t scp_ep_barrier(scp_ofd_req_h scp_req, scp_ofd_req_elem_h req_elem)
{
    ucs_status_t status;
    scp_ep_h ep = req_elem->ep;
    scp_rsc_index_t iface_idx;
    sct_event_h wait_event[EP_BARRIER_EVENT_NUM];
    sct_event_h notify_event[EP_BARRIER_EVENT_NUM];
    if (ep->sdma_ep_idx != UINT8_MAX) {
        return UCG_OK;
    }

    for (uint8_t conn_idx = 0, src; conn_idx < ep->conn_num; ++conn_idx) {
        src = ep->local_lanes[conn_idx];
        iface_idx = ep->sct_eps_rsc_idx[src];

        if (conn_idx != 0) {
            uint8_t last_src = ep->local_lanes[conn_idx - 1];
            wait_event[0] = &req_elem->scp_event[0].sct_event[src];
            notify_event[1] = &req_elem->scp_event[1].sct_event[last_src];
        } else {
            wait_event[0] = NULL;
            notify_event[1] = NULL;
        }
        if (conn_idx != ep->conn_num - 1) {
            uint8_t next_src = ep->local_lanes[conn_idx + 1];
            wait_event[1] = &req_elem->scp_event[1].sct_event[src];
            notify_event[0] = &req_elem->scp_event[0].sct_event[next_src];
        } else {
            wait_event[1] = NULL;
            notify_event[0] = NULL;
        }
        status = sct_ep_barrier(ep->sct_eps[src],
                                &scp_req->sct_req[iface_idx],
                                notify_event, wait_event,
                                EP_BARRIER_EVENT_NUM);
        if (status != UCS_OK) {
            ucg_error("Failed to do ep barrier: %s", ucs_status_string(status));
            return ucg_status_s2g(status);
        }
    }

    return UCG_OK;
}

static void scp_submit_ofd_hander(void *context, ucg_status_t status)
{
    if (ucg_unlikely(context == NULL)) {
        ucg_error("Invalid offload callback context");
        return;
    }

    scp_ofd_req_h req = (scp_ofd_req_h)context;
    if (ucg_unlikely(status != UCG_OK)) {
        ucg_error("offload tasks handler failure, %d", status);
        req->result = status;
        return;
    }

    req->finish_elem_cnt++;
    if (req->finish_elem_cnt >= req->submit_elem_cnt) {
        req->result = UCG_OK;
    }
    ucg_debug("submit_elem_cnt %d finish_elem_cnt %d", req->submit_elem_cnt, req->finish_elem_cnt);
}

static ucg_status_t scp_pre_ofd_req(scp_ofd_req_h req, scp_ofd_stars_stream_h stream, uint8_t iface_cnt)
{
    UCG_ASSERT_RET(iface_cnt != 0, UCG_ERR_INVALID_PARAM);
    UCG_ASSERT_RET(stream->num == iface_cnt, UCG_ERR_INVALID_PARAM);

    req->sct_req_len = iface_cnt;
    req->sct_req = ucg_calloc(iface_cnt, sizeof(sct_ofd_req_t), "all sct_req");
    UCG_ASSERT_RET(req->sct_req != NULL, UCG_ERR_NO_MEMORY);

    ucg_status_t status;
    for (uint8_t idx = 0; idx < req->sct_req_len; ++idx) {
        status = sct_ofd_req_init(&req->sct_req[idx], stream->stars_handle[idx]);
        UCG_CHECK_GOTO_ERR(status, err_free, "init sct req");
    }

    return UCG_OK;

err_free:
    for (uint8_t idx = 0; idx < req->sct_req_len; ++idx) {
        sct_ofd_req_clean(&req->sct_req[idx]);
    }

    ucg_free(req->sct_req);
    req->sct_req = NULL;
    return status;
}

static void scp_get_req_flag(scp_ofd_req_h req, scp_ofd_req_elem_h elem, int req_elem_idx)
{
    scp_ep_h ep = elem->ep;
    if (elem->type == OFFLOAD_BARRIER) {
        return;
    }
    for (uint8_t conn_idx = 0; conn_idx < ep->conn_num; ++conn_idx) {
        uint8_t src_idx = ep->local_lanes[conn_idx];
        uint8_t dst_idx = ep->remote_lanes[src_idx];
        if (dst_idx >= SCP_MAX_LANE) {
            continue;
        }

        req->flag_array[src_idx] = req_elem_idx;
    }

    elem->idx = req_elem_idx;
}

ucg_status_t scp_submit_ofd_req(scp_worker_h worker, scp_ofd_stars_stream_h stream,
                                scp_ofd_req_h req, ofd_stats_elem_h stats)
{
    UCG_STATS_GET_TIME(start_tick);
    req->last_req_elem->flag = 1;
    ucg_status_t status = scp_pre_ofd_req(req, stream, worker->num_ifaces);
    UCG_ASSERT_CODE_RET(status);

    UCG_STATS_GET_TIME(task_tick);
    scp_ofd_req_elem_h req_elem = NULL;
    pthread_mutex_lock(&worker->submit_mutex);

    /* get the last req_elem of each iface */
    int req_elem_idx = 0;
    ucs_queue_for_each(req_elem, &req->queue_head, queue_elem) {
        if (req_elem->type != OFFLOAD_PUT && req_elem->type != OFFLOAD_WAIT
            && req_elem->type != OFFLOAD_BARRIER) {
            pthread_mutex_unlock(&worker->submit_mutex);
            ucg_fatal("Unexpected request task type %d", req_elem->type);
        }

        scp_get_req_flag(req, req_elem, req_elem_idx++);
    }

    ucs_queue_for_each(req_elem, &req->queue_head, queue_elem) {
        if (req_elem->type == OFFLOAD_PUT) {
            status = scp_put_with_notify(req, req_elem);
        } else if (req_elem->type == OFFLOAD_WAIT) {
            status = scp_ep_wait_notify(req, req_elem);
        } else if (req_elem->type == OFFLOAD_BARRIER) {
            status = scp_ep_barrier(req, req_elem);
        } else {
            pthread_mutex_unlock(&worker->submit_mutex);
            ucg_fatal("Unexpected request task type %d", req_elem->type);
        }
        UCG_ASSERT_CODE_GOTO(status, out);
    }

    UCG_STATS_GET_TIME(submit_tick);
    ucs_status_t ucs_status;
    sct_ofd_req_h sct_req = NULL;
    for (uint8_t idx = 0; idx < req->sct_req_len; ++idx) {
        sct_req = &req->sct_req[idx];
        scp_worker_iface_h wiface = worker->ifaces[idx];
        if (wiface == NULL || sct_req->stars.trans_task.count <= 0) {
            ucg_debug("No tasks on ifaces[%d]", idx);
            sct_ofd_req_clean(sct_req);
            continue;
        }
        UCG_ASSERT_GOTO(sct_req->stars.cqe_cnt > 0, out,
                        UCG_ERR_INVALID_PARAM);

        /* rc get from mpool, sdma don't need */
        void *ptr = ucg_malloc(sizeof(stars_cqe_output_t) * sct_req->stars.cqe_cnt,
                               "stars cqe output");
        UCG_ASSERT_GOTO(ptr != NULL, out, UCG_ERR_NO_MEMORY);
        sct_req->stars.output.cqe_output = ptr;
        sct_req->stars.output.out_num = sct_req->stars.cqe_cnt;

        sct_req->cb = &scp_submit_ofd_hander;
        sct_req->context = req;

        ucs_status = sct_iface_submit_request(wiface->iface, sct_req);
        UCG_ASSERT_GOTO(ucs_status == UCS_OK, out, UCG_ERR_IO_ERROR);
        req->submit_elem_cnt++;
    }

    req->result = UCG_INPROGRESS;
    pthread_mutex_unlock(&worker->submit_mutex);
    UCG_STATS_GET_TIME(end_tick);

    UCG_STATS_COST_TIME(start_tick, task_tick,
                        stats->trigger.submit.scp_submit.pre_req);
    UCG_STATS_COST_TIME(task_tick, submit_tick,
                        stats->trigger.submit.scp_submit.enque_task);
    UCG_STATS_COST_TIME(submit_tick, end_tick,
                        stats->trigger.submit.scp_submit.submit_task);
    UCG_STATS_COST_TIME(start_tick, end_tick,
                        stats->trigger.submit.scp_submit.total);
    return UCG_OK;

out:
    pthread_mutex_unlock(&worker->submit_mutex);
    return status;
}