/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "planc_stars_offload.h"
#include "planc_stars_context.h"

static UCS_F_ALWAYS_INLINE
scp_ep_h ucg_planc_stars_get_ep_from_pool(ucg_planc_stars_context_t *context, ucg_rank_t ctx_rank)
{
    khiter_t hash_iter;

    hash_iter = kh_get(scp_ep, &context->eps_pool, ctx_rank);

    return (hash_iter == kh_end(&context->eps_pool)) ?
            NULL : kh_value(&context->eps_pool, hash_iter);
}

static UCS_F_ALWAYS_INLINE
ucg_status_t ucg_planc_stars_put_ep_to_pool(ucg_planc_stars_context_t *context, ucg_rank_t ctx_rank,
                                            scp_ep_h scp_ep)
{
    int ret = 0;
    khiter_t hash_iter = kh_put(scp_ep, &context->eps_pool, ctx_rank, &ret);
    if (ret == -1) {
        ucg_error("Failed to put ep to hash!");
        return UCG_ERR_NO_MEMORY;
    }
    kh_value(&context->eps_pool, hash_iter) = scp_ep;
    return UCG_OK;
}

ucg_status_t ucg_planc_stars_rank_dep_alloc(stars_comm_dep_h comm_dep)
{
    ucg_status_t status = UCG_OK;

    comm_dep->get_ranks = NULL;
    UCG_MATCH_GOTO(comm_dep->get_rank_num == 0, alloc_put_rank);

    comm_dep->get_ranks =
        ucg_calloc(comm_dep->get_rank_num, sizeof(stars_rank_info_t), "comm get ranks");
    UCG_ASSERT_RET(comm_dep->get_ranks != NULL, UCG_ERR_NO_MEMORY);

alloc_put_rank:
    comm_dep->put_ranks = NULL;
    UCG_MATCH_RET(comm_dep->put_rank_num == 0, UCG_OK);

    comm_dep->put_ranks =
        ucg_calloc(comm_dep->put_rank_num, sizeof(stars_rank_info_t), "comm put ranks");
    UCG_ASSERT_GOTO(comm_dep->put_ranks != NULL, err_free_memory,
                    UCG_ERR_NO_MEMORY);

    return UCG_OK;

err_free_memory:
    if (comm_dep->get_ranks) {
        ucg_free(comm_dep->get_ranks);
        comm_dep->get_ranks = NULL;
    }

    return status;
}

ucg_status_t ucg_planc_stars_rank_dep_init(ucg_planc_stars_op_t *op,
                                           stars_rank_info_h peer,
                                           ucg_rank_t peer_id,
                                           uint32_t elem_len)
{
    peer->peer_id     = peer_id;
    peer->ep          = NULL;
    peer->rbuf_desc   = NULL;
    peer->rkey_bundle = NULL;
    peer->length      = elem_len;
    peer->offset      = 0;
    peer->flag        = 0;

    ucg_planc_stars_context_t *context = op->stars_group->context;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t ctx_rank = ucg_rank_map_eval(&vgroup->group->rank_map, peer_id);
    scp_ep_h cached_ep = ucg_planc_stars_get_ep_from_pool(context, ctx_rank);
    if (cached_ep != NULL) {
        peer->ep = cached_ep;
        return UCG_OK;
    }

    // When ppn equals the vgroup size, communication is intra node, else inter node.
    int inter_node_flag = (vgroup->group->topo->ppn == vgroup->size) ? 0 : 1;
    ucg_status_t status = scp_ep_create(context->scp_worker, &peer->ep, inter_node_flag);
    UCG_ASSERT_CODE_RET(status);

    return ucg_planc_stars_put_ep_to_pool(context, ctx_rank, peer->ep);
}

ucg_status_t ucg_planc_stars_fill_ofd_put_req_elem(uint32_t eid_idx, stars_rank_info_h peer_rank,
                                                   scp_ofd_req_elem_h request)
{
    scp_ep_h scp_ep    = peer_rank->ep;
    ucg_status_t status = UCG_OK;

    void *rbuf_desc_ptr   = (void *)peer_rank->rbuf_desc;
    request->rkey_bundle = peer_rank->rkey_bundle;
    request->ep = scp_ep;

    stars_event_elem_h elem = rbuf_desc_ptr + BUF_DESC_HEAD_SIZE;
    request->scp_event = &elem[eid_idx].event;

    UCG_ASSERT_RET(request->scp_event != NULL, UCG_ERR_INVALID_ADDR);
    return status;
}

ucg_status_t ucg_planc_stars_fill_ofd_wait_req_elem(uint32_t eid_idx,
                                                    stars_rank_info_h peer,
                                                    scp_ofd_req_elem_h request,
                                                    stars_event_elem_h event_elem)
{
    request->type = OFFLOAD_WAIT;
    request->ep   = peer->ep;

    /* parse only once later */
    uint32_t elem_offset = peer->offset;
    stars_event_elem_h elem = &event_elem[elem_offset + eid_idx];
    request->scp_event = &elem->event;

    UCG_ASSERT_RET(request->scp_event != NULL, UCG_ERR_INVALID_ADDR);
    return UCG_OK;
}

ucg_status_t ucg_planc_stars_fill_ofd_barrier_req_elem(stars_rank_info_h peer, scp_ofd_req_elem_h request)
{
    request->type = OFFLOAD_BARRIER;
    request->ep   = peer->ep;
    request->scp_event = peer->barrier_event;
    UCG_ASSERT_RET(request->scp_event != NULL, UCG_ERR_INVALID_ADDR);
    return UCG_OK;
}