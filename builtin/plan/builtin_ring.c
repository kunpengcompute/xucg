/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <string.h>
#include <ucs/debug/log.h>
#include <ucs/debug/memtrack.h>
#include <uct/api/uct_def.h>

#include "builtin_plan.h"

#define INDEX_DOUBLE 2

void ucg_builtin_ring_assign_recv_thresh(ucg_builtin_plan_phase_t *phase)
{
    phase->recv_thresh.max_short_one = phase->send_thresh.max_short_one;
    phase->recv_thresh.max_short_max = phase->send_thresh.max_short_max;
    phase->recv_thresh.max_bcopy_one = phase->send_thresh.max_bcopy_one;
    phase->recv_thresh.max_bcopy_max = phase->send_thresh.max_bcopy_max;
    phase->recv_thresh.max_zcopy_one = phase->send_thresh.max_zcopy_one;
    if (phase->md_attr != NULL) {
        phase->recv_thresh.md_attr_cap_max_reg = phase->md_attr->cap.max_reg;
    }
}

ucs_status_t ucg_builtin_ring_connect(ucg_builtin_group_ctx_t *ctx,
                                      ucg_builtin_plan_phase_t *phase,
                                      ucg_step_idx_ext_t step_idx,
                                      ucg_group_member_index_t peer_index_src,
                                      ucg_group_member_index_t peer_index_dst,
                                      int is_mock,
                                      ucg_builtin_plan_t *ring)
{
    ucs_status_t status;
    uct_ep_h *next_ep = (uct_ep_h*)(phase + step_idx);
    if (peer_index_src != peer_index_dst) {
        phase->ep_cnt = 1; /* 1 sender and 1 receiver ,the phase->ep_cnt is the number of receiver */
        unsigned phase_ep_index = 1; /* index: 0 for sender and 1 for receiver */
        phase->multi_eps = next_ep++;

        /* connected to src process for second EP, recv */
        status = ucg_builtin_connect(ctx, peer_index_src, phase, phase_ep_index, 0, is_mock);
        if (status != UCS_OK) {
            return status;
        }
        phase_ep_index--;
        next_ep++;

        /* set threshold for receiver
        * for ring threshold for receiver and sender maybe not same!!!
        */
        ucg_builtin_ring_assign_recv_thresh(phase);

        /* connected to dst process for first EP, send */
        status = ucg_builtin_connect(ctx, peer_index_dst, phase, phase_ep_index, 0, is_mock);
        if (status != UCS_OK) {
            return status;
        }

        /*
        * while phase->ep_cnt is set to be 1. So phase->single_ep should
        * point to multi_eps[0].
        */
        phase->single_ep = phase->multi_eps[0];
    } else {
        phase->ep_cnt  = 1;
        ring->ep_cnt -= 1;
        phase->multi_eps = next_ep++;
        status = ucg_builtin_connect(ctx, peer_index_src, phase, UCG_BUILTIN_CONNECT_SINGLE_EP, 0, is_mock);
        if (status != UCS_OK) {
            return status;
        }
        /* set threshold for receiver
            * for ring threshold for receiver and sender maybe not same!!!
            */
        ucg_builtin_ring_assign_recv_thresh(phase);
    }

    return status;
}

void ucg_builtin_ring_find_my_index(const ucg_group_params_t *group_params, unsigned proc_count, ucg_group_member_index_t *my_index)
{
    while ((*my_index < proc_count) &&
           (group_params->distance[*my_index] !=
           UCG_GROUP_MEMBER_DISTANCE_NONE)) {
        (*my_index)++;
    }
    ucs_assert(*my_index != proc_count);
}
ucs_status_t ucg_builtin_ring_create(ucg_builtin_group_ctx_t *ctx,
                                     enum ucg_builtin_plan_topology_type plan_topo_type,
                                     const ucg_builtin_config_t *config,
                                     const ucg_group_params_t *group_params,
                                     const ucg_collective_type_t *coll_type,
                                     ucg_builtin_plan_t **plan_p)
{
    int is_mock = coll_type->modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_MOCK_EPS;

    /* only phase0  need to call builtin_connect */
    ucg_builtin_plan_phase_t  phase_zero;

    /* the number of ring steps is proc_count-1,and the step_size is always 1 */
    unsigned proc_count = group_params->member_count;
    /* the step number of reduce is proc_count-1, and the step number of allgather is proc_count-1 */
    ucg_step_idx_ext_t step_idx = INDEX_DOUBLE * (proc_count - 1);

    /* Allocate memory resources */
    /* when proc_count >2, the number of endpoints is 2, and when proc_count = 2, the number of endpoints is 1. */
    size_t alloc_size =            sizeof(ucg_builtin_plan_t) +
                                   (step_idx * sizeof(ucg_builtin_plan_phase_t)
                                    + (INDEX_DOUBLE * step_idx * sizeof(uct_ep_h)));

    ucg_builtin_plan_t *ring       = (ucg_builtin_plan_t*)UCS_ALLOC_CHECK(alloc_size, "ring topology");
    memset(ring, 0, alloc_size);
    ucg_builtin_plan_phase_t *phase = &ring->phss[0];
    ring->ep_cnt                   = step_idx * INDEX_DOUBLE;  /* the number of endpoints each step is always 2 for ring */
    ring->phs_cnt                  = step_idx;

    /* Find my own index */
    ucg_group_member_index_t my_index = 0;
    ucg_builtin_ring_find_my_index(group_params, proc_count, &my_index);

    ucs_status_t status;
    /* builtin phase 0 */
    phase->method = UCG_PLAN_METHOD_REDUCE_SCATTER_RING;

    phase->step_index = 0;

    /* In each step, there are two peers */
    ucg_group_member_index_t peer_index_src;
    ucg_group_member_index_t peer_index_dst; /* src: source,  dst: destination */
    peer_index_src = (my_index - 1 + proc_count) % proc_count;
    peer_index_dst = (my_index + 1) % proc_count;

#if ENABLE_DEBUG_DATA
    phase->indexes     = UCS_ALLOC_CHECK((peer_index_src == peer_index_dst ? 1 : INDEX_DOUBLE) * sizeof(my_index),
                                         "ring indexes");
#endif

    ucs_info("%u's peer #%u(source) and #%u(destination) at (step #%u/%u)", my_index, (unsigned)peer_index_src,
             (unsigned)peer_index_dst, (unsigned)step_idx + 1, ring->phs_cnt);

    status = ucg_builtin_ring_connect(ctx, phase, step_idx, peer_index_src, peer_index_dst, is_mock, ring);
    if (status != UCS_OK) {
        ucs_free(ring);
        ring = NULL;
        ucs_error("Error in ring create: %d", (int)status);
        return status;
    }
    phase_zero = *phase;
    phase++;

    for (step_idx = 1; step_idx < ring->phs_cnt; step_idx++, phase++) {
        /* the following endpoint is as same as phase(0) */
        *phase = phase_zero;
        phase->ucp_eps = NULL;

        /* modify method and step_index in phase */
        if (step_idx < proc_count - 1) {
            phase->method = UCG_PLAN_METHOD_REDUCE_SCATTER_RING;
        } else {
            phase->method = UCG_PLAN_METHOD_ALLGATHER_RING;
        }

        phase->step_index = step_idx;
        ucs_info("%u's peer #%u(source) and #%u(destination) at (step #%u/%u)", my_index, (unsigned)peer_index_src,
                 (unsigned)peer_index_dst, (unsigned)(phase->step_index) + 1, ring->phs_cnt);
    }

    ring->super.my_index = my_index;
    ring->super.support_non_commutative = 0;
    ring->super.support_large_datatype = 1;
    *plan_p = ring;
    return status;
}
