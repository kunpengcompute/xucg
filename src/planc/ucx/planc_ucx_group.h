/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_UCX_GROUP_H_
#define UCG_PLANC_UCX_GROUP_H_

#include "planc_ucx_context.h"
#include "planc/ucg_planc.h"

typedef enum ucg_planc_ucx_algo_group_type {
    UCG_ALGO_GROUP_TYPE_NODE_LEADER, /**< Offset node_leader group to which myrank belongs. */
    UCG_ALGO_GROUP_TYPE_SOCKET_LEADER, /**< Offset socket_leader group to which myrank belongs. */
    UCG_ALGO_GROUP_TYPE_LAST
} ucg_planc_ucx_algo_group_type_t;

typedef enum ucg_planc_ucx_algo_group_state {
    UCG_ALGO_GROUP_STATE_NOT_INIT, /** Not initialize. */
    UCG_ALGO_GROUP_STATE_ERROR, /** Error occurred during group creation. */
    UCG_ALGO_GROUP_STATE_ENABLE, /** I'm the member of the group. */
    UCG_ALGO_GROUP_STATE_DISABLE, /** I'm not the member of the group. */
    UCG_ALGO_GROUP_STATE_LAST
} ucg_planc_ucx_algo_group_state_t;

typedef struct ucg_planc_ucx_algo_group {
    ucg_vgroup_t super;
    ucg_planc_ucx_algo_group_state_t state;
} ucg_planc_ucx_algo_group_t;

typedef struct ucg_planc_ucx_group {
    ucg_planc_group_t super;
    ucg_planc_ucx_context_t *context;

    /* cached groups */
    ucg_planc_ucx_algo_group_t groups[UCG_ALGO_GROUP_TYPE_LAST];
} ucg_planc_ucx_group_t;

ucg_status_t ucg_planc_ucx_group_create(ucg_planc_context_h context,
                                        const ucg_planc_group_params_t *params,
                                        ucg_planc_group_h *planc_group);
void ucg_planc_ucx_group_destroy(ucg_planc_group_h planc_group);

ucg_status_t ucg_planc_ucx_create_node_leader_algo_group(ucg_planc_ucx_group_t *ucx_group,
                                                         ucg_vgroup_t *vgroup);
ucg_status_t ucg_planc_ucx_create_socket_leader_algo_group(ucg_planc_ucx_group_t *ucx_group,
                                                           ucg_vgroup_t *vgroup);

#endif