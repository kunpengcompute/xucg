/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_TOPO_H_
#define UCG_TOPO_H_

#include "ucg/api/ucg.h"

#include "ucg_vgroup.h"

/**
 * @brief vgroup rank of leader in the topo-group
 *
 * The first member added to the group is regarded as the leader. Therefore,
 * the vgroup rank of the leader in a topo-group is always 0.
 * For example, in a node group, the process whose vgroup rank is 0 will become
 * the member of the node leader group.
 */
#define UCG_TOPO_GROUP_LEADER 0

#define UCG_TOPO_PPX_UNBALANCED -1

/**
 * @brief Get location.
 *
 * @param [in]  group       UCG group.
 * @param [in]  rank        Group rank.
 * @param [out] location    Location of rank.
 */
typedef ucg_status_t (*ucg_topo_get_location_cb_t)(ucg_group_t *group,
                                                   ucg_rank_t rank,
                                                   ucg_location_t *location);

typedef enum ucg_topo_group_type {
    UCG_TOPO_GROUP_TYPE_NET, /**< Consist of all processes. */
    UCG_TOPO_GROUP_TYPE_SUBNET, /**< Consist of the processes in same subnet. */
    UCG_TOPO_GROUP_TYPE_SUBNET_LEADER, /**< Consist of the leader process of all subnet groups. */
    UCG_TOPO_GROUP_TYPE_NODE, /**< Consist of the processes in same node. */
    UCG_TOPO_GROUP_TYPE_NODE_LEADER, /**< Consist of the leader process of all node groups. */
    UCG_TOPO_GROUP_TYPE_SOCKET, /**< Consist of the processes in same socket. */
    UCG_TOPO_GROUP_TYPE_SOCKET_LEADER, /**< Consist of the leader process of all socket groups. */
    UCG_TOPO_GROUP_TYPE_LAST
} ucg_topo_group_type_t;

typedef enum ucg_topo_group_state {
    UCG_TOPO_GROUP_STATE_NOT_INIT, /** Not initialize. */
    UCG_TOPO_GROUP_STATE_ERROR, /** Error occurred during group creation. */
    UCG_TOPO_GROUP_STATE_ENABLE, /** I'm the member of the group. */
    UCG_TOPO_GROUP_STATE_DISABLE, /** I'm not the member of the group. */
    UCG_TOPO_GROUP_STATE_LAST
} ucg_topo_group_state_t;

typedef struct ucg_topo_group {
    ucg_vgroup_t super;
    ucg_topo_group_state_t state;
} ucg_topo_group_t;

typedef struct ucg_topo_params {
    /** Original group of all topo groups. */
    ucg_group_t *group;
    /** My rank in the group. */
    ucg_rank_t myrank;
    /** Convert group rank to context rank. */
    const ucg_rank_map_t *rank_map;
    /** Get location callback */
    ucg_topo_get_location_cb_t get_location;
} ucg_topo_params_t;

typedef struct ucg_topo_location {
    int32_t node_id;
    int32_t socket_id;
} ucg_topo_location_t;

typedef struct ucg_topo_detail {
    int32_t nnode;
    int32_t nsocket;
    /* The length of the locations array is determined by @ref ucg_group_t::size */
    ucg_topo_location_t *locations;
} ucg_topo_detail_t;

/**
 * @brief the topology of processes
 */
typedef struct ucg_topo {
    /** Cached groups. */
    ucg_topo_group_t groups[UCG_TOPO_GROUP_TYPE_LAST];
    /** Owner group. */
    ucg_group_t *group;
    /** My rank in the group. */
    ucg_rank_t myrank;
    /** Convert group rank to context rank. */
    ucg_rank_map_t rank_map;
    /** Get location callback. */
    ucg_topo_get_location_cb_t get_location;
    /** My location. */
    ucg_location_t myloc;
    /** Detail topo info. */
    ucg_topo_detail_t detail;
    /** Processes per node. */
    int32_t ppn;
    /** Processes per socket. */
    int32_t pps;
} ucg_topo_t;

/**
 * @brief Auxiliary for creating groups
 */
typedef struct ucg_topo_group_aux {
    /** Initialize auxiliary */
    ucg_status_t (*init)(void **aux);
    /** Cleanup auxiliary */
    void (*cleanup)(void **aux);
    /** Check whether the prerequisites for creating the group are met. */
    ucg_status_t (*check)(void **aux, const ucg_location_t *location);
    /** Check whether the rank is the member of the group. */
    int32_t (*is_member)(void **aux, const ucg_topo_t *topo,
                         const ucg_rank_map_t *rank_map, ucg_rank_t rank,
                         const ucg_location_t *location);
    /** Let auxiliary know the members. */
    ucg_status_t (*add_member)(void **aux, const ucg_topo_t *topo,
                               const ucg_rank_map_t *rank_map, ucg_rank_t rank,
                               const ucg_location_t *location);
} ucg_topo_group_aux_t;

/**
 * @brief Initialize topology.
 *
 * The members of this topology are the processes with the context ranks which are
 * obtained through the specified rank-map.
 *
 * @param [in]  params          Parameters
 * @param [out] topo            Initialized topology
 * @return ucg_status_t
 */
ucg_status_t ucg_topo_init(const ucg_topo_params_t *params, ucg_topo_t **topo);

/**
 * @brief Cleanup topology.
 */
void ucg_topo_cleanup(ucg_topo_t *topo);

/**
 * @brief Get a specific type of topology group.
 *
 * @param [in] topo             Topology
 * @param [in] type             Topo group type
 * @return NULL if some error happened
 *
 * @note Caller needs to check whether the group is enabled.
 */
ucg_topo_group_t* ucg_topo_get_group(ucg_topo_t *topo, ucg_topo_group_type_t type);

#endif