/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_BUILTIN_PLAN_H
#define UCG_BUILTIN_PLAN_H

#include <ucg/base/ucg_component.h>
#include <ucs/datastruct/mpool.inl>
#include <uct/api/uct.h>

typedef struct ucg_builtin_config ucg_builtin_config_t;
typedef struct ucg_builtin_planner_ctx ucg_builtin_planner_ctx_t;

enum UCS_S_PACKED ucg_builtin_algorithm_feature {
    UCG_ALGORITHM_SUPPORT_COMMON_FEATURE        = UCS_BIT(0),   /* support common feature */
    UCG_ALGORITHM_SUPPORT_UNBALANCE_PPN         = UCS_BIT(1),   /* support unbalanced ppn */
    UCG_ALGORITHM_SUPPORT_DISCONTINOUS_RANK     = UCS_BIT(2),   /* suport discontinuous rank */
    UCG_ALGORITHM_SUPPORT_NON_COMMUTATIVE_OPS   = UCS_BIT(3),   /* support non-commutative operation (e.g. matrix muliplication) */
    UCG_ALGORITHM_SUPPORT_LARGE_DATATYPE        = UCS_BIT(4),    /* support large datatype */
    UCG_ALGORITHM_SUPPORT_BIND_TO_NONE          = UCS_BIT(5),    /* suport bind-to none */
};

/************** Algorithm selection related varibales **************/
struct ucg_builtin_algorithm {
    unsigned bmtree;     /* bmtree     0: builtin tree    1: binomial tree        */
    unsigned kmtree;     /* kmtree for inter communication     0: buildin tree    1: k-momial tree        */
    unsigned kmtree_intra; /* kmtree for intra communication     0: buildin tree    1: k-momial tree        */
    unsigned recursive;  /* recursive  0: recursive       1: topo-aware recursive */
    unsigned bruck;      /* recursive  0: recursive       1: allgather bruck */
    unsigned topo;       /* topo       0: standard tree   1: topo-aware tree */
    enum ucg_group_hierarchy_level topo_level;
    /* topo_level =                                      */
    /* UCG_GROUP_HIERARCHY_LEVEL_NODE:     node-aware    */
    /* UCG_GROUP_HIERARCHY_LEVEL_SOCKET:   socket-aware  */
    /* UCG_GROUP_HIERARCHY_LEVEL_L3CACHE:  L3cache-aware */
    unsigned ring;       /* ring       0: recursive       1: ring */
    unsigned pipeline;   /* pipeline   0: normal send     1: pipelining send for waypoint */
    uint8_t  feature_flag; /* @ref enum ucg_builtin_algorithm_feature */
};

extern struct ucg_builtin_algorithm ucg_builtin_algo_config;

enum choose_ops_mask {
    OPS_AUTO_DECISION,
    OPS_BCAST,
    OPS_ALLREDUCE,
    OPS_BARRIER
};

enum ucg_change_algo {
    NONE_CASE = 0,
    UNSUPPORT_CASE = 1,
    NONCOMMUTATIVE_LARGEDATA_CASE = 2,
};

/************** Algorithm selection related varibales **************/
enum ucg_builtin_plan_topology_type {
    UCG_PLAN_RECURSIVE,
    UCG_PLAN_TREE_FANIN,
    UCG_PLAN_TREE_FANOUT,
    UCG_PLAN_TREE_FANIN_FANOUT,
    UCG_PLAN_ALLTOALL_AGGREGATION,
    UCG_PLAN_ALLTOALL_BRCUK,
    UCG_PLAN_BRUCK,
    UCG_PLAN_LAST,
    UCG_PLAN_RING,
};

enum UCS_S_PACKED ucg_builtin_plan_method_type {
    UCG_PLAN_METHOD_SEND_TERMINAL,     /* Send the message(s), nothing fancy */
    UCG_PLAN_METHOD_RECV_TERMINAL,     /* Final stop for incoming messages */
    UCG_PLAN_METHOD_BCAST_WAYPOINT,    /* receive and send on to all peers */
    UCG_PLAN_METHOD_GATHER_WAYPOINT,   /* gather from all peers, and pass on */
    UCG_PLAN_METHOD_SCATTER_TERMINAL,  /* scatter to all peers in the map */
    UCG_PLAN_METHOD_SCATTER_WAYPOINT,  /* scatter and send "downwards" */
    UCG_PLAN_METHOD_REDUCE_TERMINAL,   /* receive and reduce from each peer */
    UCG_PLAN_METHOD_REDUCE_WAYPOINT,   /* receive, reduce, and pass onwards */
    UCG_PLAN_METHOD_REDUCE_RECURSIVE,  /* send+receive and reduce (RD) */
    UCG_PLAN_METHOD_NEIGHBOR,          /* "halo exchange", for neighborhood ops */

    UCG_PLAN_METHOD_ALLGATHER_BRUCK,   /* send+receive for allgather  (BRUCK) */
    UCG_PLAN_METHOD_ALLGATHER_RECURSIVE,
    UCG_PLAN_METHOD_ALLTOALL_BRUCK,    /* send+receive for alltoall   (BRUCK) */
    UCG_PLAN_METHOD_REDUCE_SCATTER_RING,
    UCG_PLAN_METHOD_ALLGATHER_RING,
};

enum ucg_builtin_bcast_algorithm {
    UCG_ALGORITHM_BCAST_AUTO_DECISION                = 0,
    UCG_ALGORITHM_BCAST_BMTREE                       = 1, /* Binomial tree */
    UCG_ALGORITHM_BCAST_NODE_AWARE_BMTREE            = 2, /* Topo-aware tree (Binomial tree + Binomial tree) */
    UCG_ALGORITHM_BCAST_NODE_AWARE_KMTREE_AND_BMTREE = 3, /* Topo-aware tree (K-nomial tree + Binomial tree) */
    UCG_ALGORITHM_BCAST_NODE_AWARE_KMTREE            = 4, /* Topo-aware tree (K-nomial tree + K-nomial tree) */
    UCG_ALGORITHM_BCAST_LAST,
};

enum ucg_builtin_allreduce_algorithm {
    UCG_ALGORITHM_ALLREDUCE_AUTO_DECISION                      = 0,
    UCG_ALGORITHM_ALLREDUCE_RECURSIVE                          = 1, /* Recursive */
    UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_RECURSIVE_AND_BMTREE    = 2, /* Topo-aware Recursive (ppn inside node) */
    UCG_ALGORITHM_ALLREDUCE_SOCKET_AWARE_RECURSIVE_AND_BMTREE  = 3, /* Topo-aware Recursive (ppn inside socket) */
    UCG_ALGORITHM_ALLREDUCE_RING                               = 4, /* Ring */
    UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_RECURSIVE_AND_KMTREE    = 5, /* Topo-aware Recursive (with K-nomial tree for intra node) */
    UCG_ALGORITHM_ALLREDUCE_SOCKET_AWARE_RECURSIVE_AND_KMTREE  = 6, /* Topo-aware Recursive (with K-nomial tree for intra node, ppn inside socket) */
    UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_KMTREE                  = 7, /* Topo-aware FANIN-FANOUT (with K-nomial tree for intra node, ppn inside node) */
    UCG_ALGORITHM_ALLREDUCE_SOCKET_AWARE_KMTREE                = 8, /* Topo-aware FANIN-FANOUT (with K-nomial tree for intra node, ppn inside socket) */
    UCG_ALGORITHM_ALLREDUCE_LAST,
};

enum ucg_builtin_barrier_algorithm {
    UCG_ALGORITHM_BARRIER_AUTO_DECISION                      = 0,
    UCG_ALGORITHM_BARRIER_RECURSIVE                          = 1, /* Recursive */
    UCG_ALGORITHM_BARRIER_NODE_AWARE_RECURSIVE_AND_BMTREE    = 2, /* Topo-aware Recursive (ppn inside node) */
    UCG_ALGORITHM_BARRIER_SOCKET_AWARE_RECURSIVE_AND_BMTREE  = 3, /* Topo-aware Recursive (ppn inside socket) */
    UCG_ALGORITHM_BARRIER_NODE_AWARE_RECURSIVE_AND_KMTREE    = 4, /* Topo-aware Recursive (with K-nomial tree for intra node) */
    UCG_ALGORITHM_BARRIER_SOCKET_AWARE_RECURSIVE_AND_KMTREE  = 5, /* Topo-aware Recursive (with K-nomial tree for intra node, ppn inside socket) */
    UCG_ALGORITHM_BARRIER_NODE_AWARE_KMTREE                  = 6, /* Topo-aware FANIN-FANOUT (with K-nomial tree for intra node, ppn inside node) */
    UCG_ALGORITHM_BARRIER_SOCKET_AWARE_KMTREE                = 7, /* Topo-aware FANIN-FANOUT (with K-nomial tree for intra node, ppn inside socket) */
    UCG_ALGORITHM_BARRIER_LAST,
};

typedef struct ucg_builtin_tl_threshold {
    int                               initialized;
    size_t                            max_short_one; /* max single short message */
    size_t                            max_short_max; /* max length to use short */
    size_t                            max_bcopy_one; /* max single bcopy message */
    size_t                            max_bcopy_max; /* max length to use bcopy */
    size_t                            max_zcopy_one; /* max single zcopy message */
    size_t                            md_attr_cap_max_reg;
} ucg_builtin_tl_threshold_t;

typedef struct ucg_builtin_plan_phase {
    /* Parameters for buffer send/recv action */
    union {
        uct_ep_h                     *multi_eps;     /* endpoint pointer array */
        uct_ep_h                      single_ep;     /* single endpoint handle */
    };
    uint32_t                          ep_cnt;        /* Number of endpoints (below) */
    enum ucg_builtin_plan_method_type method;        /* how to apply this map */
    ucg_step_idx_ext_t                step_index;    /* determines step index */

    ucg_builtin_tl_threshold_t          send_thresh;   /* threshold for sender */
    ucg_builtin_tl_threshold_t          recv_thresh;   /* threshold for receiver */

    uct_md_h                          md;            /* memory (registration) domain */
    const uct_md_attr_t              *md_attr;       /* memory domain attributes */
    const uct_iface_attr_t           *ep_attr;       /* endpoint attributes */

    /* flag for swap recv buffer and data when op is non commutative */
    unsigned                          is_swap;
    int                               segmented;     /* 1: message to receive is segmented;0: message to receive is not segmented. */
    int8_t                           *recv_cache_buffer; /* temp buffer to receive segmented messages. */

    ucp_ep_h                         *ucp_eps;       /* ucp_ep related with this phase(used for release) */

#if ENABLE_DEBUG_DATA
    ucg_group_member_index_t         *indexes;       /* array corresponding to EPs */
#endif
} ucg_builtin_plan_phase_t;

typedef struct ucg_builtin_plan {
    ucg_plan_t                 super;
    void                      *slots;   /* slots for builtin operations */
    ucs_list_link_t           *resend;  /* per-group list of requests to resend */
    ucs_list_link_t            list;    /* member of a per-group list of plans */
    ucs_list_link_t            by_root; /* extra phases for non-zero root */
    ucs_mpool_t                op_mp;   /* memory pool for (builtin_)operations */
    ucg_step_idx_ext_t         phs_cnt; /* number of phases in the normal flow */
    ucg_step_idx_ext_t         step_cnt; /* number of steps in the normal flow */
    ucg_step_idx_ext_t         ep_cnt;  /* total endpoint count */
    uint16_t                   am_id;   /* active message ID */
    size_t                     non_power_of_two; /* number of processes is power of two or not */
    uint64_t                   feature; /* enum ucg_builtin_algorithm_feature */
    ucg_builtin_planner_ctx_t *context;
    
    ucg_builtin_plan_phase_t phss[];  /* topology's phases */
/*  uct_ep_h                 eps[];    * logically located here */
} ucg_builtin_plan_t;

#define UCG_BUILTIN_CONNECT_SINGLE_EP ((unsigned)-1)
ucs_status_t ucg_builtin_connect(ucg_builtin_planner_ctx_t *ctx,
                                 ucg_group_member_index_t idx, ucg_builtin_plan_phase_t *phase,
                                 unsigned phase_ep_index);



typedef struct ucg_builtin_binomial_tree_config {
    unsigned degree_inter_fanout;
    unsigned degree_inter_fanin;
    unsigned degree_intra_fanout;
    unsigned degree_intra_fanin;
} ucg_builtin_binomial_tree_config_t;
extern ucs_config_field_t ucg_builtin_binomial_tree_config_table[];
ucs_status_t ucg_builtin_binomial_tree_create(ucg_builtin_planner_ctx_t *ctx,
                                              enum ucg_builtin_plan_topology_type plan_topo_type,
                                              const ucg_builtin_config_t *config,
                                              const ucg_group_params_t *group_params,
                                              const ucg_collective_type_t *coll_type,
                                              ucg_builtin_plan_t **plan_p);

typedef struct ucg_builtin_recursive_config {
    unsigned factor;
} ucg_builtin_recursive_config_t;

ucs_status_t ucg_builtin_recursive_create(ucg_builtin_planner_ctx_t *ctx,
                                          enum ucg_builtin_plan_topology_type plan_topo_type,
                                          const ucg_builtin_config_t *config,
                                          const ucg_group_params_t *group_params,
                                          const ucg_collective_type_t *coll_type,
                                          ucg_builtin_plan_t **plan_p);

ucs_status_t ucg_builtin_recursive_connect(ucg_builtin_planner_ctx_t *ctx,
                                           ucg_group_member_index_t my_rank,
                                           ucg_group_member_index_t* member_list,
                                           ucg_group_member_index_t member_cnt,
                                           unsigned factor,
                                           unsigned check_swap,
                                           ucg_builtin_plan_t *recursive);

ucs_status_t ucg_builtin_recursive_compute_steps(ucg_group_member_index_t my_index_local,
                                                 unsigned rank_count, unsigned factor, unsigned *steps);


typedef struct ucg_builtin_bruck_config {
    unsigned factor;
} ucg_builtin_bruck_config_t;

typedef struct ucg_builtin_ring_config {
    unsigned factor;
} ucg_builtin_ring_config_t;

ucs_status_t ucg_builtin_ring_create(ucg_builtin_planner_ctx_t *ctx,
                                     enum ucg_builtin_plan_topology_type plan_topo_type,
                                     const ucg_builtin_config_t *config,
                                     const ucg_group_params_t *group_params,
                                     const ucg_collective_type_t *coll_type,
                                     ucg_builtin_plan_t **plan_p);

ucs_status_t ucg_topo_neighbor_create(ucg_builtin_planner_ctx_t *ctx,
                                      enum ucg_builtin_plan_topology_type plan_topo_type,
                                      const ucg_builtin_config_t *config,
                                      const ucg_group_params_t *group_params,
                                      const ucg_collective_type_t *coll_type,
                                      ucg_builtin_plan_t **plan_p);

ucs_status_t choose_distance_from_topo_aware_level(enum ucg_group_member_distance *domain_distance);

/***************************** Topology information *****************************/
typedef struct ucg_builtin_topology_info_params {
    unsigned ppn_cnt;
    unsigned node_cnt;
    ucg_group_member_index_t *rank_same_node;
    ucg_group_member_index_t *subroot_array;
} ucg_builtin_topology_info_params_t;

ucs_status_t ucg_builtin_topology_info_create(ucg_builtin_topology_info_params_t *topo_params,
                                              const ucg_group_params_t *group_params,
                                              ucg_group_member_index_t root);

ucs_status_t ucg_builtin_am_handler(void *arg, void *data, size_t length, unsigned am_flags);

void ucg_builtin_msg_dump(void *arg, uct_am_trace_type_t type,
                          uint8_t id, const void *data, size_t length,
                          char *buffer, size_t max);

ucs_status_t ucg_builtin_bcast_algo_switch(const enum ucg_builtin_bcast_algorithm bcast_algo_decision, struct ucg_builtin_algorithm *algo);

ucs_status_t ucg_builtin_barrier_algo_switch(const enum ucg_builtin_barrier_algorithm barrier_algo_decision, struct ucg_builtin_algorithm *algo);

ucs_status_t ucg_builtin_allreduce_algo_switch(const enum ucg_builtin_allreduce_algorithm allreduce_algo_decision, struct ucg_builtin_algorithm *algo);

ucs_status_t ucg_builtin_check_ppn(const ucg_group_params_t *group_params,
                                   unsigned *unequal_ppn);

ucs_status_t ucg_builtin_find_myself(const ucg_group_params_t *group_params,
                                     ucg_group_member_index_t *myrank);
 
ucs_status_t ucg_builtin_check_continuous_number(const ucg_group_params_t *group_params,
                                                 enum ucg_group_member_distance domain_distance,
                                                 unsigned *discont_flag);

void ucg_builtin_plan_decision_in_discontinuous_case(const size_t msg_size,
                                                     const ucg_group_params_t *group_params,
                                                     const enum ucg_collective_modifiers modifiers,
                                                     const ucg_collective_params_t *coll_params);

void plan_decision_fixed(const size_t msg_size,
                         const ucg_group_params_t *group_params,
                         const enum ucg_collective_modifiers modifiers,
                         const ucg_collective_params_t *coll_params,
                         const unsigned large_datatype_threshold,
                         const int is_unbalanced_ppn,
                         enum ucg_builtin_bcast_algorithm *bcast_algo_decision,
                         enum ucg_builtin_allreduce_algorithm *allreduce_algo_decision,
                         enum ucg_builtin_barrier_algorithm *barrier_algo_decision);

enum choose_ops_mask ucg_builtin_plan_choose_ops(ucg_builtin_config_t *config, enum ucg_collective_modifiers ops_type_choose);

ucs_status_t ucg_builtin_algorithm_decision(const ucg_builtin_planner_ctx_t *ctx,
                                            const ucg_collective_params_t *coll_params);

unsigned ucg_builtin_calculate_ppx(const ucg_group_params_t *group_params,
                                   enum ucg_group_member_distance domain_distance);

#endif
