/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2021.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_BUILTIN_PLAN_H
#define UCG_BUILTIN_PLAN_H

#include <ucg/api/ucg_plan_component.h>
#include <ucs/datastruct/mpool.inl>
#include <uct/api/uct.h>
#include <ucg/builtin/plan/builtin_topo.h>
#include <ucg/builtin/plan/builtin_algo_decision.h>

BEGIN_C_DECLS

#ifndef ENABLE_GTEST
    #define STATIC_GTEST static
#else
    #define STATIC_GTEST
#endif

#ifndef MPI_IN_PLACE
#define MPI_IN_PLACE ((void *)0x1)
#endif

enum UCS_S_PACKED ucg_builtin_algorithm_feature {
    UCG_ALGORITHM_SUPPORT_COMMON_FEATURE        = UCS_BIT(0),   /* support common feature */
    UCG_ALGORITHM_SUPPORT_UNBALANCE_PPN         = UCS_BIT(1),   /* support unbalanced ppn */
    UCG_ALGORITHM_SUPPORT_DISCONTINUOUS_RANK    = UCS_BIT(2),   /* suport discontinuous rank */
    UCG_ALGORITHM_SUPPORT_RANK_FEATURE          = (UCS_BIT(1) | UCS_BIT(2)), /* support discontinuous rank and unbalanced ppn */
    UCG_ALGORITHM_SUPPORT_NON_COMMUTATIVE_OPS   = UCS_BIT(3),   /* support non-commutative operation (e.g. matrix multiplication) */
    UCG_ALGORITHM_SUPPORT_LARGE_DATATYPE        = UCS_BIT(4),    /* support large datatype */
    UCG_ALGORITHM_SUPPORT_ALLREDUCE_RARE_FEATURE = (UCS_BIT(3) | UCS_BIT(4)), /* support non-commutative and large datatype */
    UCG_ALGORITHM_SUPPORT_BIND_TO_NONE          = UCS_BIT(5),    /* suport bind-to none */
};

/************** Algorithm selection related variables **************/
typedef struct ucg_builtin_algorithm {
    uint16_t bmtree   : 1;     /* bmtree     0: builtin tree    1: binomial tree        */
    uint16_t kmtree   : 1;     /* kmtree for inter communication    0: builtin tree    1: k-nomial tree        */
    uint16_t kmtree_intra   : 1; /* kmtree for intra communication  0: builtin tree    1: k-nomial tree        */
    uint16_t recursive   : 1;  /* recursive  0: recursive       1: topo-aware recursive */
    uint16_t bruck   : 1;      /* recursive  0: recursive       1: allgather bruck */
    uint16_t ring    : 1;       /* ring       0: recursive       1: ring */
    uint16_t NAP   : 1;       /* NAP       0: recursive      1: Node Aware Parallel */
    uint16_t pipeline   : 1;   /* pipeline   0: normal send     1: pipelining send for waypoint */
    uint16_t  inc : 1;       /*inc  0: normal algo 1:in net work computing */
    uint16_t   binary_block : 1;       /*binary block 0:false 1:yes*/
    uint16_t  ladd  : 1;       /* ladd 0:false 1:yes*/
    uint16_t   plummer : 1;       /*plummer 0:false 1:yes*/
    uint16_t topo   : 1;       /* topo       0: standard tree   1: topo-aware tree */
    /* topo_level =                                      */
    /* UCG_GROUP_HIERARCHY_LEVEL_NODE:     node-aware    */
    /* UCG_GROUP_HIERARCHY_LEVEL_SOCKET:   socket-aware  */
    /* UCG_GROUP_HIERARCHY_LEVEL_L3CACHE:  L3cache-aware */
    uint16_t   topo_level : 2;
    uint16_t   reserved : 2;
    uint8_t  feature_flag; /* @ref enum ucg_builtin_algorithm_feature */
} ucg_builtin_algo_t;

extern struct ucg_builtin_algorithm ucg_algo;

/************** Algorithm selection related variables **************/
enum ucg_builtin_plan_topology_type {
    UCG_PLAN_RECURSIVE,
    UCG_PLAN_TREE_FANIN,
    UCG_PLAN_TREE_FANOUT,
    UCG_PLAN_TREE_FANIN_FANOUT,
    UCG_PLAN_ALLTOALL_AGGREGATION,
    UCG_PLAN_ALLTOALL_BRUCK,
    UCG_PLAN_BRUCK,
    UCG_PLAN_RING,
    UCG_PLAN_INC,
    UCG_PLAN_BMTREE,
    UCG_PLAN_KMTREE,
    UCG_PLAN_NAP,
    UCG_PLAN_BINARY_BLOCK,
    UCG_PLAN_ALLTOALLV_LADD,
    UCG_PLAN_ALLTOALLV_PLUMMER,
    UCG_PLAN_LAST
};

enum ucg_builtin_plan_recursive_type {
    UCG_PLAN_RECURSIVE_TYPE_ALLREDUCE
};

/* connection pattern for different collective operations */
enum ucg_builtin_plan_connect_pattern {
    UCG_PLAN_PATTERN_ONE_TO_MANY    = UCS_BIT(0),
    UCG_PLAN_PATTERN_MANY_TO_ONE    = UCS_BIT(1),
    UCG_PLAN_PATTERN_MANY_TO_MANY   = UCS_BIT(2),
    UCG_PLAN_PATTERN_COLLECT        = UCS_BIT(3),
    UCG_PLAN_PATTERN_DISTRIBUTE     = UCS_BIT(4),
};

/* plan build type
 * FULL: all members will participate plan creation
 * PARTIAL: only partial members will create the plan (e.g. topo-aware algorithm)
 */
enum ucg_builtin_plan_build_type {
    UCG_PLAN_BUILD_FULL,
    UCG_PLAN_BUILD_PARTIAL
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
    UCG_PLAN_METHOD_REDUCE_SCATTER_RECURSIVE,  /* send + receive in half and reduce (RH) */
    UCG_PLAN_METHOD_NEIGHBOR,          /* "halo exchange", for neighborhood ops */

    UCG_PLAN_METHOD_ALLGATHER_BRUCK,   /* send+receive for allgather  (BRUCK) */
    UCG_PLAN_METHOD_ALLGATHER_RECURSIVE,
    UCG_PLAN_METHOD_ALLTOALL_BRUCK,    /* send+receive for alltoall   (BRUCK) */
    UCG_PLAN_METHOD_REDUCE_SCATTER_RING,
    UCG_PLAN_METHOD_ALLGATHER_RING,
    UCG_PLAN_METHOD_INC,
    UCG_PLAN_METHOD_EXCHANGE,          /* exchange messages between peers */
    UCG_PLAN_METHOD_ALLTOALLV_LADD,    /* alltoallv for ladd */
    UCG_PLAN_METHOD_SEND_V_TERMINAL,   /* send operation for gatherv */
    UCG_PLAN_METHOD_RECV_V_TERMINAL,   /* recv operation for scatter */
    UCG_PLAN_METHOD_SCATTER_V_TERMINAL,/* scatterv operation for fanout */
    UCG_PLAN_METHOD_GATHER_V_TERMINAL, /* gatherv operation for fanin */
    UCG_PLAN_METHOD_ALLTOALLV_PLUMMER, /* inter node alltoallv for plummer*/
};

enum ucg_builtin_bcast_algorithm {
    UCG_ALGORITHM_BCAST_AUTO_DECISION                = 0,
    UCG_ALGORITHM_BCAST_BMTREE                       = 1, /* Binomial tree */
    UCG_ALGORITHM_BCAST_NODE_AWARE_BMTREE            = 2, /* Topo-aware tree (Binomial tree + Binomial tree) */
    UCG_ALGORITHM_BCAST_NODE_AWARE_KMTREE_AND_BMTREE = 3, /* Topo-aware tree (K-nomial tree + Binomial tree) */
    UCG_ALGORITHM_BCAST_NODE_AWARE_KMTREE            = 4, /* Topo-aware tree (K-nomial tree + K-nomial tree) */
    UCG_ALGORITHM_BCAST_NODE_AWARE_INC               = 5, /* Node-aware In Network Computing (INC)*/
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
    UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_INC                     = 9,
    UCG_ALGORITHM_ALLREDUCE_SOCKET_AWARE_INC                   = 10,
    UCG_ALGORITHM_ALLREDUCE_NAP                                = 11,
    UCG_ALGORITHM_ALLREDUCE_RABENSEIFNER_BINARY_BLOCK          = 12,
    UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_RABENSEIFNER_BINARY_BLOCK = 13,
    UCG_ALGORITHM_ALLREDUCE_SOCKET_AWARE_RABENSEIFNER_BINARY_BLOCK = 14,
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
    UCG_ALGORITHM_BARRIER_NODE_AWARE_INC                     = 8,
    UCG_ALGORITHM_BARRIER_SOCKET_AWARE_INC                   = 9,
    UCG_ALGORITHM_BARRIER_NAP                                = 10,
    UCG_ALGORITHM_BARRIER_LAST,
};

enum ucg_builtin_alltoallv_algorithm {
    UCG_ALGORITHM_ALLTOALLV_AUTO_DECISION    = 0,
    UCG_ALGORITHM_ALLTOALLV_LADD             = 1,  /* Throttled scattered destination */
    UCG_ALGORITHM_ALLTOALLV_NODE_AWARE_PLUMMER = 2,  /* gatherv+alltoallv+scatterv*/
    UCG_ALGORITHM_ALLTOALLV_LAST,
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

/* special feature for some algorithms (rabenseifner, bruck),
 * e.g. first rank, start position of buffer, sending buffer period
 */
typedef struct ucg_builtin_plan_extra_attr {
    unsigned total_num_blocks;        /* total number of blocks for all phase */
    unsigned num_blocks;              /* number of blocks the buffer for current phase */
    unsigned start_block;             /* send start block for current phase */
    unsigned recv_start_block;        /* recv start block for current phase */
    unsigned peer_block;              /* start block for peer */
    unsigned peer_start_block;        /* peer's start block for current phase */
    unsigned is_partial;              /* send or recv partially */
    unsigned not_shift_send_buffer;   /* whether send_buffer is not to shift */
    unsigned member_cnt;              /* number of members for local plan */
    unsigned local_first_idx;         /* first step index  for local plan */
    unsigned is_inequal;              /* different block between local and peer side*/
    unsigned packed_rank;             /* local rank information */
    unsigned is_node_leader;          /* indicates whether the node leader is the node leader */
    unsigned is_variable_len;         /* indicates whether the length is variable */
    unsigned is_plummer;              /* indicates whether plummer algorithm */
    unsigned ppn;                     /* number of processes on a node */
} ucg_builtin_plan_extra_attr_t;
struct ucg_builtin_plan_phase;
typedef ucs_status_t (*ucg_builtin_init_phase_by_step_cb_t)(struct ucg_builtin_plan_phase *phase,
                                                            const ucg_collective_params_t *coll_params);

/* for binary block rabenseifner algorithms requires group information */
typedef struct ucg_builtin_index_group {
    ucg_group_member_index_t    my_index;
    unsigned cur_group_begin_index;
    unsigned cur_group_process_cnt;
    unsigned next_group_begin_index;
    unsigned next_group_process_cnt;
    unsigned total_group_process_cnt;
    unsigned ahead_group_cnt;
    unsigned behind_group_cnt;
    unsigned total_group_cnt;

    unsigned local_group_index;
    unsigned local_peer_ahead_group;
    unsigned recv_block_index;  /* receive from ahead group block index */
} ucg_builtin_index_group_t;

typedef struct ucg_builtin_plan_raben_phase_extend {
    unsigned step_index;   /* step index in each phase of algorithm */
    unsigned first_step_flag;
    ucg_builtin_index_group_t index_group;
} ucg_builtin_plan_raben_phase_extend_t;

typedef struct ucg_builtin_plan_phase {
    /* Parameters for buffer send/recv action */
    union {
        uct_ep_h                     *multi_eps;     /* endpoint pointer array */
        uct_ep_h                      single_ep;     /* single endpoint handle */
    };
    uint32_t                          ep_cnt;        /* Number of endpoints (below) */
    uint32_t                          send_ep_cnt;   /* Send number of endpoints (below) */
    uint32_t                          recv_ep_cnt;   /* Recv number of endpoints (below) */
    enum ucg_builtin_plan_method_type method;        /* how to apply this map */
    ucg_step_idx_ext_t                step_index;    /* determines step index */

    ucg_builtin_tl_threshold_t          send_thresh;   /* threshold for sender */
    ucg_builtin_tl_threshold_t          recv_thresh;   /* threshold for receiver */

    uct_md_h                          md;            /* memory (registration) domain */
    const uct_md_attr_t              *md_attr;       /* memory domain attributes */
    const uct_iface_attr_t           *ep_attr;       /* endpoint attributes */
    ucg_builtin_plan_extra_attr_t     ex_attr;       /* plan extra attributes */
    /* flag for swap recv buffer and data when op is non commutative */
    unsigned                          is_swap;
    int                               segmented;     /* 1: message to receive is segmented;0: message to receive is not segmented. */
    int8_t                           *recv_cache_buffer; /* temp buffer to receive segmented messages. */

    ucp_ep_h                         *ucp_eps;       /* ucp_ep related with this phase(used for release) */
    /* layout for multi_eps : s s s | r r */
    ucg_builtin_tl_threshold_t       *ep_thresh;   /* threshold for every uct_ep*/
#if ENABLE_DEBUG_DATA
    ucg_group_member_index_t         *indexes;       /* array corresponding to EPs */
#endif
    ucg_builtin_init_phase_by_step_cb_t init_phase_cb;  /* callback fun : init phase params by step */
    ucg_builtin_plan_raben_phase_extend_t raben_extend;  /*extended attribute of Rabenseifner, used by the binary block algorithm */
} ucg_builtin_plan_phase_t;

typedef struct ucg_builtin_group_ctx ucg_builtin_group_ctx_t;
typedef struct ucg_builtin_plan {
    ucg_plan_t               super;
    void                    *slots;   /* slots for builtin operations */
    ucs_list_link_t         *resend;  /* per-group list of requests to resend */
    ucs_list_link_t          list;    /* member of a per-group list of plans */
    ucs_list_link_t          by_root; /* extra phases for non-zero root */
    ucs_mpool_t              op_mp;   /* memory pool for (builtin_)operations */
    ucg_step_idx_ext_t       phs_cnt; /* number of phases in the normal flow */
    ucg_step_idx_ext_t       step_cnt; /* number of steps in the normal flow */
    ucg_step_idx_ext_t       ep_cnt;  /* total endpoint count */
    uint16_t                 am_id;   /* active message ID */
    ucg_builtin_algo_t       ucg_algo;
    dt_convert_f             convert_f; /* convert datatypes */
    dt_span_f                dtspan_f;
    ucg_builtin_plan_phase_t phss[];  /* topology's phases */
} ucg_builtin_plan_t;

#define UCG_BUILTIN_CONNECT_SINGLE_EP ((unsigned)-1)
ucs_status_t ucg_builtin_connect(ucg_builtin_group_ctx_t *ctx,
                                 ucg_group_member_index_t idx, ucg_builtin_plan_phase_t *phase,
                                 unsigned phase_ep_index);

typedef struct ucg_builtin_config ucg_builtin_config_t;

/* NAP Algorithm related functions */
typedef struct ucg_builtin_NAP_config {
    unsigned num_NAP_group;
    unsigned init_allreduce_method;
    unsigned final_allreduce_method;
} ucg_builtin_NAP_config_t;
extern ucs_config_field_t ucg_builtin_NAP_config_table[];
ucs_status_t ucg_builtin_NAP_create(ucg_builtin_group_ctx_t *ctx,
                                    enum ucg_builtin_plan_topology_type plan_topo_type,
                                    const ucg_builtin_config_t *config,
                                    const ucg_group_params_t *group_params,
                                    const ucg_collective_type_t *coll_type,
                                    ucg_builtin_plan_t **plan_p);

typedef struct ucg_builtin_binomial_tree_config {
    unsigned degree_inter_fanout;
    unsigned degree_inter_fanin;
    unsigned degree_intra_fanout;
    unsigned degree_intra_fanin;
} ucg_builtin_binomial_tree_config_t;
extern ucs_config_field_t ucg_builtin_binomial_tree_config_table[];

typedef struct ucg_builtin_binomial_tree_params {
    ucg_builtin_group_ctx_t *ctx;
    const ucg_group_params_t *group_params;
    const ucg_collective_type_t *coll_type;
    enum ucg_builtin_plan_topology_type topo_type;
    ucg_group_member_index_t root;
    int tree_degree_inter_fanout;
    int tree_degree_inter_fanin;
    int tree_degree_intra_fanout;
    int tree_degree_intra_fanin;
} ucg_builtin_binomial_tree_params_t;

ucs_status_t ucg_builtin_binomial_tree_create(ucg_builtin_group_ctx_t *ctx,
                                              enum ucg_builtin_plan_topology_type plan_topo_type,
                                              const ucg_builtin_config_t *config,
                                              const ucg_group_params_t *group_params,
                                              const ucg_collective_type_t *coll_type,
                                              ucg_builtin_plan_t **plan_p);

typedef struct ucg_builtin_recursive_config {
    unsigned factor;
} ucg_builtin_recursive_config_t;

ucs_status_t ucg_builtin_recursive_create(ucg_builtin_group_ctx_t *ctx,
                                          enum ucg_builtin_plan_topology_type plan_topo_type,
                                          const ucg_builtin_config_t *config,
                                          const ucg_group_params_t *group_params,
                                          const ucg_collective_type_t *coll_type,
                                          ucg_builtin_plan_t **plan_p);

ucs_status_t ucg_builtin_recursive_connect(ucg_builtin_group_ctx_t *ctx,
                                           ucg_group_member_index_t my_rank,
                                           ucg_group_member_index_t* member_list,
                                           ucg_group_member_index_t member_cnt,
                                           unsigned factor,
                                           unsigned check_swap,
                                           ucg_builtin_plan_t *recursive);

void ucg_builtin_recursive_compute_steps(ucg_group_member_index_t my_index_local,
                                                 unsigned rank_count, unsigned factor, unsigned *steps);

/* Binary block Algorithm related functions */
typedef struct ucg_builtin_binary_block_config {
    unsigned inter_allreduce_method;
} ucg_builtin_binary_block_config_t;

ucs_status_t ucg_builtin_binary_block_create(ucg_builtin_group_ctx_t *ctx,
                                             enum ucg_builtin_plan_topology_type plan_topo_type,
                                             const ucg_builtin_config_t *config,
                                             const ucg_group_params_t *group_params,
                                             const ucg_collective_type_t *coll_type,
                                             ucg_builtin_plan_t **plan_p);

void ucg_builtin_free(void **p);

typedef struct throttled_scatter_params {
    unsigned max_phs_cnt;
    unsigned max_ep_cnt;
    unsigned throttle_factor;
} throttled_scatter_params_t;

/* Throttled Scattered Destination Algorithm related functions */
void ucg_builtin_throttled_scatter_get_max_phase_cnt(const ucg_builtin_config_t *config,
                                                     ucg_group_member_index_t member_count,
                                                     throttled_scatter_params_t *params);

void ucg_builtin_ladd_modify_ep_thresholds(ucg_builtin_plan_phase_t *phase, unsigned phase_ep_index);

ucs_status_t ucg_builtin_throttled_scatter_build(ucg_builtin_group_ctx_t *ctx,
                                                 const throttled_scatter_params_t *ladd_params,
                                                 unsigned ppn,
                                                 enum ucg_builtin_plan_build_type plan_build_type,
                                                 ucg_group_member_index_t member_cnt,
                                                 const ucg_group_member_index_t *member_list,
                                                 const ucg_collective_params_t *coll_params,
                                                 ucg_builtin_plan_t *throttled_scatter,
                                                 uct_ep_h **next_ep);

ucs_status_t ucg_builtin_throttled_scatter_create(ucg_builtin_group_ctx_t *ctx,
                                                  enum ucg_builtin_plan_topology_type plan_topo_type,
                                                  const ucg_builtin_config_t *config,
                                                  const ucg_group_params_t *group_params,
                                                  const ucg_collective_params_t *coll_params,
                                                  ucg_builtin_plan_t **plan_p);

/* Throttled scattered destination algorithm related functions */
ucs_status_t ucg_builtin_Plummer_create(ucg_builtin_group_ctx_t *ctx,
                                        const enum ucg_builtin_plan_topology_type plan_topo_type,
                                        const ucg_builtin_config_t *config,
                                        const ucg_group_params_t *group_params,
                                        const ucg_collective_type_t *coll_type,
                                        const ucg_collective_params_t *coll_params,
                                        ucg_builtin_plan_t **plan_p);

/* configuration for tree family */
typedef struct ucg_builtin_trees_config {
    unsigned inter_tree_type;
    unsigned intra_tree_type;
    unsigned inter_degree_fanout;
    unsigned inter_degree_fanin;
    unsigned intra_degree_fanout;
    unsigned intra_degree_fanin;
} ucg_builtin_trees_config_t;
extern ucs_config_field_t ucg_builtin_trees_config_table[];

typedef struct ucg_builtin_bruck_config {
    unsigned factor;
} ucg_builtin_bruck_config_t;

typedef struct ucg_builtin_ring_config {
    unsigned factor;
} ucg_builtin_ring_config_t;

typedef struct ucg_inc_config {
    int enable;
    uint16_t comm_id_control;
    uint16_t tag;
    uint16_t tag_low32;
    uint8_t query_hop;
    uint8_t notify_hop;
    uint32_t kill_hop;
    uint32_t tag_high32;
    int max_data_size;
    int node_under_tor;
    int socket_count;
    unsigned header_under_tor;
    uint64_t job_id;
} ucg_inc_config_t;

ucs_status_t ucg_builtin_ring_create(ucg_builtin_group_ctx_t *ctx,
                                     enum ucg_builtin_plan_topology_type plan_topo_type,
                                     const ucg_builtin_config_t *config,
                                     const ucg_group_params_t *group_params,
                                     const ucg_collective_type_t *coll_type,
                                     ucg_builtin_plan_t **plan_p);

ucs_status_t ucg_topo_neighbor_create(ucg_builtin_group_ctx_t *ctx,
                                      enum ucg_builtin_plan_topology_type plan_topo_type,
                                      const ucg_builtin_config_t *config,
                                      const ucg_group_params_t *group_params,
                                      const ucg_collective_type_t *coll_type,
                                      ucg_builtin_plan_t **plan_p);

extern ucs_config_field_t ucg_inc_config_table[]; /* INC configure table */

struct ucg_builtin_config {
    ucg_plan_config_t    super;

    ucg_builtin_binomial_tree_config_t bmtree;
    ucg_builtin_recursive_config_t     recursive;
    ucg_builtin_trees_config_t     trees;
#if ENABLE_UCG_HICOLL
    ucg_inc_config_t               inc;
#endif
    ucg_builtin_NAP_config_t       NAP;
    ucg_builtin_binary_block_config_t binary_block;
    unsigned                       cache_size;
    size_t                         short_max_tx;
    size_t                         bcopy_max_tx;
    unsigned                       mem_reg_opt_cnt;
    unsigned                       large_datatype_threshold;

    unsigned                       bcopy_to_zcopy_opt;
    double                         bcast_algorithm;
    double                         allreduce_algorithm;
    double                         barrier_algorithm;
    double                         alltoallv_algorithm;
    unsigned                       pipelining;

    unsigned                       max_msg_list_size;
    unsigned                       throttle_factor;
    int                            reduce_consistency;  /* reduce operate result consistency flag, default is n */
};

void choose_distance_from_topo_aware_level(enum ucg_group_member_distance *domain_distance);

/***************************** Topology information *****************************/
typedef struct ucg_builtin_topology_info_params {
    unsigned ppn_cnt;
    unsigned node_cnt;
    ucg_group_member_index_t *rank_same_node;
    ucg_group_member_index_t *subroot_array;
} ucg_builtin_topology_info_params_t;

/* base parameters for all plans */
typedef struct ucg_builtin_base_params {
    ucg_builtin_group_ctx_t *ctx;
    const ucg_group_params_t *group_params;
    const ucg_collective_type_t *coll_type;
    enum ucg_builtin_plan_topology_type topo_type;
} ucg_builtin_base_params_t;

/* Topology-Aware Algorithm related functions */
typedef struct ucg_builtin_topo_aware_params {
    ucg_builtin_base_params_t super;
    ucg_group_member_index_t root;
    ucg_builtin_topo_params_t *topo_params;
} ucg_builtin_topo_aware_params_t;

ucs_status_t ucg_builtin_topo_aware_add_intra(ucg_builtin_plan_t *topo_aware,
                                              const ucg_builtin_config_t *config,
                                              ucg_builtin_topo_aware_params_t *params,
                                              const ucg_group_member_index_t *member_list,
                                              const ucg_group_member_index_t member_cnt,
                                              enum ucg_builtin_plan_topology_type topo_type,
                                              enum ucg_group_hierarchy_level topo_level,
                                              enum ucg_builtin_plan_connect_pattern pattern);

/*base plan build*/
ucs_status_t ucg_builtin_bmtree_build(ucg_builtin_plan_t *bmtree,
                                      ucg_builtin_base_params_t *params,
                                      const ucg_builtin_config_t *config,
                                      const ucg_group_member_index_t *member_list,
                                      const ucg_group_member_index_t member_cnt,
                                      const ucg_group_member_index_t member_root,
                                      enum ucg_builtin_plan_build_type build_type,
                                      enum ucg_builtin_plan_connect_pattern pattern);

ucs_status_t ucg_builtin_kmtree_build(ucg_builtin_plan_t *kmtree,
                                      ucg_builtin_base_params_t *params,
                                      const ucg_builtin_config_t *config,
                                      const ucg_group_member_index_t *member_list,
                                      const ucg_group_member_index_t member_cnt,
                                      const ucg_group_member_index_t member_root,
                                      const unsigned degree,
                                      enum ucg_builtin_plan_build_type build_type,
                                      enum ucg_builtin_plan_connect_pattern pattern);

ucs_status_t ucg_builtin_recursive_build(ucg_builtin_plan_t *recursive,
                                         ucg_builtin_group_ctx_t *ctx,
                                         const ucg_builtin_config_t *config,
                                         const ucg_group_member_index_t *member_list,
                                         const ucg_group_member_index_t member_cnt,
                                         enum ucg_builtin_plan_build_type build_type,
                                         enum ucg_builtin_plan_recursive_type recursive_type);

ucs_status_t ucg_builtin_recursive_binary_build(ucg_builtin_plan_t *recursive,
                                         ucg_builtin_group_ctx_t *ctx,
                                         const ucg_builtin_config_t *config,
                                         const ucg_group_member_index_t *member_list,
                                         const ucg_group_member_index_t member_cnt,
                                         enum ucg_builtin_plan_build_type build_type,
                                         enum ucg_builtin_plan_recursive_type recursive_type);

ucs_status_t ucg_builtin_topology_info_create(ucg_builtin_topology_info_params_t *topo_params,
                                              const ucg_group_params_t *group_params,
                                              ucg_group_member_index_t root);

ucs_status_t ucg_builtin_am_handler(void *arg, void *data, size_t length, unsigned am_flags);

void ucg_builtin_msg_dump(ucp_worker_h worker, uct_am_trace_type_t type,
                          uint8_t id, const void *data, size_t length,
                          char *buffer, size_t max);

void ucg_builtin_bcast_algo_switch(const enum ucg_builtin_bcast_algorithm bcast_algo_decision,
                                   struct ucg_builtin_algorithm *algo);

void ucg_builtin_barrier_algo_switch(const enum ucg_builtin_barrier_algorithm barrier_algo_decision,
                                     struct ucg_builtin_algorithm *algo);

void ucg_builtin_allreduce_algo_switch(const enum ucg_builtin_allreduce_algorithm allreduce_algo_decision,
                                       struct ucg_builtin_algorithm *algo);

void ucg_builtin_alltoallv_algo_switch(const enum ucg_builtin_alltoallv_algorithm alltoallv_algo_decision,
                                       struct ucg_builtin_algorithm *algo);

ucs_status_t ucg_builtin_check_ppn(const ucg_group_params_t *group_params,
                                   unsigned *unequal_ppn);

ucs_status_t ucg_builtin_check_nap(const ucg_group_params_t *group_params);

ucs_status_t ucg_builtin_find_myself(const ucg_group_params_t *group_params,
                                     ucg_group_member_index_t *myrank);

enum ucg_group_member_distance ucg_builtin_get_distance(const ucg_group_params_t *group_params,
                                                        ucg_group_member_index_t rank1,
                                                        ucg_group_member_index_t rank2);

ucs_status_t ucg_builtin_check_continuous_number(const ucg_group_params_t *group_params,
                                                 enum ucg_group_member_distance domain_distance,
                                                 unsigned *discont_flag);

enum ucg_builtin_plan_topology_type ucg_builtin_choose_type(enum ucg_collective_modifiers flags);

unsigned ucg_builtin_calculate_ppx(const ucg_group_params_t *group_params,
                                   enum ucg_group_member_distance domain_distance);


ucs_status_t ucg_builtin_destroy_plan(ucg_builtin_plan_t *plan, ucg_group_h group);

ucs_status_t ucg_builtin_check_non_aware_Raben(const ucg_group_params_t *group_params);

ucg_group_member_index_t ucg_builtin_get_local_index(ucg_group_member_index_t global_index,
                                                     const ucg_group_member_index_t *local_members,
                                                     ucg_group_member_index_t member_cnt);

int ucg_is_allreduce_consistency(const ucg_builtin_group_ctx_t *ctx);

short ucg_get_tree_buffer_pos(ucg_group_member_index_t myrank,
                              ucg_group_member_index_t uprank,
                              ucg_group_member_index_t root,
                              unsigned size,
                              unsigned degree,
                              const ucg_group_member_index_t *member_list);

int ucg_builtin_need_calate_position(const ucg_collective_type_t *coll,
                                     unsigned up_cnt,
                                     const ucg_builtin_group_ctx_t *ctx,
                                     enum ucg_builtin_plan_topology_type tree_topo);

int inc_get_header_size();
void init_inc_params(void *ucg_group);
ucs_status_t inc_create(void *ucg_group, void *ucg_config, const void *ucg_params);
ucs_status_t inc_destroy(void *ucg_group, uint8_t fail_cause);
ucs_status_t inc_check_set_packet_para(void *group, void *params);
ucs_status_t ucg_builtin_add_inc(void *tree, void *phase, const void *params, uct_ep_h **eps,
    unsigned *phs_inc_cnt, unsigned *step_inc_cnt, unsigned ppx, enum ucg_group_hierarchy_level topo_level);
void inc_send_cb(void *builtin_req);
ucs_status_t inc_comp_recv_one(void *req, uint64_t offset, const void *data, size_t length);
ucs_status_t inc_comp_recv_many(void *req, uint64_t offset, const void *data, size_t length);
size_t inc_enable(void *builtin_config);
size_t inc_available(const void *group);
size_t inc_used(const void *ucg_params);

END_C_DECLS

#endif
