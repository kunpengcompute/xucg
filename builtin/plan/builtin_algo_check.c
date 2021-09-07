/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2021.  ALL RIGHTS RESERVED.
 * Description: Algorithm decision for collective operation
 */


#include <ucs/debug/log.h>
#include <ucp/dt/dt_contig.h>

#include "builtin_plan.h"
#include "../../api/ucg.h"
#include "builtin_algo_decision.h"

typedef enum {
    CHECK_ALGO_NOT_EXIST,
    CHECK_NON_CONTIG_DATATYPE,
    CHECK_NON_COMMUTATIVE,
    CHECK_NAP_UNSUPPORT,
    CHECK_RABEN_UNSUPPORT,
    CHECK_NAWARE_RABEN_UNSUPPORT,
    CHECK_SAWARE_RABEN_UNSUPPORT,
    CHECK_BIND_TO_NONE,
    CHECK_PPN_UNBALANCE,
    CHECK_NRANK_UNCONTINUE,
    CHECK_PPS_UNBALANCE,
    CHECK_SRANK_UNCONTINUE,
    CHECK_LARGE_DATATYPE,
    CHECK_PHASE_SEGMENT,
    CHECK_INC_UNSUPPORT,
    CHECK_MPI_IN_PLACE,
    CHECK_PLUMMER_UNSUPPORT,
    /* The new check item must be added above */
    CHECK_ITEM_NUMS
} check_item_t;

static const char *check_item_str_array[CHECK_ITEM_NUMS] = {
    "algo_not_exist",
    "non_contig_datatype",
    "non_commutative",
    "nap_unsupport",
    "raben_unsupport",
    "naware_raben_unsupport",
    "saware_raben_unsupport",
    "bind_to_none",
    "ppn_unbalance",
    "nrank_uncontinue",
    "pps_unbalance",
    "srank_uncontinue",
    "large_datatype",
    "phase_segment",
    "inc_unsupport",
    "mpi_in_place",
    "plummer_unsupport",
};

static int ucg_builtin_check_algo_not_exist(const ucg_group_params_t *group_params,
                                            const ucg_collective_params_t *coll_params,
                                            const int algo)
{
    switch (coll_params->coll_type) {
        case COLL_TYPE_BARRIER:
            if (algo == UCG_ALGORITHM_BARRIER_NODE_AWARE_INC ||
                algo == UCG_ALGORITHM_BARRIER_SOCKET_AWARE_INC ||
                algo == UCG_ALGORITHM_BARRIER_NAP) {
                return 1;
            }
            break;
        case COLL_TYPE_BCAST:
            if (algo == UCG_ALGORITHM_BCAST_NODE_AWARE_INC) {
                return 1;
            }
            break;
        case COLL_TYPE_ALLREDUCE:
            if (algo == UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_INC ||
                algo == UCG_ALGORITHM_ALLREDUCE_SOCKET_AWARE_INC ||
                algo == UCG_ALGORITHM_ALLREDUCE_NAP) {
                return 1;
            }
            break;
        default:
            break;
    }
    return 0;
}

static int ucg_builtin_check_non_contig_datatype(const ucg_group_params_t *group_params,
                                                 const ucg_collective_params_t *coll_params,
                                                 const int algo)
{
    ucp_datatype_t ucp_datatype;
    if (coll_params->send.count <= 0 || coll_params->send.dt_len <= 0) {
        return 0;
    }

    group_params->mpi_dt_convert(coll_params->send.dt_ext, &ucp_datatype);
    return !UCP_DT_IS_CONTIG(ucp_datatype);
}

static int ucg_builtin_check_non_commutative(const ucg_group_params_t *group_params,
                                             const ucg_collective_params_t *coll_params,
                                             const int algo)
{
    return !group_params->op_is_commute_f(coll_params->send.op_ext);
}

static int ucg_builtin_check_nap_unsupport(const ucg_group_params_t *group_params,
                                           const ucg_collective_params_t *coll_params,
                                           const int algo)
{
    unsigned node_nums;
    int ppn_local;
    ppn_local = group_params->topo_args.ppn_local;
    node_nums = group_params->member_count / group_params->topo_args.ppn_local;
    return (group_params->topo_args.ppn_unbalance || ppn_local <= 1 || (node_nums & (node_nums - 1)));
}

static int ucg_builtin_check_raben_unsupport(const ucg_group_params_t *group_params,
                                             const ucg_collective_params_t *coll_params,
                                             const int algo)
{
    /* Raben does not support odd number of processes */
    const int even_number = 2;
    return (group_params->member_count % even_number);
}

static int ucg_builtin_check_naware_raben_unsupport(const ucg_group_params_t *group_params,
                                                    const ucg_collective_params_t *coll_params,
                                                    const int algo)
{
    int ppn_local;
    const int even_number = 2;

    ppn_local = group_params->topo_args.ppn_local;
    return (group_params->topo_args.ppn_unbalance ||  (ppn_local % even_number == 1 && ppn_local != 1));
}

static int ucg_builtin_check_saware_raben_unsupport(const ucg_group_params_t *group_params,
                                                    const ucg_collective_params_t *coll_params,
                                                    const int algo)
{
    int pps_local;
    const int even_number = 2;

    pps_local = group_params->topo_args.pps_local;
    return (group_params->topo_args.pps_unbalance ||  (pps_local % even_number == 1 && pps_local != 1));
}

static int ucg_builtin_check_bind_to_none(const ucg_group_params_t *group_params,
                                          const ucg_collective_params_t *coll_params,
                                          const int algo)
{
    return group_params->topo_args.bind_to_none;
}

static int ucg_builtin_check_ppn_unbalance(const ucg_group_params_t *group_params,
                                           const ucg_collective_params_t *coll_params,
                                           const int algo)
{
    return group_params->topo_args.ppn_unbalance;
}

static int ucg_builtin_check_nrank_uncontinue(const ucg_group_params_t *group_params,
                                              const ucg_collective_params_t *coll_params,
                                              const int algo)
{
    return group_params->topo_args.nrank_uncontinue;
}

static int ucg_builtin_check_pps_unbalance(const ucg_group_params_t *group_params,
                                           const ucg_collective_params_t *coll_params,
                                           const int algo)
{
    return group_params->topo_args.pps_unbalance;
}

static int ucg_builtin_check_srank_uncontinue(const ucg_group_params_t *group_params,
                                              const ucg_collective_params_t *coll_params,
                                              const int algo)
{
    return group_params->topo_args.srank_uncontinue || group_params->topo_args.nrank_uncontinue;
}

static int ucg_builtin_check_large_datatype(const ucg_group_params_t *group_params,
                                            const ucg_collective_params_t *coll_params,
                                            const int algo)
{
    const int large_datatype_threshold = 32;
    return (coll_params->send.dt_len > large_datatype_threshold);
}

static int ucg_builtin_check_phase_segment(const ucg_group_params_t *group_params,
                                           const ucg_collective_params_t *coll_params,
                                           const int algo)
{
    int count = coll_params->send.count;
    size_t dt_len = coll_params->send.dt_len;

#define UCG_SHORT_THRESHHOLD 176
#define UCT_MIN_SHORT_ONE_LEN 80
#define UCT_MIN_BCOPY_ONE_LEN 1000
    if (dt_len > UCT_MIN_BCOPY_ONE_LEN) {
        return 1;
    }
    if (dt_len > UCT_MIN_SHORT_ONE_LEN && (dt_len * count) <= UCG_SHORT_THRESHHOLD) {
        return 1;
    }
#undef UCG_SHORT_THRESHHOLD
#undef UCT_MIN_SHORT_ONE_LEN
#undef UCT_MIN_BCOPY_ONE_LEN
    return 0;
}

static int ucg_builtin_check_inc_unsupport(const ucg_group_params_t *group_params,
                                           const ucg_collective_params_t *coll_params,
                                           const int algo)
{
    int enable_inc = 0;
#if ENABLE_UCG_HICOLL
    enable_inc = inc_used(group_params);
#endif
    return !enable_inc;
}

static int ucg_builtin_check_mpi_in_place(const ucg_group_params_t *group_params,
                                          const ucg_collective_params_t *coll_params,
                                          const int algo)
{
    return coll_params->send.buf == MPI_IN_PLACE;
}

STATIC_GTEST int ucg_builtin_check_plummer_unsupport(const ucg_group_params_t *group_params,
                                                     const ucg_collective_params_t *coll_params,
                                                     const int algo)
{
    ucg_group_member_index_t i;
    for (i = 0; i < group_params->member_count; i++) {
        if (coll_params->send.displs[i] < 0 || coll_params->recv.displs[i] < 0) {
            return 1;
        }
    }
    if (coll_params->send.displs[0] != 0 || coll_params->recv.displs[0] != 0) {
        return 1;
    }
    for (i = 0; i < (group_params->member_count - 1); i++) {
        if ((coll_params->send.displs[i + 1] != (coll_params->send.displs[i] + coll_params->send.counts[i])) ||
            (coll_params->recv.displs[i + 1] != (coll_params->recv.displs[i] + coll_params->recv.counts[i]))) {
            return 1;
        }
    }
    return 0;
}

typedef int (*check_f)(const ucg_group_params_t *group_params, const ucg_collective_params_t *coll_params, const int algo);

static check_f check_fun_array[CHECK_ITEM_NUMS] = {
    ucg_builtin_check_algo_not_exist,
    ucg_builtin_check_non_contig_datatype,
    ucg_builtin_check_non_commutative,
    ucg_builtin_check_nap_unsupport,
    ucg_builtin_check_raben_unsupport,
    ucg_builtin_check_naware_raben_unsupport,
    ucg_builtin_check_saware_raben_unsupport,
    ucg_builtin_check_bind_to_none,
    ucg_builtin_check_ppn_unbalance,
    ucg_builtin_check_nrank_uncontinue,
    ucg_builtin_check_pps_unbalance,
    ucg_builtin_check_srank_uncontinue,
    ucg_builtin_check_large_datatype,
    ucg_builtin_check_phase_segment,
    ucg_builtin_check_inc_unsupport,
    ucg_builtin_check_mpi_in_place,
    ucg_builtin_check_plummer_unsupport,
};

typedef struct {
    check_item_t chk_item;
    int algo_fb;
} check_fallback_t;

typedef struct {
    check_fallback_t *chkfb;
    int chkfb_size;
} chkfb_tbl_t;

#define CHKFB_BARRIER(n)  \
        chkfb_barrier_algo##n

#define CHKFB_SIZE_BARRIER(n) \
        (sizeof(chkfb_barrier_algo##n) / sizeof(chkfb_barrier_algo##n[0]))

#define CHKFB_BCAST(n) \
        chkfb_bcast_algo##n

#define CHKFB_SIZE_BCAST(n) \
        (sizeof(chkfb_bcast_algo##n) / sizeof(chkfb_bcast_algo##n[0]))

#define CHKFB_ALLREDUCE(n) \
        chkfb_allreduce_algo##n

#define CHKFB_SIZE_ALLREDUCE(n) \
        (sizeof(chkfb_allreduce_algo##n) / sizeof(chkfb_allreduce_algo##n[0]))

#define CHKFB_ALLTOALLV(n) \
        chkfb_alltoallv_algo##n

#define CHKFB_SIZE_ALLTOALLV(n) \
        (sizeof(chkfb_alltoallv_algo##n) / sizeof(chkfb_alltoallv_algo##n[0]))

static check_fallback_t chkfb_allreduce_algo2[] = {
    {CHECK_NON_CONTIG_DATATYPE,   1},
    {CHECK_NON_COMMUTATIVE,   1},
    {CHECK_LARGE_DATATYPE,   1},
};

static check_fallback_t chkfb_allreduce_algo3[] = {
    {CHECK_NON_CONTIG_DATATYPE,   1},
    {CHECK_NON_COMMUTATIVE,   1},
    {CHECK_BIND_TO_NONE,   2},
    {CHECK_PPN_UNBALANCE,  2},
    {CHECK_PPS_UNBALANCE,   2},
    {CHECK_SRANK_UNCONTINUE,   2},
    {CHECK_LARGE_DATATYPE,   1},
};

static check_fallback_t chkfb_allreduce_algo4[] = {
    {CHECK_NON_CONTIG_DATATYPE,   1},
    {CHECK_NON_COMMUTATIVE,   1},
    {CHECK_PHASE_SEGMENT,   1},
};

static check_fallback_t chkfb_allreduce_algo5[] = {
    {CHECK_NON_CONTIG_DATATYPE,   1},
    {CHECK_NON_COMMUTATIVE,   1},
    {CHECK_PPN_UNBALANCE,   2},
    {CHECK_NRANK_UNCONTINUE,   2},
    {CHECK_LARGE_DATATYPE,   1},
};

static check_fallback_t chkfb_allreduce_algo6[] = {
    {CHECK_NON_CONTIG_DATATYPE,   1},
    {CHECK_NON_COMMUTATIVE,   1},
    {CHECK_BIND_TO_NONE,   5},
    {CHECK_PPN_UNBALANCE,  2},
    {CHECK_PPS_UNBALANCE,   5},
    {CHECK_SRANK_UNCONTINUE,   5},
    {CHECK_LARGE_DATATYPE,   1},
};

static check_fallback_t chkfb_allreduce_algo7[] = {
    {CHECK_NON_CONTIG_DATATYPE,   1},
    {CHECK_NON_COMMUTATIVE,   1},
    {CHECK_PPN_UNBALANCE,  2},
    {CHECK_NRANK_UNCONTINUE,   2},
    {CHECK_LARGE_DATATYPE,   1},
};

static check_fallback_t chkfb_allreduce_algo8[] = {
    {CHECK_NON_CONTIG_DATATYPE,   1},
    {CHECK_NON_COMMUTATIVE,   1},
    {CHECK_BIND_TO_NONE,   7},
    {CHECK_PPN_UNBALANCE,  2},
    {CHECK_PPS_UNBALANCE,   7},
    {CHECK_SRANK_UNCONTINUE,   7},
    {CHECK_LARGE_DATATYPE,   1},
};

static check_fallback_t chkfb_allreduce_algo9[] = {
    {CHECK_ALGO_NOT_EXIST,   1},
    {CHECK_NON_CONTIG_DATATYPE,   1},
    {CHECK_NON_COMMUTATIVE,   1},
    {CHECK_PPN_UNBALANCE,  2},
    {CHECK_NRANK_UNCONTINUE,   2},
    {CHECK_LARGE_DATATYPE,   1},
    {CHECK_INC_UNSUPPORT,  2},
};

static check_fallback_t chkfb_allreduce_algo10[] = {
    {CHECK_ALGO_NOT_EXIST,   1},
    {CHECK_NON_CONTIG_DATATYPE,   1},
    {CHECK_NON_COMMUTATIVE,   1},
    {CHECK_BIND_TO_NONE,   2},
    {CHECK_PPN_UNBALANCE,  2},
    {CHECK_PPS_UNBALANCE,   2},
    {CHECK_SRANK_UNCONTINUE,   2},
    {CHECK_LARGE_DATATYPE,   1},
    {CHECK_INC_UNSUPPORT,    3},
};

static check_fallback_t chkfb_allreduce_algo11[] = {
    {CHECK_ALGO_NOT_EXIST,   1},
    {CHECK_NON_CONTIG_DATATYPE,   1},
    {CHECK_NON_COMMUTATIVE,   1},
    {CHECK_NAP_UNSUPPORT,   2},
    {CHECK_BIND_TO_NONE,   2},
    {CHECK_NRANK_UNCONTINUE,   2},
    {CHECK_LARGE_DATATYPE,   1},
};

static check_fallback_t chkfb_allreduce_algo12[] = {
    {CHECK_NON_CONTIG_DATATYPE,   1},
    {CHECK_NON_COMMUTATIVE,   1},
    {CHECK_RABEN_UNSUPPORT,   4},
    {CHECK_BIND_TO_NONE,   2},
    {CHECK_LARGE_DATATYPE,   4},
};

static check_fallback_t chkfb_allreduce_algo13[] = {
    {CHECK_NON_CONTIG_DATATYPE,   1},
    {CHECK_NON_COMMUTATIVE,   1},
    {CHECK_RABEN_UNSUPPORT,   4},
    {CHECK_NAWARE_RABEN_UNSUPPORT,   12},
    {CHECK_BIND_TO_NONE,   2},
    {CHECK_PPN_UNBALANCE,  12},
    {CHECK_NRANK_UNCONTINUE,   12},
    {CHECK_LARGE_DATATYPE,   4},
};

static check_fallback_t chkfb_allreduce_algo14[] = {
    {CHECK_NON_CONTIG_DATATYPE,   1},
    {CHECK_NON_COMMUTATIVE,   1},
    {CHECK_RABEN_UNSUPPORT,   4},
    {CHECK_SAWARE_RABEN_UNSUPPORT,   12},
    {CHECK_BIND_TO_NONE,   2},
    {CHECK_PPN_UNBALANCE,  12},
    {CHECK_PPS_UNBALANCE,   12},
    {CHECK_SRANK_UNCONTINUE,   12},
    {CHECK_LARGE_DATATYPE,   4},
};

static check_fallback_t chkfb_barrier_algo3[] = {
    {CHECK_BIND_TO_NONE,   2},
    {CHECK_PPN_UNBALANCE,  2},
    {CHECK_PPS_UNBALANCE,   2},
    {CHECK_SRANK_UNCONTINUE,   2},
};

static check_fallback_t chkfb_barrier_algo4[] = {
    {CHECK_PPN_UNBALANCE,  2},
    {CHECK_NRANK_UNCONTINUE,   2},
};

static check_fallback_t chkfb_barrier_algo5[] = {
    {CHECK_BIND_TO_NONE,   4},
    {CHECK_PPN_UNBALANCE,  2},
    {CHECK_PPS_UNBALANCE,   4},
    {CHECK_SRANK_UNCONTINUE,   4},
};

static check_fallback_t chkfb_barrier_algo6[] = {
    {CHECK_PPN_UNBALANCE,  2},
    {CHECK_NRANK_UNCONTINUE,   2},
};

static check_fallback_t chkfb_barrier_algo7[] = {
    {CHECK_BIND_TO_NONE,   6},
    {CHECK_PPN_UNBALANCE,  2},
    {CHECK_PPS_UNBALANCE,   6},
    {CHECK_SRANK_UNCONTINUE,   6},
};

static check_fallback_t chkfb_barrier_algo8[] = {
    {CHECK_ALGO_NOT_EXIST,   2},
    {CHECK_PPN_UNBALANCE,  2},
    {CHECK_NRANK_UNCONTINUE,   2},
    {CHECK_INC_UNSUPPORT,  2},
};

static check_fallback_t chkfb_barrier_algo9[] = {
    {CHECK_ALGO_NOT_EXIST,   2},
    {CHECK_BIND_TO_NONE,   2},
    {CHECK_PPN_UNBALANCE,  2},
    {CHECK_PPS_UNBALANCE,   2},
    {CHECK_SRANK_UNCONTINUE,   2},
    {CHECK_INC_UNSUPPORT,  3},
};

static check_fallback_t chkfb_barrier_algo10[] = {
    {CHECK_ALGO_NOT_EXIST,   2},
    {CHECK_NAP_UNSUPPORT,   2},
    {CHECK_BIND_TO_NONE,   2},
    {CHECK_NRANK_UNCONTINUE,   2},
};

static check_fallback_t chkfb_bcast_algo3[] = {
    {CHECK_PPN_UNBALANCE,  2},
    {CHECK_NRANK_UNCONTINUE,   2},
};

static check_fallback_t chkfb_bcast_algo4[] = {
    {CHECK_PPN_UNBALANCE,  2},
    {CHECK_NRANK_UNCONTINUE,   2},
};

static check_fallback_t chkfb_bcast_algo5[] = {
    {CHECK_ALGO_NOT_EXIST,   2},
    {CHECK_PPN_UNBALANCE,  2},
    {CHECK_NRANK_UNCONTINUE,  2},
    {CHECK_INC_UNSUPPORT,  2},
};

static check_fallback_t chkfb_alltoallv_algo2[] = {
    {CHECK_PPN_UNBALANCE,  1},
    {CHECK_NRANK_UNCONTINUE,  1},
    {CHECK_MPI_IN_PLACE,  1},
    {CHECK_PLUMMER_UNSUPPORT,  1},
};

chkfb_tbl_t chkfb_barrier[UCG_ALGORITHM_BARRIER_LAST] = {
    {NULL, 0},
    {NULL, 0},
    {NULL, 0},
    {CHKFB_BARRIER(3), CHKFB_SIZE_BARRIER(3)},
    {CHKFB_BARRIER(4), CHKFB_SIZE_BARRIER(4)},
    {CHKFB_BARRIER(5), CHKFB_SIZE_BARRIER(5)},
    {CHKFB_BARRIER(6), CHKFB_SIZE_BARRIER(6)},
    {CHKFB_BARRIER(7), CHKFB_SIZE_BARRIER(7)},
    {CHKFB_BARRIER(8), CHKFB_SIZE_BARRIER(8)},
    {CHKFB_BARRIER(9), CHKFB_SIZE_BARRIER(9)},
    {CHKFB_BARRIER(10), CHKFB_SIZE_BARRIER(10)},
};

chkfb_tbl_t chkfb_bcast[UCG_ALGORITHM_BCAST_LAST] = {
    {NULL, 0},
    {NULL, 0},
    {NULL, 0},
    {CHKFB_BCAST(3), CHKFB_SIZE_BCAST(3)},
    {CHKFB_BCAST(4), CHKFB_SIZE_BCAST(4)},
    {CHKFB_BCAST(5), CHKFB_SIZE_BCAST(5)},
};

chkfb_tbl_t chkfb_alltoallv[UCG_ALGORITHM_ALLTOALLV_LAST] = {
    {NULL, 0},
    {NULL, 0},
    {CHKFB_ALLTOALLV(2), CHKFB_SIZE_ALLTOALLV(2)},
};

chkfb_tbl_t chkfb_allreduce[UCG_ALGORITHM_ALLREDUCE_LAST] = {
    {NULL, 0},
    {NULL, 0},
    {CHKFB_ALLREDUCE(2), CHKFB_SIZE_ALLREDUCE(2)},
    {CHKFB_ALLREDUCE(3), CHKFB_SIZE_ALLREDUCE(3)},
    {CHKFB_ALLREDUCE(4), CHKFB_SIZE_ALLREDUCE(4)},
    {CHKFB_ALLREDUCE(5), CHKFB_SIZE_ALLREDUCE(5)},
    {CHKFB_ALLREDUCE(6), CHKFB_SIZE_ALLREDUCE(6)},
    {CHKFB_ALLREDUCE(7), CHKFB_SIZE_ALLREDUCE(7)},
    {CHKFB_ALLREDUCE(8), CHKFB_SIZE_ALLREDUCE(8)},
    {CHKFB_ALLREDUCE(9), CHKFB_SIZE_ALLREDUCE(9)},
    {CHKFB_ALLREDUCE(10), CHKFB_SIZE_ALLREDUCE(10)},
    {CHKFB_ALLREDUCE(11), CHKFB_SIZE_ALLREDUCE(11)},
    {CHKFB_ALLREDUCE(12), CHKFB_SIZE_ALLREDUCE(12)},
    {CHKFB_ALLREDUCE(13), CHKFB_SIZE_ALLREDUCE(13)},
    {CHKFB_ALLREDUCE(14), CHKFB_SIZE_ALLREDUCE(14)},
};

#undef CHKFB_BARRIER
#undef CHKFB_SIZE_BARRIER

#undef CHKFB_BCAST
#undef CHKFB_SIZE_BCAST

#undef CHKFB_ALLREDUCE
#undef CHKFB_SIZE_ALLREDUCE

#undef CHKFB_ALLTOALLV
#undef CHKFB_SIZE_ALLTOALLV

static inline check_fallback_t *ucg_builtin_barrier_check_fallback_array(int algo, int *arr_size)
{
    *arr_size = chkfb_barrier[algo].chkfb_size;
    return chkfb_barrier[algo].chkfb;
}

static inline check_fallback_t *ucg_builtin_bcast_check_fallback_array(int algo, int *arr_size)
{
    *arr_size = chkfb_bcast[algo].chkfb_size;
    return chkfb_bcast[algo].chkfb;
}

static inline check_fallback_t *ucg_builtin_allreduce_check_fallback_array(int algo, int *arr_size)
{
    *arr_size = chkfb_allreduce[algo].chkfb_size;
    return chkfb_allreduce[algo].chkfb;
}

static inline check_fallback_t *ucg_builtin_alltoallv_check_fallback_array(int algo, int *arr_size)
{
    *arr_size = chkfb_alltoallv[algo].chkfb_size;
    return chkfb_alltoallv[algo].chkfb;
}

typedef check_fallback_t *(*chk_fb_arr_f)(int algo, int *arr_size);

static chk_fb_arr_f check_fallback[COLL_TYPE_NUMS] = {
    ucg_builtin_barrier_check_fallback_array,
    ucg_builtin_bcast_check_fallback_array,
    ucg_builtin_allreduce_check_fallback_array,
    ucg_builtin_alltoallv_check_fallback_array,
};

static check_fallback_t *ucg_builtin_get_check_fallback_array(coll_type_t coll_type, int algo, int *arr_size)
{
    return check_fallback[coll_type](algo, arr_size);
}

int ucg_builtin_algo_check_fallback(const ucg_group_params_t *group_params,
                                    const ucg_collective_params_t *coll_params,
                                    int algo)
{
    check_fallback_t *chk_fb = NULL;
    check_item_t chk_item;
    check_f chk_fun;
    int i, size, algo_fb;
    algo_fb = algo;
    algo = 0;
    while (algo != algo_fb) {
        algo = algo_fb;
        size = 0;
        chk_fb = ucg_builtin_get_check_fallback_array(coll_params->coll_type, algo, &size);
        for (i = 0; i < size; i++) {
            chk_item = chk_fb[i].chk_item;
            chk_fun = check_fun_array[chk_item];
            if (chk_fun(group_params, coll_params, algo)) {
                algo_fb = chk_fb[i].algo_fb;
                ucs_info("current algo is %d, check item is %s, fallback to algo %d",
                        algo, check_item_str_array[chk_item], algo_fb);
                break;
            }
        }
    }
    return algo_fb;
}
