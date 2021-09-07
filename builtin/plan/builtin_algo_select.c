/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2021.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */


#include <ucs/debug/log.h>
#include <ucp/dt/dt.h>
#include "src/ucg/builtin/ops/builtin_ops.h"

#include "builtin_algo_decision.h"
#include "builtin_plan.h"
typedef enum {
    SIZE_LEVEL_4B,
    SIZE_LEVEL_8B,
    SIZE_LEVEL_16B,
    SIZE_LEVEL_32B,
    SIZE_LEVEL_64B,
    SIZE_LEVEL_128B,
    SIZE_LEVEL_256B,
    SIZE_LEVEL_512B,
    SIZE_LEVEL_1KB,
    SIZE_LEVEL_2KB,
    SIZE_LEVEL_4KB,
    SIZE_LEVEL_8KB,
    SIZE_LEVEL_1MB,
    SIZE_LEVEL_LG,
    SIZE_LEVEL_NUMS
} size_level_t;

typedef enum {
    PPN_LEVEL_4,
    PPN_LEVEL_8,
    PPN_LEVEL_16,
    PPN_LEVEL_32,
    PPN_LEVEL_64,
    PPN_LEVEL_LG,
    PPN_LEVEL_NUMS
} ppn_level_t;

typedef enum {
    NODE_LEVEL_4,
    NODE_LEVEL_8,
    NODE_LEVEL_16,
    NODE_LEVEL_32,
    NODE_LEVEL_LG,
    NODE_LEVEL_NUMS
} node_level_t;

const static int barrier_algo_tbl[PPN_LEVEL_NUMS][NODE_LEVEL_NUMS] = {
    {10, 7, 2, 6, 4},
    {10, 10, 2, 6, 7},
    {10, 10, 10, 7, 7},
    {10, 10, 10, 10, 6},
    {10, 10, 10, 10, 6},
    {10, 10, 10, 10, 5},
};
const static int bcast_algo_tbl[SIZE_LEVEL_NUMS][PPN_LEVEL_NUMS][NODE_LEVEL_NUMS] = {
    {
        {3, 3, 3, 3, 3},
        {4, 4, 3, 3, 3},
        {4, 4, 4, 3, 3},
        {3, 3, 4, 4, 3},
        {3, 3, 3, 3, 3},
        {3, 3, 4, 3, 3},
    }, { /* SIZE_LEVEL_8B*/
        {3, 3, 3, 3, 3},
        {4, 4, 4, 3, 3},
        {4, 4, 4, 3, 3},
        {3, 3, 4, 4, 3},
        {3, 3, 3, 4, 3},
        {3, 3, 4, 3, 3},
    }, { /* SIZE_LEVEL_16B*/
        {3, 3, 4, 3, 3},
        {3, 4, 3, 3, 3},
        {4, 4, 4, 3, 3},
        {3, 3, 4, 4, 3},
        {3, 3, 3, 4, 3},
        {3, 3, 4, 3, 3},
    }, {/* SIZE_LEVEL_32B*/
        {3, 3, 3, 3, 3},
        {4, 4, 3, 3, 3},
        {3, 4, 4, 3, 3},
        {3, 3, 4, 4, 3},
        {3, 3, 3, 4, 3},
        {3, 3, 3, 3, 3},
    }, { /* SIZE_LEVEL_64B*/
        {3, 3, 3, 3, 3},
        {4, 3, 3, 3, 3},
        {4, 4, 4, 3, 3},
        {3, 3, 4, 4, 3},
        {3, 3, 3, 4, 3},
        {3, 3, 3, 3, 3},
    }, { /* SIZE_LEVEL_128B*/
        {3, 3, 3, 3, 3},
        {3, 4, 3, 3, 3},
        {3, 4, 4, 3, 3},
        {3, 3, 3, 4, 3},
        {3, 3, 3, 4, 3},
        {3, 3, 3, 4, 3},
    }, {/* SIZE_LEVEL_256B*/
        {3, 3, 3, 3, 3},
        {4, 3, 3, 3, 3},
        {3, 4, 4, 4, 3},
        {3, 3, 3, 4, 3},
        {3, 3, 4, 4, 3},
        {3, 3, 4, 3, 3},
    }, { /* SIZE_LEVEL_512B*/
        {4, 3, 4, 3, 3},
        {4, 3, 3, 3, 3},
        {3, 3, 4, 4, 4},
        {4, 3, 4, 4, 3},
        {3, 3, 3, 4, 3},
        {3, 3, 4, 4, 3},
    }, { /* SIZE_LEVEL_1KB*/
        {3, 3, 3, 3, 3},
        {3, 3, 3, 3, 3},
        {4, 4, 4, 3, 3},
        {3, 3, 3, 4, 3},
        {3, 3, 4, 4, 3},
        {3, 3, 4, 3, 3},
    }, {/* SIZE_LEVEL_2KB*/
        {4, 3, 3, 3, 3},
        {4, 3, 3, 4, 3},
        {4, 4, 4, 3, 4},
        {4, 3, 4, 4, 4},
        {3, 4, 3, 4, 3},
        {3, 4, 4, 4, 3},
    }, { /* SIZE_LEVEL_4KB*/
        {3, 3, 3, 3, 3},
        {4, 4, 4, 4, 3},
        {4, 4, 4, 3, 3},
        {3, 4, 4, 4, 3},
        {4, 4, 4, 4, 4},
        {4, 4, 4, 4, 4},
    }, { /* SIZE_LEVEL_8KB*/
        {4, 3, 3, 3, 3},
        {4, 3, 3, 4, 3},
        {4, 4, 4, 4, 4},
        {4, 4, 4, 4, 4},
        {4, 4, 4, 4, 4},
        {4, 4, 4, 4, 4},
    }, { /* SIZE_LEVEL_1MB*/
        {4, 3, 3, 3, 3},
        {4, 3, 3, 4, 3},
        {4, 4, 4, 4, 4},
        {4, 4, 4, 4, 4},
        {4, 4, 4, 4, 4},
        {4, 4, 4, 4, 4},
    }, {/* SIZE_LEVEL_LG*/
        {4, 3, 3, 3, 3},
        {4, 3, 3, 4, 3},
        {4, 4, 4, 4, 4},
        {4, 4, 4, 4, 4},
        {4, 4, 4, 4, 4},
        {4, 4, 4, 4, 4},
    }
};
const static int allreduce_algo_tbl[SIZE_LEVEL_NUMS][PPN_LEVEL_NUMS][NODE_LEVEL_NUMS] = {
    {
        {11, 8, 8, 8, 7},
        {11, 11, 8, 8, 7},
        {11, 11, 11, 8, 7},
        {11, 11, 11, 11, 7},
        {11, 11, 11, 11, 7},
        {11, 11, 11, 11, 7},
    }, { /* SIZE_LEVEL_8B*/
        {11, 8, 8, 8, 7},
        {11, 11, 8, 8, 7},
        {11, 11, 11, 8, 7},
        {11, 11, 11, 11, 7},
        {11, 11, 11, 11, 7},
        {11, 11, 11, 11, 7},
    }, { /* SIZE_LEVEL_16B*/
        {11, 8, 8, 8, 7},
        {11, 11, 8, 8, 7},
        {11, 11, 11, 8, 7},
        {11, 11, 11, 11, 7},
        {11, 11, 11, 11, 7},
        {11, 11, 11, 11, 7},
    }, { /* SIZE_LEVEL_32B*/
        {11, 8, 8, 8, 7},
        {11, 11, 8, 8, 7},
        {11, 11, 11, 8, 7},
        {11, 11, 11, 11, 7},
        {11, 11, 11, 11, 7},
        {11, 11, 11, 11, 7},
    }, { /* SIZE_LEVEL_64B*/
        {11, 8, 8, 8, 7},
        {11, 11, 8, 8, 7},
        {11, 11, 11, 8, 7},
        {11, 11, 11, 11, 7},
        {11, 11, 11, 11, 7},
        {11, 11, 11, 11, 11},
    }, { /* SIZE_LEVEL_128B*/
        {14, 8, 8, 8, 7},
        {11, 11, 8, 8, 8},
        {11, 11, 11, 8, 7},
        {11, 11, 11, 11, 8},
        {11, 11, 11, 11, 7},
        {11, 11, 11, 11, 7},
    }, { /* SIZE_LEVEL_256B*/
        {14, 13, 13, 13, 13},
        {11, 11, 13, 13, 13},
        {11, 11, 11, 14, 14},
        {11, 11, 11, 11, 7},
        {11, 11, 11, 8, 7},
        {11, 11, 11, 7, 7},
    }, { /* SIZE_LEVEL_512B*/
        {13, 13, 13, 13, 13},
        {13, 13, 13, 13, 13},
        {14, 14, 11, 13, 13},
        {14, 11, 14, 14, 14},
        {11, 11, 11, 8, 7},
        {11, 11, 7, 8, 7},
    }, { /* SIZE_LEVEL_1KB*/
        {13, 8, 8, 8, 8},
        {13, 13, 13, 13, 13},
        {14, 14, 14, 14, 14},
        {13, 13, 13, 13, 13},
        {14, 14, 14, 14, 7},
        {11, 11, 7, 8, 8},
    }, { /* SIZE_LEVEL_2KB*/
        {13, 13, 13, 13, 8},
        {13, 13, 12, 12, 12},
        {13, 13, 13, 13, 13},
        {14, 14, 14, 14, 14},
        {13, 13, 13, 13, 13},
        {14, 11, 7, 7, 7},
    }, { /* SIZE_LEVEL_4KB*/
        {13, 13, 13, 13, 12},
        {13, 13, 13, 12, 12},
        {12, 12, 12, 12, 12},
        {13, 13, 13, 13, 13},
        {14, 14, 13, 14, 14},
        {13, 13, 13, 13, 7},
    }, { /* SIZE_LEVEL_8KB*/
        {13, 13, 13, 13, 12},
        {13, 13, 13, 12, 12},
        {13, 12, 12, 12, 12},
        {12, 12, 12, 12, 12},
        {13, 13, 13, 13, 13},
        {12, 12, 5, 5, 6},
    }, { /* SIZE_LEVEL_1MB*/
        {13, 13, 13, 13, 12},
        {13, 13, 13, 12, 12},
        {13, 12, 12, 12, 12},
        {12, 12, 12, 12, 12},
        {13, 13, 13, 13, 13},
        {12, 12, 13, 13, 13},
    }, { /* SIZE_LEVEL_LG*/
        {4, 4, 4, 4, 4},
        {4, 4, 4, 4, 4},
        {4, 4, 4, 4, 4},
        {4, 4, 4, 4, 4},
        {4, 4, 4, 4, 4},
        {4, 4, 4, 4, 4},
    }
};

static inline int log2_n(unsigned int n, unsigned int begin)
{
    int index = 0;
    while (n >= begin) {
        n = n >> 1;
        index++;
    }
    return index;
}

static size_level_t ucg_builtin_get_size_level(const ucg_group_params_t *group_params,
                                                const ucg_collective_params_t *coll_params)
{
    const int size_lev_small = 4;
    const int size_lev_mid = 8192;
    const int size_lev_large = 1048576;
    int size;
    int dt_len;
    ucp_datatype_t ucp_datatype;
    group_params->mpi_dt_convert(coll_params->send.dt_ext, &ucp_datatype);
    dt_len = UCP_DT_IS_CONTIG(ucp_datatype) ? coll_params->send.dt_len :
             ucg_builtin_get_dt_len(ucp_dt_generic(ucp_datatype));
    size = dt_len * coll_params->send.count;
    ucs_info("The SIZE parameter of auto select algorithm is %d", size);
    if (size <= size_lev_small) {
        return SIZE_LEVEL_4B;
    }
    if (size > size_lev_mid && size <= size_lev_large) {
        return SIZE_LEVEL_1MB;
    }
    if (size > size_lev_large) {
        return SIZE_LEVEL_LG;
    }
    size--;
    return (size_level_t)log2_n(size, size_lev_small);
}

static ppn_level_t ucg_builtin_get_ppn_level(const ucg_group_params_t *group_params)
{
    const int ppn_lev_small = 4;
    const int ppn_lev_large = 64;
    int ppn_max;
    ppn_max = group_params->topo_args.ppn_max;
    ucs_info("The PPN parameter of auto select algorithm is %d", ppn_max);
    if (ppn_max <= ppn_lev_small) {
        return PPN_LEVEL_4;
    }
    if (ppn_max > ppn_lev_large) {
        return PPN_LEVEL_LG;
    }
    ppn_max--;
    return (ppn_level_t)log2_n(ppn_max, ppn_lev_small);
}

static node_level_t ucg_builtin_get_node_level(const ucg_group_params_t *group_params)
{
    const int node_lev_small = 4;
    const int node_lev_large = 32;
    int node_nums;
    node_nums = group_params->topo_args.node_nums;
    ucs_info("The NODE parameter of auto select algorithm is %d", node_nums);
    if (node_nums <= node_lev_small) {
        return NODE_LEVEL_4;
    }
    if (node_nums > node_lev_large) {
        return NODE_LEVEL_LG;
    }
    node_nums--;
    return (node_level_t)log2_n(node_nums, node_lev_small);
}

static int ucg_builtin_barrier_algo_select(const ucg_group_params_t *group_params,
                                                const ucg_collective_params_t *coll_params)
{
    ppn_level_t ppn_lev;
    node_level_t node_lev;

    ppn_lev = ucg_builtin_get_ppn_level(group_params);
    node_lev = ucg_builtin_get_node_level(group_params);
    return barrier_algo_tbl[ppn_lev][node_lev];
}

static int ucg_builtin_bcast_algo_select(const ucg_group_params_t *group_params,
                                                const ucg_collective_params_t *coll_params)
{
    size_level_t size_lev;
    ppn_level_t ppn_lev;
    node_level_t node_lev;

    size_lev = ucg_builtin_get_size_level(group_params, coll_params);
    ppn_lev = ucg_builtin_get_ppn_level(group_params);
    node_lev = ucg_builtin_get_node_level(group_params);
    return bcast_algo_tbl[size_lev][ppn_lev][node_lev];
}

static int ucg_builtin_allreduce_algo_select(const ucg_group_params_t *group_params,
                                                const ucg_collective_params_t *coll_params)
{
    size_level_t size_lev;
    ppn_level_t ppn_lev;
    node_level_t node_lev;

    size_lev = ucg_builtin_get_size_level(group_params, coll_params);
    ppn_lev = ucg_builtin_get_ppn_level(group_params);
    node_lev = ucg_builtin_get_node_level(group_params);
    return allreduce_algo_tbl[size_lev][ppn_lev][node_lev];
}

static int ucg_builtin_alltoallv_algo_select(const ucg_group_params_t *group_params,
                                             const ucg_collective_params_t *coll_params)
{
    return UCG_ALGORITHM_ALLTOALLV_NODE_AWARE_PLUMMER;
}

typedef int (*algo_select_f)(const ucg_group_params_t *group_params, const ucg_collective_params_t *coll_params);
static algo_select_f algo_select[COLL_TYPE_NUMS] = {
    ucg_builtin_barrier_algo_select,
    ucg_builtin_bcast_algo_select,
    ucg_builtin_allreduce_algo_select,
    ucg_builtin_alltoallv_algo_select,
};

int ucg_builtin_algo_auto_select(const ucg_group_params_t *group_params,
                                const ucg_collective_params_t *coll_params)
{
    return algo_select[coll_params->coll_type](group_params, coll_params);
}
