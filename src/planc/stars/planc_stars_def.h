/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef UCG_PLANC_STARS_DEF_H_
#define UCG_PLANC_STARS_DEF_H_

// todo only export scp.h at last
#include "scp_ep.h"
#include "scp_mm.h"
#include "scp_address.h"
#include "scp_rma.h"


typedef struct stars_buf_desc {
    uintptr_t               addr;
    size_t                  length;
    char                    key_packed[0];
} stars_buf_desc_t;
typedef struct stars_buf_desc *stars_buf_desc_h;

typedef struct stars_rank_info {
    ucg_rank_t              peer_id;                // communication peer rank id
    scp_ep_h                ep;
    stars_buf_desc_h        rbuf_desc;
    uct_rkey_bundle_t       *rkey_bundle;
    size_t                  rkey_packed_size;
    uint32_t                length;
    uint32_t                offset;
    int                     flag;
    ucp_dt_iov_t            desc_iov_dt[2];
    uint8_t                 barrier_flag;
    scp_event_h             barrier_event;
} stars_rank_info_t;
typedef struct stars_rank_info *stars_rank_info_h;

typedef struct stars_comm_dep {
    stars_rank_info_h       put_ranks;
    stars_rank_info_h       get_ranks;
    uint32_t                put_rank_num;
    uint32_t                get_rank_num;
} stars_comm_dep_t;
typedef struct stars_comm_dep *stars_comm_dep_h;

typedef struct stars_event_elem {
    ucg_rank_t      rank;      /* which peer rank will notify the scp event */
    scp_event_t     event;
} stars_event_elem_t;
typedef struct stars_event_elem *stars_event_elem_h;

/* 2 is the max rank number that get data from currently */
#define STARS_EVENT_ELEM_SIZE(elem_num) ((elem_num) * sizeof(stars_event_elem_t))

#define BUF_DESC_HEAD_SIZE (sizeof(stars_buf_desc_t) + SCP_MM_PACK_SIZE)


typedef struct stars_comm_plan {
    stars_comm_dep_t         comm_dep;
    uint8_t                  buf_desc_flag;          // flag indicates whether sbuf and rbuf initialized.
    stars_buf_desc_h         lsbuf_desc;             // local send buffer
    stars_buf_desc_h         lrbuf_desc;             // local recv buffer
    sct_mem_h                *lsmemh;                // use for local send buffer
    sct_mem_h                *lrmemh;                // use for local receive buffer
    size_t                   lrbuf_desc_size;        // use for send rbuf_desc to parent
    size_t                   sbuf_size;              // local send buffer size
    int                      eid_shared_flag;
    int                      eid_shared_stride;
    stars_event_elem_h       event_elem;
    uint8_t                  max_lanes_num_in_ep;    // Max numbers of lane in EP
} stars_comm_plan_t;
typedef struct stars_comm_plan *stars_comm_plan_h;

typedef enum ucg_planc_stars_ppn_level {
    PPN_LEVEL_STARS_1,        /* 1 */
    PPN_LEVEL_STARS_4,        /* 2-4 */
    PPN_LEVEL_STARS_8,        /* 5-8 */
    PPN_LEVEL_STARS_16,       /* 9-16 */
    PPN_LEVEL_STARS_32,       /* 17-32 */
    PPN_LEVEL_STARS_64,       /* 33-64 */
    PPN_LEVEL_STARS_LG,       /* >64 */
    /* The new ppn level must be added above */
    PPN_LEVEL_STARS_NUMS
} ucg_planc_stars_ppn_level_t;

typedef enum ucg_planc_stars_node_level {
    NODE_LEVEL_STARS_4,       /* 1-4 */
    NODE_LEVEL_STARS_8,       /* 5-8 */
    NODE_LEVEL_STARS_16,      /* 9-16 */
    NODE_LEVEL_STARS_LG,      /* >16 */
    /* The new node level must be added above */
    NODE_LEVEL_STARS_NUMS
} ucg_planc_stars_node_level_t;

#endif
