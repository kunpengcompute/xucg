/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef XUCG_SCP_DEF_H
#define XUCG_SCP_DEF_H

#include "sct.h"

#include <ucp/api/ucp.h>

#ifndef STARS_DEFAULT_ENV_PREFIX
    #define STARS_DEFAULT_ENV_PREFIX          "UCG_PLANC_STARS_"
#endif

/* stars only support two sct component ["sdma_ofd", "ib", "ub"] */
#define MAX_SCT_COMPONENT_NUM           3

/* One CPU socket have four ROH and one SDMA device
 * may exist nine ifaces for one scp endpoint
 * */
#define SCP_MAX_LANE           9

#define SCP_WORKER_NAME_MAX             32   /* Worker name for debugging */

#define STARS_RC_TL_NAME                "rc_acc"
#define STARS_SDMA_TL_NAME              "sdma_acc"

#define SCP_NULL_RESOURCE               ((scp_rsc_index_t)-1)

#define SCP_THREAD_LOCK_INIT(_spinlock)                 \
    do {                                                \
        ucs_recursive_spinlock_init((_spinlock), 0);    \
    } while (0)

#define SCP_THREAD_LOCK_FINALIZE(_spinlock)                                  \
    do { \
            ucs_recursive_spinlock_destroy((_spinlock)); \
    } while (0)

#define SCP_ASYNC_BLOCK(_async)         \
    do {                                \
        pthread_mutex_lock(_async);     \
    } while (0)

#define SCP_ASYNC_UNBLOCK(_async)       \
    do {                                \
        pthread_mutex_unlock(_async);   \
    } while (0)

typedef uint8_t                         scp_rsc_index_t;
typedef uint8_t                         scp_lane_index_t;
typedef scp_lane_index_t                scp_md_index_t;
typedef struct scp_address              scp_address_t;

typedef struct scp_ep_params {
    scp_address_t       *address;
} scp_ep_params_t;

typedef struct scp_event {
    /* one lane have self elem, index by remote ep idx */
    sct_event_t         sct_event[SCP_MAX_LANE];
} scp_event_t;
typedef struct scp_event *scp_event_h;

/*
 * Defines how a lane should be selected and used
 */
typedef enum {
    SCP_LANE_TYPE_FIRST,                    /* First item in enum */
    SCP_LANE_TYPE_SDMA,                     /* Remote memory access */
    SCP_LANE_TYPE_ROCE,                     /* High-BW remote memory access */
    SCP_LANE_TYPE_UB,
    SCP_LANE_TYPE_LAST
} scp_lane_type_t;

typedef enum {
    OFFLOAD_PUT,
    OFFLOAD_WAIT,
    OFFLOAD_BARRIER
} ofd_req_type_t;

typedef struct scp_ep *scp_ep_h;
typedef struct scp_worker *scp_worker_h;

typedef struct scp_ofd_stars_stream {
    uint8_t num;
    void **stars_handle;
} scp_ofd_stars_stream_t;
typedef struct scp_ofd_stars_stream *scp_ofd_stars_stream_h;

typedef struct scp_ofd_req_elem {
    void                    *lbuf;              /* local buffer used to storage send data */
    size_t                  llen;               /* send data length */
    sct_mem_h               *lmemh;             /* local buffer mr */
    void                    *rbuf;              /* remote buffer used to receive send data */
    uct_rkey_bundle_t       *rkey_bundle;
    uint8_t                 unnotify;           /* 0 means write_with_notify, 1 means write only */
    scp_event_h             scp_event;
    int                     flag;               /* value is 1 mean last elem in the queue */
    ucs_queue_elem_t        queue_elem;
    ofd_req_type_t          type;
    scp_ep_h                ep;
    uint8_t                 idx;
} scp_ofd_req_elem_t;
typedef struct scp_ofd_req_elem *scp_ofd_req_elem_h;

typedef ucs_status_t (*scp_ofd_req_cb_t)(void *arg);
typedef struct scp_ofd_req {
    ucs_queue_head_t        queue_head;
    uint16_t                req_elem_cnt;
    scp_ofd_req_elem_h      last_req_elem;
    ucg_status_t            result;
    sct_ofd_req_h           sct_req;                  /* mapping to stars handle, current mapping to iface */
    uint8_t                 sct_req_len;              /* arrary size */
    uint8_t                 submit_elem_cnt;
    uint8_t                 finish_elem_cnt;
    uint8_t                 flag_array[SCP_MAX_LANE]; /* use to record the last elem of each iface */
} scp_ofd_req_t;
typedef struct scp_ofd_req *scp_ofd_req_h;

#endif // XUCG_SCP_DEF_H
