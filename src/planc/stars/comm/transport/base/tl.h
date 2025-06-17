/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef STARS_SCT_TL_H
#define STARS_SCT_TL_H

#include "sct_def.h"


BEGIN_C_DECLS

/** @file tl.h */

/* endpoint - put */
typedef ucs_status_t (*sct_ep_put_with_notify_func_t)(sct_ep_h ep, sct_ofd_req_h req, const sct_iov_t *iov);

/* endpoint - wait */
typedef ucs_status_t (*sct_ep_wait_notify_func_t)(sct_ep_h ep, sct_ofd_req_h req, sct_wait_elem_h elem);

/* endpoint - barrier */
typedef ucs_status_t (*sct_ep_barrier_func_t)(sct_ep_h ep, sct_ofd_req_h req,
                                              sct_event_h *notify_event, sct_event_h *wait_event,
                                              int event_num);

/* endpoint - connection establishment */

typedef ucs_status_t (*sct_ep_create_func_t)(const sct_ep_params_t *params,
                                             sct_ep_h *ep_p);

typedef ucs_status_t (*sct_ep_disconnect_func_t)(sct_ep_h ep, unsigned flags);

typedef void         (*sct_ep_destroy_func_t)(sct_ep_h ep);

typedef ucs_status_t (*sct_ep_get_address_func_t)(sct_ep_h ep,
                                                  uct_ep_addr_t *addr);

typedef ucs_status_t (*sct_ep_connect_to_ep_func_t)(sct_ep_h ep,
                                                    const uct_device_addr_t *dev_addr,
                                                    const uct_ep_addr_t *ep_addr);

typedef ucs_status_t (*sct_ep_alloc_event_func_t)(sct_ep_h ep, sct_event_h event, uint8_t flag);

typedef ucs_status_t (*sct_iface_create_stars_stream_func_t)(sct_iface_h iface, void **handle_p, uint16_t stream_depth);

typedef ucs_status_t (*sct_iface_delete_stars_stream_func_t)(sct_iface_h iface, void *handle_p);

typedef ucs_status_t (*sct_iface_submit_req_func_t)(sct_iface_h iface, sct_ofd_req_h req);

/* interface - progress control */
typedef ucs_status_t (*sct_iface_notify_progress_func_t)(sct_iface_h iface);

/* interface - management */

typedef void         (*sct_iface_close_func_t)(sct_iface_h iface);

typedef ucs_status_t (*sct_iface_query_func_t)(sct_iface_h iface,
                                               sct_iface_attr_t *iface_attr);

/* interface - connection establishment */

typedef ucs_status_t (*sct_iface_get_device_address_func_tt)(sct_iface_h iface,
                                                             uct_device_addr_t *addr);

typedef ucs_status_t (*sct_iface_get_address_func_t)(sct_iface_h iface,
                                                     uct_iface_addr_t *addr);

typedef int          (*sct_iface_is_reachable_func_t)(const sct_iface_h iface,
                                                      const uct_device_addr_t *dev_addr,
                                                      const uct_iface_addr_t *iface_addr);

typedef uint8_t      (*sct_iface_get_stars_dev_id_func_t)(const sct_iface_h iface);
/**
 * Transport interface operations.
 * Every operation exposed in the API must appear in the table below, to allow
 * creating interface/endpoint with custom operations.
 */
typedef struct sct_iface_ops {
    /* endpoint - put */
    sct_ep_put_with_notify_func_t       ep_put_with_notify;

    /* endpoint - wait */
    sct_ep_wait_notify_func_t           ep_wait_notify;

    /* endpoint - barrier */
    sct_ep_barrier_func_t               ep_barrier;

    sct_ep_alloc_event_func_t           ep_alloc_event;

    /* endpoint - connection establishment */
    sct_ep_create_func_t                ep_create;
    sct_ep_disconnect_func_t            ep_disconnect;
    sct_ep_destroy_func_t               ep_destroy;
    sct_ep_get_address_func_t           ep_get_address;
    sct_ep_connect_to_ep_func_t         ep_connect_to_ep;

    sct_iface_create_stars_stream_func_t iface_create_stars_stream;

    sct_iface_delete_stars_stream_func_t iface_delete_stars_stream;

    /* interface - progress control */
    sct_iface_notify_progress_func_t    iface_notify_progress;

    sct_iface_submit_req_func_t         iface_submit_req;

    /* interface - management */
    sct_iface_close_func_t              iface_close;
    sct_iface_query_func_t              iface_query;

    /* interface - connection establishment */
    sct_iface_get_device_address_func_tt iface_get_device_address;
    sct_iface_get_address_func_t         iface_get_address;
    sct_iface_is_reachable_func_t        iface_is_reachable;

    sct_iface_get_stars_dev_id_func_t    iface_get_stars_dev_id;
} sct_iface_ops_t;

/**
 *  A progress engine and a domain for allocating communication resources.
 *  Different workers are progressed independently.
 */
typedef struct sct_worker {
} sct_worker_t;

/**
 * Communication interface context
 */
typedef struct sct_iface {
    sct_iface_ops_t          ops;
} sct_iface_t;

/**
 * Remote endpoint
 */
typedef struct sct_ep {
    sct_iface_h              iface;
} sct_ep_t;


END_C_DECLS

#endif