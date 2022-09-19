/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_REQUEST_H_
#define UCG_REQUEST_H_

#include "ucg/api/ucg.h"

#include "ucg_def.h"
#include "ucg_dt.h"

#include "util/ucg_class.h"
#include "util/ucg_list.h"

typedef enum {
    UCG_COLL_TYPE_BCAST,
    UCG_COLL_TYPE_ALLREDUCE,
    UCG_COLL_TYPE_BARRIER,
    UCG_COLL_TYPE_ALLTOALLV,
    UCG_COLL_TYPE_SCATTERV,
    UCG_COLL_TYPE_GATHERV,
    UCG_COLL_TYPE_ALLGATHERV,
    UCG_COLL_TYPE_REDUCE,
    UCG_COLL_TYPE_LAST,
} ucg_coll_type_t;

typedef struct ucg_coll_bcast_args {
    void *buffer;
    int32_t count;
    ucg_dt_t *dt;
    ucg_rank_t root;
} ucg_coll_bcast_args_t;

typedef struct ucg_coll_allreduce_args {
    const void *sendbuf;
    void *recvbuf;
    int32_t count;
    ucg_dt_t *dt;
    ucg_op_t *op;
    /* Use only at the ucg_request_allreduce_init(), not elsewhere. */
    ucg_op_generic_t gop;
    void *recvbuf_stored;
    void *staging_area_stored;
} ucg_coll_allreduce_args_t;

typedef struct ucg_coll_alltoallv_args {
    const void *sendbuf;
    const int32_t *sendcounts;
    const int32_t *sdispls;
    ucg_dt_t *sendtype;
    void *recvbuf;
    const int32_t *recvcounts;
    const int32_t *rdispls;
    ucg_dt_t *recvtype;
} ucg_coll_alltoallv_args_t;

typedef struct ucg_coll_gatherv_args {
    const void *sendbuf;
    int32_t sendcount;
    ucg_dt_t *sendtype;
    void *recvbuf;
    const int32_t *recvcounts;
    const int32_t *displs;
    ucg_dt_t *recvtype;
    ucg_rank_t root;
} ucg_coll_gatherv_args_t;

typedef struct ucg_coll_scatterv_args {
    const void *sendbuf;
    const int32_t *sendcounts;
    const int32_t *displs;
    ucg_dt_t *sendtype;
    void *recvbuf;
    int32_t recvcount;
    ucg_dt_t *recvtype;
    ucg_rank_t root;
} ucg_coll_scatterv_args_t;

typedef struct ucg_coll_allgatherv_args {
    const void *sendbuf;
    int32_t sendcount;
    ucg_dt_t *sendtype;
    void *recvbuf;
    const int32_t *recvcounts;
    const int32_t *displs;
    ucg_dt_t *recvtype;
} ucg_coll_allgatherv_args_t;

typedef struct ucg_coll_reduce_args {
    const void *sendbuf;
    void *recvbuf;
    int32_t count;
    ucg_dt_t *dt;
    ucg_op_t *op;
    ucg_rank_t root;
} ucg_coll_reduce_args_t;

typedef struct ucg_coll_args {
    ucg_coll_type_t type;
    ucg_request_info_t info;
    union {
        ucg_coll_bcast_args_t bcast;
        ucg_coll_allreduce_args_t allreduce;
        ucg_coll_alltoallv_args_t alltoallv;
        ucg_coll_scatterv_args_t scatterv;
        ucg_coll_gatherv_args_t gatherv;
        ucg_coll_allgatherv_args_t allgatherv;
        ucg_coll_reduce_args_t reduce;
    };
} ucg_coll_args_t;

typedef struct ucg_request {
    ucg_status_t status;
    ucg_coll_args_t args;
    ucg_group_t *group;
    ucg_list_link_t list; /* link to progress list */
    uint16_t id;
    char pending[32]; /* cacheline pending, `ucg_info -t` check struct size */
} ucg_request_t;
UCG_CLASS_DECLARE(ucg_request_t,
                  UCG_CLASS_CTOR_ARGS(const ucg_coll_args_t *arg));

ucg_status_t ucg_request_msg_size(const ucg_coll_args_t *args, const uint32_t size,
                                  uint32_t *msize);

const char* ucg_coll_type_string(ucg_coll_type_t coll_type);

#endif