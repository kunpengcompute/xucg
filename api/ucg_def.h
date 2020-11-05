/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2020.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_DEF_H_
#define UCG_DEF_H_

#include <ucp/api/ucp.h>
#include <ucs/type/status.h>
#include <ucs/config/types.h>
#include <stddef.h>
#include <stdint.h>

/**
 * @ingroup UCG_CONTEXT
 * @brief UCG Application Context
 */
typedef struct ucg_context               *ucg_context_h;

/**
 * @ingroup UCG_CONFIG
 * @brief UCG configuration descriptor
 */
typedef struct ucg_config                ucg_config_t;

 /**
  * @ingroup UCG_GROUP
  * @brief UCG Group
  *
  * UCG group is an opaque object representing a set of connected remote workers.
  * This object is used for collective operations - like the ones defined by the
  * Message Passing Interface (MPI). Groups are created with respect to a local
  * worker, and share its endpoints for communication with the remote workers.
  */
typedef struct ucg_group                *ucg_group_h;


 /**
  * @ingroup UCG_GROUP
  * @brief UCG collective operation handle
  *
  * UCG collective is an opaque object representing a description of a collective
  * operation. Much like in object-oriented paradigms, a collective is like a
  * "class" which can be instantiated - an instance would be a UCG request to
  * perform this collective operation once. The description holds all the
  * necessary information to perform collectives, so re-starting an operation
  * requires no additional parameters.
  */
typedef void                            *ucg_coll_h;


/**
 * @ingroup UCG_GROUP
 * @brief UCG group member index.
 *
 * UCG groups have multiple peers: remote worker objects acting as group members.
 * Each group member, including the local worker which was used to create the
 * group, has an unique identifier within the group - an integer between 0 and
 * the number of peers in it. The same worker may have different identifiers
 * in different groups, identifiers which are passed by user during creation.
 */
typedef uint64_t                         ucg_group_member_index_t;

/**
 * @ingroup UCP_GROUP
 * @brief Completion callback for non-blocking collective operations.
 *
 * This callback routine is invoked whenever the @ref ucg_collective
 * "collective operation" is completed. It is important to note that the call-back is
 * only invoked in a case when the operation cannot be completed in place.
 *
 * @param [in]  request   The completed collective operation request.
 * @param [in]  status    Completion status. If the send operation was completed
 *                        successfully UCX_OK is returned. If send operation was
 *                        canceled UCS_ERR_CANCELED is returned.
 *                        Otherwise, an @ref ucs_status_t "error status" is
 *                        returned.
 */
typedef void (*ucg_collective_callback_t)(void *request, ucs_status_t status);

/**
 * @ingroup ucg_collective
 * @brief Hash index for each hash table.
 *
 * This type is used as index of hash array.
 */
typedef uint32_t                         ucg_hash_index_t;

/**
 * @ingroup UCG_GROUP
 * @brief Lookup address by member index.
 */
typedef ucs_status_t (*ucg_addr_lookup_callback_t)(void *cb_group_obj,
                                                   ucg_group_member_index_t index,
                                                   ucp_address_t **addr,
                                                   size_t *addr_len);

/**
 * @ingroup UCG_GROUP
 * @brief Release address returned by lookup.
 */
typedef void (*ucg_addr_release_callback_t)(ucp_address_t *addr);

/**
 * @ingroup UCG_GROUP
 * @brief Perform a reduction operation.
 */
typedef void (*ucg_mpi_reduce_callback_t)(void *mpi_op,
                                          void *src,
                                          void *dst,
                                          int count,
                                          void *mpi_dtype);

/**
 * @ingroup UCG_GROUP
 * @brief Check to see if an op is communative or not
 */
typedef int (*ucg_mpi_op_is_commute_callback_t)(void *mpi_op);

#endif