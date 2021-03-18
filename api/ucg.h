/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2020.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_H_
#define UCG_H_

#include <ucg/api/ucg_def.h>
#include <ucg/api/ucg_version.h>

#include <ucp/api/ucp.h>
#include <uct/api/uct.h>
#include <ucs/type/thread_mode.h>
#include <ucs/type/cpu_set.h>
#include <ucs/config/types.h>
#include <ucs/sys/compiler_def.h>
#include <stdio.h>
#include <sys/types.h>

BEGIN_C_DECLS

/**
 * @ingroup UCG_GROUP
 * @brief UCG group collective operation description.
 *
 * The enumeration allows specifying modifiers to describe the requested
 * collective operation, as part of @ref ucg_collective_params_t
 * passed to @ref ucg_collective_start .
 */
enum ucg_collective_modifiers {
    /* Network Pattern Considerations */
    UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_SOURCE      = UCS_BIT(0), /* otherwise from all */
    UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_DESTINATION = UCS_BIT(1), /* otherwise to all */
    UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE          = UCS_BIT(2), /* otherwise gather */
    UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST          = UCS_BIT(3), /* otherwise scatter */
    UCG_GROUP_COLLECTIVE_MODIFIER_VARIABLE_LENGTH    = UCS_BIT(4), /* otherwise fixed length */
    UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE_PARTIAL  = UCS_BIT(5), /* MPI_Scan */
    UCG_GROUP_COLLECTIVE_MODIFIER_NEIGHBOR           = UCS_BIT(6), /* Neighbor collectives */

    /* Buffer/Data Management Considerations */
    UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE_STABLE   = UCS_BIT(7), /* stable reduction */
    UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE_EXCLUDE  = UCS_BIT(8), /* MPI_Exscan */
    UCG_GROUP_COLLECTIVE_MODIFIER_IN_PLACE           = UCS_BIT(9), /* otherwise two buffers */
    UCG_GROUP_COLLECTIVE_MODIFIER_VARIABLE_DATATYPE  = UCS_BIT(10), /* otherwise fixed data-type */
    UCG_GROUP_COLLECTIVE_MODIFIER_PERSISTENT         = UCS_BIT(11), /* otherwise destroy coll_h */
    UCG_GROUP_COLLECTIVE_MODIFIER_BARRIER            = UCS_BIT(12), /* preven others from starting */

    UCG_GROUP_COLLECTIVE_MODIFIER_ALLTOALL           = UCS_BIT(13), /* MPI_ALLTOALL */
    UCG_GROUP_COLLECTIVE_MODIFIER_ALLGATHER          = UCS_BIT(14), /* MPI_ALLGATHER */

    UCG_GROUP_COLLECTIVE_MODIFIER_MASK               = UCS_MASK(16)
};

typedef struct ucg_collective_type {
    enum ucg_collective_modifiers modifiers :16;
    ucg_group_member_index_t      root :48;
} ucg_collective_type_t;

enum UCS_S_PACKED ucg_group_member_distance {
    UCG_GROUP_MEMBER_DISTANCE_SELF = 0,
    UCG_GROUP_MEMBER_DISTANCE_L3CACHE,
    UCG_GROUP_MEMBER_DISTANCE_SOCKET,
    UCG_GROUP_MEMBER_DISTANCE_HOST,
    UCG_GROUP_MEMBER_DISTANCE_NET,
    UCG_GROUP_MEMBER_DISTANCE_LAST
};

enum UCS_S_PACKED ucg_group_hierarchy_level {
    UCG_GROUP_HIERARCHY_LEVEL_NODE = 0,
    UCG_GROUP_HIERARCHY_LEVEL_SOCKET,
    UCG_GROUP_HIERARCHY_LEVEL_L3CACHE
};


/**
 * @ingroup UCG_CONTEXT
 * @brief UCG context parameters field mask.
 *
 * The enumeration allows specifying which fields in @ref ucg_params_t are
 * present. It is used to enable backward compatibility support.
 */
enum ucg_params_field {
    UCG_PARAM_FIELD_ADDRESS_CB      = UCS_BIT(0), /**< Peer address lookup */
    UCG_PARAM_FIELD_REDUCE_CB       = UCS_BIT(1), /**< Callback for reduce ops */
    UCG_PARAM_FIELD_COMMUTE_CB      = UCS_BIT(2), /**< Callback for check op communative */
    UCG_PARAM_FIELD_GLOBALIDX_CB    = UCS_BIT(3),
    UCG_PARAM_FIELD_DTCONVERT_CB    = UCS_BIT(4),
    UCG_PARAM_FIELD_DATATYPESPAN_CB = UCS_BIT(5),
    UCG_PARAM_FIELD_MPI_IN_PLACE    = UCS_BIT(6), /**< MPI_IN_PLACE value */
};

/**
 * @ingroup UCG_CONTEXT
 * @brief Creation parameters for the UCG context.
 *
 * The structure defines the parameters that are used during the UCG
 * initialization by @ref ucg_init .
 */
typedef struct ucg_params {
    /**
     * Mask of valid fields in this structure, using bits from @ref ucg_params_field.
     * Fields not specified in this mask will be ignored.
     * Provides ABI compatibility with respect to adding new fields.
     */
    uint64_t field_mask;

    /* Callback functions for address lookup, used at connection establishment */
    struct {
        ucg_addr_lookup_callback_t lookup_f;
        ucg_addr_release_callback_t release_f;
    } address;

    /* Callback function for mpi reduce */
    ucg_mpi_reduce_callback_t mpi_reduce_f;

    /* Callback function for checking op communative */
    ucg_mpi_op_is_commute_callback_t op_is_commute_f;

    /* Callback function for rank in MPI_COMM_WORLD */
    mpi_global_idx_t mpi_global_idx_f;

    /* Callback function for MPI datatype convert function */
    dt_convert_t mpi_dt_convert;

    /* Callback function for reduce buffer */
    dt_span_t mpi_datatype_span;

    /* The value of MPI_IN_PLACE */
    void *mpi_in_place;
} ucg_params_t;

enum ucg_group_params_field {
    UCG_GROUP_PARAM_FIELD_UCP_WORKER        = UCS_BIT(0),
    UCG_GROUP_PARAM_FIELD_ID                = UCS_BIT(1),
    UCG_GROUP_PARAM_FIELD_MEMBER_COUNT      = UCS_BIT(2),
    UCG_GROUP_PARAM_FIELD_TOPO_MAP          = UCS_BIT(3),
    UCG_GROUP_PARAM_FIELD_DISTANCE          = UCS_BIT(4),
    UCG_GROUP_PARAM_FIELD_NODE_INDEX        = UCS_BIT(5),
    UCG_GROUP_PARAM_FIELD_BIND_TO_NONE      = UCS_BIT(6),
    UCG_GROUP_PARAM_FIELD_CB_GROUP_IBJ      = UCS_BIT(7),
    UCG_GROUP_PARAM_FIELD_MPI_DT_CONVERT    = UCS_BIT(8),
    UCG_GROUP_PARAM_FIELD_MPI_DATATYPE_SPAN = UCS_BIT(9),
    UCG_GROUP_PARAM_FIELD_IS_SOCKET_BALANCE = UCS_BIT(10),
};

typedef struct {
    uint32_t rank_continuous_in_node : 1;
    uint32_t rank_continuous_in_sock : 1;
    uint32_t rank_balance_in_node    : 1;
    uint32_t rank_balance_in_sock    : 1;
} ucg_topo_args_t;

typedef struct ucg_group_params {
    /**
     * Mask of valid fields in this structure, using bits from @ref ucg_group_params_field.
     * Fields not specified in this mask will be ignored.
     * Provides ABI compatibility with respect to adding new fields.
     */
    uint64_t field_mask;

    /* UCP worker to create a ucg group on top of */
    ucp_worker_h ucp_worker;

    /* Unique group identifier */
    uint32_t group_id;

    /* number of group members */
    int member_count;

    ucg_topo_args_t topo_args;

    /*
     * This array contains information about the process placement of different
     * group members, which is used to select the best topology for collectives.
     *
     *
     * For example, for 2 nodes, 3 sockets each, 4 cores per socket, each member
     * should be passed the distance array contents as follows:
     *   1st group member distance array:  0111222222223333333333333333
     *   2nd group member distance array:  1011222222223333333333333333
     *   3rd group member distance array:  1101222222223333333333333333
     *   4th group member distance array:  1110222222223333333333333333
     *   5th group member distance array:  2222011122223333333333333333
     *   6th group member distance array:  2222101122223333333333333333
     *   7th group member distance array:  2222110122223333333333333333
     *   8th group member distance array:  2222111022223333333333333333
     *    ...
     *   12th group member distance array: 3333333333333333011122222222
     *   13th group member distance array: 3333333333333333101122222222
     *    ...
     */
    enum ucg_group_member_distance *distance;

    /* node index */
    uint16_t *node_index;

    /* bind-to none flag */
    unsigned is_bind_to_none;

    /* external group object for call-backs (MPI_Comm) */
    void *cb_group_obj;
    char is_socket_balance;
} ucg_group_params_t;

typedef struct ucg_collective {
    ucg_collective_type_t     type;    /* the type (and root) of the collective */
    ucg_hash_index_t          plan_cache_index; /* the index of collective type in plan cache. */

    struct {
        void                 *buf;     /* buffer location to use */
        union {
            int               count;   /* item count */
            int              *counts;  /* item count array */
        };
        union {
            size_t            dt_len;  /* external datatype length */
            size_t           *dts_len; /* external datatype length array */
        };
        union {
            void             *dt_ext;  /* external datatype context */
            void             *dts_ext; /* external datatype context array */
        };
        union {
            int              *displs;  /* item displacement array */
            void             *op_ext;  /* external reduce operation handle */
        };
    } send, recv;

    ucg_collective_callback_t comp_cb; /* completion callback */

} ucg_collective_params_t;

/** @cond PRIVATE_INTERFACE */
/**
 * @ingroup UCG_CONTEXT
 * @brief UCG context initialization with particular API version
 *
 * This is an internal routine used to check compatibility with a paticular
 * API version. @ref ucg_init should be used to create UCG context
 */
ucs_status_t ucg_init_version(unsigned api_major_version,
                              unsigned api_minor_version,
                              const ucg_params_t *params,
                              const ucg_config_t *config,
                              ucg_context_h *ctx_p);
/** @endcond */

/**
 * @ingroup UCG_CONTEXT
 * @brief UCG context initialization.
 * @warning This routine must be called before any other UCG function
 * call in the application.
 */
ucs_status_t ucg_init(const ucg_params_t *params,
                      const ucg_config_t *config,
                      ucg_context_h *ctx_p);


/**
 * @ingroup UCG_CONTEXT
 * @brief Release UCG application context.
 *
 * This routine finalizes and releases the resources associated with a
 * @ref ucg_context_h "UCG application context".
 *
 * @warning An application cannot call any UCG routine
 * once the UCG application context released.
 */
void ucg_cleanup(ucg_context_h ctx_p);


/**
 * @ingroup UCG_GROUP
 * @brief Create a group object.
 *
 * This routine allocates and initializes a @ref ucg_group_h "group" object.
 * This routine is a "collective operation", meaning it has to be called for
 * each worker participating in the group - before the first call on the group
 * is invoked on any of those workers. The call does not contain a barrier,
 * meaning a call on one worker can complete regardless of call on others.
 *
 * @note The group object is allocated within context of the calling thread
 *
 * @param [in] context     Handle to @ref ucg_context_h
 * @param [in] params      User defined @ref ucg_group_params_t configurations for the
 *                         @ref ucg_group_h "UCG group".
 * @param [out] group_p    A pointer to the group object allocated by the
 *                         UCG library
 *
 * @return Error code as defined by @ref ucs_status_t
 */
ucs_status_t ucg_group_create(ucg_context_h context,
                              const ucg_group_params_t *params,
                              ucg_group_h *group_p);


/**
 * @ingroup UCG_GROUP
 * @brief Destroy a group object.
 *
 * This routine releases the resources associated with a @ref ucg_group_h
 * "UCG group". This routine is also a "collective operation", similarly to
 * @ref ucg_group_create, meaning it must be called on each worker participating
 * in the group.
 *
 * @warning Once the UCG group handle is destroyed, it cannot be used with any
 * UCG routine.
 *
 * The destroy process releases and shuts down all resources associated with
 * the @ref ucg_group_h "group".
 *
 * @param [in]  group       Group object to destroy.
 */
void ucg_group_destroy(ucg_group_h group);


/**
 * @ingroup UCG_GROUP
 * @brief Progresses a Group object.
 *
 * @param [in]  group       Group object to progress.
 */
unsigned ucg_group_progress(ucg_group_h group);


/**
 * @ingroup UCG_GROUP
 * @brief Creates a collective operation on a group object.
 * The parameters are intentionally non-constant, to allow UCG to write-back some
 * information and avoid redundant actions on the next call. For example, memory
 * registration handles are written back to the parameters pointer passed to the
 * function, and are re-used in subsequent calls.
 *
 * @param [in]  group       Group object to use.
 * @param [in]  params      Collective operation parameters.
 * @param [out] coll        Collective operation handle.
 *
 * @return Error code as defined by @ref ucs_status_t
 */
ucs_status_t ucg_collective_create(ucg_group_h group,
                                   ucg_collective_params_t *params,
                                   ucg_coll_h *coll);


/**
 * @ingroup UCG_GROUP
 * @brief Starts a collective operation.
 *
 * @param [in]  coll        Collective operation handle.
 *
 * @return UCS_OK           - The collective operation was completed immediately.
 * @return UCS_PTR_IS_ERR(_ptr) - The collective operation failed.
 * @return otherwise        - Operation was scheduled for send and can be
 *                          completed in any point in time. The request handle
 *                          is returned to the application in order to track
 *                          progress of the message.
 */
ucs_status_ptr_t ucg_collective_start_nb(ucg_coll_h coll);


/**
 * @ingroup UCG_GROUP
 * @brief Starts a collective operation.
 *
 * @param [in]  coll        Collective operation handle.
 * @param [in]  req         Request handle allocated by the user. There should
 *                          be at least UCG request size bytes of available
 *                          space before the @a req. The size of UCG request
 *                          can be obtained by @ref ucg_context_query function.
 *
 * @return UCS_OK           - The collective operation was completed immediately.
 * @return UCS_INPROGRESS   - The collective was not completed and is in progress.
 *                            @ref ucg_request_check_status() should be used to
 *                            monitor @a req status.
 * @return Error code as defined by @ref ucs_status_t
 */
ucs_status_t ucg_collective_start_nbr(ucg_coll_h coll, void *req);


/**
 * @ingroup UCG_GROUP
 * @brief Destroys a collective operation handle.
 *
 * This is only required for persistent collectives, where the flag
 * UCG_GROUP_COLLECTIVE_MODIFIER_PERSISTENT is passed when calling
 * @ref ucg_collective_create. Otherwise, the handle is
 * destroyed when the collective operation is completed.
 *
 * @param [in]  coll         Collective operation handle.
 *
 * @return Error code as defined by @ref ucs_status_t
 */
void ucg_collective_destroy(ucg_coll_h coll);


/**
 * @ingroup UCG_GROUP
 * @brief Check the status of non-blocking request.
 *
 * This routine checks the state of the request and returns its current status.
 * Any value different from UCS_INPROGRESS means that request is in a completed
 * state.
 *
 * @param [in]  request     Non-blocking request to check.
 *
 * @return Error code as defined by @ref ucs_status_t
 */
ucs_status_t ucg_request_check_status(void *request);

/**
 * @ingroup UCG_GROUP
 * @brief Cancel the non-blocking request.
 */
void ucg_request_cancel(ucg_group_h group, void *request);

/**
 * @ingroup UCG_GROUP
 * @brief free the non-blocking request.
 */
void ucg_request_free(void *request);


/**
 * @ingroup UCG_CONFIG
 * @brief Read UCG configuration descriptor
 *
 * The routine fetches the information about UCG library configuration from
 * the run-time environment. Then, the fetched descriptor is used for
 * UCG library @ref ucg_init "initialization". The Application can print out the
 * descriptor using @ref ucg_config_print "print" routine. In addition
 * the application is responsible for @ref ucg_config_release "releasing" the
 * descriptor back to the UCG library.
 *
 * @param [in]  env_prefix    If non-NULL, the routine searches for the
 *                            environment variables that start with
 *                            @e \<env_prefix\>_UCX_ prefix.
 *                            Otherwise, the routine searches for the
 *                            environment variables that start with
 *                            @e UCX_ prefix.
 * @param [in]  filename      If non-NULL, read configuration from the file
 *                            defined by @e filename. If the file does not
 *                            exist, it will be ignored and no error reported
 *                            to the application.
 * @param [out] config_p      Pointer to configuration descriptor as defined by
 *                            @ref ucg_config_t "ucg_config_t".
 *
 * @return Error code as defined by @ref ucs_status_t
 */
ucs_status_t ucg_config_read(const char *env_prefix, const char *filename,
                             ucg_config_t **config_p);

/**
 * @ingroup UCG_CONFIG
 * @brief Release configuration descriptor
 *
 * The routine releases the configuration descriptor that was allocated through
 * @ref ucg_config_read "ucg_config_read()" routine.
 *
 * @param [out] config        Configuration descriptor as defined by
 *                            @ref ucg_config_t "ucg_config_t".
 */
void ucg_config_release(ucg_config_t *config);

/**
 * @ingroup UCG_CONFIG
 * @brief Modify context configuration.
 *
 * The routine changes one configuration setting stored in @ref ucg_config_t
 * "configuration" descriptor.
 *
 * @param [in]  config        Configuration to modify.
 * @param [in]  name          Configuration variable name.
 * @param [in]  value         Value to set.
 *
 * @return Error code.
 */
ucs_status_t ucg_config_modify(ucg_config_t *config, const char *name,
                               const char *value);


/**
 * @ingroup UCG_CONFIG
 * @brief Print configuration information
 *
 * The routine prints the configuration information that is stored in
 * @ref ucg_config_t "configuration" descriptor.
 *
 * @todo Expose ucs_config_print_flags_t
 *
 * @param [in]  config        @ref ucg_config_t "Configuration descriptor"
 *                            to print.
 * @param [in]  stream        Output stream to print the configuration to.
 * @param [in]  title         Configuration title to print.
 * @param [in]  print_flags   Flags that control various printing options.
 */
void ucg_config_print(const ucg_config_t *config, FILE *stream,
                      const char *title, ucs_config_print_flags_t print_flags);


/**
 * @ingroup UCG_CONTEXT
 * @brief Get UCG library version.
 *
 * This routine returns the UCG library version.
 *
 * @param [out] major_version       Filled with library major version.
 * @param [out] minor_version       Filled with library minor version.
 * @param [out] release_number      Filled with library release number.
 */
void ucg_get_version(unsigned *major_version, unsigned *minor_version,
                     unsigned *release_number);

/**
 * @ingroup UCG_CONTEXT
 * @brief Get UCG library version as a string.
 *
 * This routine returns the UCG library version as a string which consists of:
 * "major.minor.release".
 */
const char* ucg_get_version_string(void);

END_C_DECLS

#endif
