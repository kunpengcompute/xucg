/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef STARS_SCT_H
#define STARS_SCT_H

#include "tl.h"


BEGIN_C_DECLS

/** @file uct.h */

/**
 * @defgroup UCT_API Unified Communication Transport (UCT) API
 * @{
 * This section describes UCT API.
 * @}
 */

/**
* @defgroup UCT_RESOURCE   UCT Communication Resource
* @ingroup UCT_API
* @{
* This section describes a concept of the Communication Resource and routines
* associated with the concept.
* @}
*/

/**
 * @defgroup UCT_CONTEXT    UCT Communication Context
 * @ingroup UCT_API
 * @{
 *
 * UCT context abstracts all the resources required for network communication.
 * It is designed to enable either share or isolate resources for multiple
 * programming models used by an application.
 *
 * This section provides a detailed description of this concept and
 * routines associated with it.
 *
 * @}
 */

/**
 * @defgroup UCT_MD    UCT Memory Domain
 * @ingroup UCT_API
 * @{
 * The Memory Domain abstracts resources required for network communication,
 * which typically includes memory, transport mechanisms, compute and
 * network resources. It is an isolation  mechanism that can be employed
 * by the applications for isolating resources between multiple programming models.
 * The attributes of the Memory Domain are defined by the structure @ref uct_md_attr().
 * The communication and memory operations are defined in the context of Memory Domain.
 *
 * @}
 */

/**
 * @defgroup UCT_RMA  UCT Remote memory access operations
 * @ingroup UCT_API
 * @{
 * Defines remote memory access operations.
 * @}
 */

/**
 * @ingroup UCT_RESOURCE
 * @brief Communication resource descriptor.
 *
 * Resource descriptor is an object representing the network resource.
 * Resource descriptor could represent a stand-alone communication resource
 * such as an HCA port, network interface, or multiple resources such as
 * multiple network interfaces or communication ports. It could also represent
 * virtual communication resources that are defined over a single physical
 * network interface.
 */
typedef struct sct_tl_resource_desc {
    char                     tl_name[UCT_TL_NAME_MAX];   /**< Transport name */
    char                     dev_name[UCT_DEVICE_NAME_MAX]; /**< Hardware device name */
    uct_device_type_t        dev_type;     /**< The device represented by this resource
                                                (e.g. UCT_DEVICE_TYPE_NET for a network interface) */
    ucs_sys_device_t         sys_device;   /**< The identifier associated with the device
                                                bus_id as captured in ucs_sys_bus_id_t struct */
} sct_tl_resource_desc_t;


#define SCT_IFACE_FLAG_PUT_WITH_NOTIFY  UCS_BIT(54) /**< Hardware one-side support */
#define SCT_IFACE_FLAG_WAIT_NOTIFY      UCS_BIT(55)

/**
 * @ingroup UCT_RESOURCE
 * @brief Interface attributes: capabilities and limitations.
 */
struct sct_iface_attr {
    struct {
        struct {
            size_t           align_mtu;       /**< MTU used for alignment */
            size_t           max_iov;    /**< Maximal @a iovcnt parameter in
                                              @ref ::sct_ep_put_zcopy
                                              @anchor uct_iface_attr_cap_put_max_iov */
        } put;                           /**< Attributes for PUT operations */

        struct {
            size_t           align_mtu;       /**< MTU used for alignment */
            size_t           max_iov;    /**< Maximal @a iovcnt parameter in
                                              @ref sct_ep_get_zcopy
                                              @anchor uct_iface_attr_cap_get_max_iov */
        } get;                           /**< Attributes for GET operations */

        uint64_t             flags;      /**< Flags from @ref UCT_RESOURCE_IFACE_CAP */
        uint64_t             event_flags;/**< Flags from @ref UCT_RESOURCE_IFACE_EVENT_CAP */
    } cap;                               /**< Interface capabilities */

    size_t                   device_addr_len;/**< Size of device address */
    size_t                   iface_addr_len; /**< Size of interface address */
    size_t                   ep_addr_len;    /**< Size of endpoint address */
    /*
     * The following fields define expected performance of the communication
     * interface, this would usually be a combination of device and system
     * characteristics and determined at run time.
     */
    double                   overhead;     /**< Message overhead, seconds */
    uct_ppn_bandwidth_t      bandwidth;    /**< Bandwidth model */
    ucs_linear_func_t        latency;      /**< Latency as function of number of
                                                active endpoints */
    uint8_t                  priority;     /**< Priority of device */
    size_t                   max_num_eps;  /**< Maximum number of endpoints */
    unsigned                 dev_num_paths;/**< How many network paths can be
                                                utilized on the device used by
                                                this interface for optimal
                                                performance. Endpoints that connect
                                                to the same remote address but use
                                                different paths can potentially
                                                achieve higher total bandwidth
                                                compared to using only a single
                                                endpoint. */
};


/**
 * @ingroup UCT_RESOURCE
 * @brief Parameters used for interface creation.
 *
 * This structure should be allocated by the user and should be passed to
 * @ref sct_iface_open. User has to initialize all fields of this structure.
 */
struct sct_iface_params {
    /** Mask of valid fields in this structure, using bits from
     *  @ref uct_iface_params_field. Fields not specified in this mask will be
     *  ignored. */
    uint64_t                                     field_mask;
    /** Mask of CPUs to use for resources */
    ucs_cpu_set_t                                cpu_mask;
    /** Interface open mode bitmap. @ref uct_iface_open_mode */
    uint64_t                                     open_mode;
    /** Mode-specific parameters */
    union {
        /** @anchor uct_iface_params_t_mode_device
         *  The fields in this structure (tl_name and dev_name) need to be set only when
         *  the @ref UCT_IFACE_OPEN_MODE_DEVICE bit is set in @ref
         *  sct_iface_params_t.open_mode This will make @ref sct_iface_open
         *  open the interface on the specified device.
         */
        struct {
            const char                           *tl_name;  /**< Transport name */
            const char                           *dev_name; /**< Device Name */
        } device;
    } mode;
};


/**
 * @ingroup UCT_RESOURCE
 * @brief Parameters for creating a UCT endpoint by @ref sct_ep_create
 */
struct sct_ep_params {
    /**
     * Mask of valid fields in this structure, using bits from
     * @ref uct_ep_params_field. Fields not specified by this mask will be
     * ignored.
     */
    uint64_t                          field_mask;

    /**
     * Interface to create the endpoint on.
     * Either @a iface or @a cm field must be initialized but not both.
     */
    sct_iface_h                       iface;

    /**
     * User data associated with the endpoint.
     */
    void                              *user_data;

    /**
     * The device address to connect to on the remote peer. This must be defined
     * together with @ref sct_ep_params_t::iface_addr to create an endpoint
     * connected to a remote interface.
     */
    const uct_device_addr_t           *dev_addr;

    /**
     * This specifies the remote address to use when creating an endpoint that
     * is connected to a remote interface.
     * @note This requires @ref UCT_IFACE_FLAG_CONNECT_TO_IFACE capability.
     */
    const uct_iface_addr_t            *iface_addr;

    /**
     * Index of the path which the endpoint should use, must be in the range
     * 0..(@ref sct_iface_attr_t.dev_num_paths - 1).
     */
    unsigned                            path_index;
};


/**
 * @ingroup UCT_MD
 * @brief Query attributes of a given pointer
 *
 * Return attributes such as memory type, and system device for the
 * given pointer of specific length.
 *
 * @param [in]     md          Memory domain to run the query on. This function
 *                             returns an error if the md does not recognize the
 *                             pointer.
 * @param [in]     address     The address of the pointer. Must be non-NULL
 *                             else UCS_ERR_INVALID_PARAM error is returned.
 * @param [in]     length      Length of the memory region to examine.
 *                             Must be nonzero else UCS_ERR_INVALID_PARAM error
 *                             is returned.
 * @param [out]    mem_attr    If successful, filled with ptr attributes.
 *
 * @return Error code.
 */
ucs_status_t sct_md_mem_query(sct_md_h md, const void *address, const size_t length,
                              uct_md_mem_attr_t *mem_attr);


/**
 * @ingroup UCT_MD
 * @brief Describes a memory allocated by UCT.
 *
 * This structure describes the memory block which includes the address, size, and
 * Memory Domain used for allocation. This structure is passed to interface
 * and the memory is allocated by memory allocation functions @ref sct_mem_alloc.
 */
typedef struct sct_allocated_memory {
    void                     *address; /**< Address of allocated memory */
    size_t                   length;   /**< Real size of allocated memory */
    uct_alloc_method_t       method;   /**< Method used to allocate the memory */
    ucs_memory_type_t        mem_type; /**< type of allocated memory */
    sct_md_h                 md;       /**< if method==MD: MD used to allocate the memory */
    sct_mem_h                memh;     /**< if method==MD: MD memory handle */
} sct_allocated_memory_t;


extern const char *sct_alloc_method_names[];

/**
 * @ingroup UCT_RESOURCE

 * @brief Query for list of components.
 *
 * Obtain the list of transport components available on the current system.
 *
 * @param [out] components_p      Filled with a pointer to an array of component
 *                                handles.
 * @param [out] num_components_p  Filled with the number of elements in the array.
 *
 * @return UCS_OK if successful, or UCS_ERR_NO_MEMORY if failed to allocate the
 *         array of component handles.
 */
ucs_status_t sct_query_components(sct_component_h **components_p,
                                  unsigned *num_components_p);


/**
 * @ingroup UCT_RESOURCE
 * @brief Release the list of components returned from @ref sct_query_components.
 *
 * This routine releases the memory associated with the list of components
 * allocated by @ref sct_query_components.
 *
 * @param [in] components  Array of component handles to release.
 */
void sct_release_component_list(sct_component_h *components);


/**
 * @ingroup UCT_RESOURCE
 * @brief Get component attributes
 *
 * Query various attributes of a component.
 *
 * @param [in] component          Component handle to query attributes for. The
 *                                handle can be obtained from @ref sct_query_components.
 * @param [inout] component_attr  Filled with component attributes.
 *
 * @return UCS_OK if successful, or nonzero error code in case of failure.
 */
ucs_status_t sct_component_query(sct_component_h component,
                                 uct_component_attr_t *component_attr);


/**
 * @ingroup UCT_RESOURCE
 * @brief Open a memory domain.
 *
 * Open a specific memory domain. All communications and memory operations
 * are performed in the context of a specific memory domain. Therefore it
 * must be created before communication resources.
 *
 * @param [in]  component       Component on which to open the memory domain,
 *                              as returned from @ref sct_query_components.
 * @param [in]  md_name         Memory domain name, as returned from @ref
 *                              sct_component_query.
 * @param [in]  config          MD configuration options. Should be obtained
 *                              from sct_md_config_read() function, or point to
 *                              MD-specific structure which extends sct_md_config_t.
 * @param [out] md_p            Filled with a handle to the memory domain.
 *
 * @return Error code.
 */
ucs_status_t sct_md_open(sct_component_h component, const char *md_name,
                         const sct_md_config_t *config, uct_md_h context, sct_md_h *md_p);

/**
 * @ingroup UCT_RESOURCE
 * @brief Close a memory domain.
 *
 * @param [in]  md               Memory domain to close.
 */
void sct_md_close(sct_md_h md);


/**
 * @ingroup UCT_RESOURCE
 * @brief Query for transport resources.
 *
 * This routine queries the @ref sct_md_h "memory domain" for communication
 * resources that are available for it.
 *
 * @param [in]  md              Handle to memory domain.
 * @param [out] resources_p     Filled with a pointer to an array of resource
 *                              descriptors.
 * @param [out] num_resources_p Filled with the number of resources in the array.
 *
 * @return Error code.
 */
ucs_status_t sct_md_query_tl_resources(sct_md_h md,
                                       sct_tl_resource_desc_t **resources_p,
                                       uint8_t *num_resources_p);


/**
 * @ingroup UCT_RESOURCE
 * @brief Release the list of resources returned from @ref uct_md_query_tl_resources.
 *
 * This routine releases the memory associated with the list of resources
 * allocated by @ref uct_md_query_tl_resources.
 *
 * @param [in] resources  Array of resource descriptors to release.
 */
void sct_release_tl_resource_list(sct_tl_resource_desc_t *resources);


/**
 * @ingroup UCT_CONTEXT
 * @brief Create a worker object.
 *
 *  The worker represents a progress engine. Multiple progress engines can be
 * created in an application, for example to be used by multiple threads.
 *  Transports can allocate separate communication resources for every worker,
 * so that every worker can be progressed independently of others.
 *
 * @param [in]  async         Context for async event handlers. Must not be NULL.
 * @param [in]  thread_mode   Thread access mode to the worker and all interfaces
 *                             and endpoints associated with it.
 * @param [out] worker_p      Filled with a pointer to the worker object.
 */
ucs_status_t sct_worker_create(pthread_mutex_t *async,
                               ucs_thread_mode_t thread_mode,
                               sct_worker_h *worker_p);


/**
 * @ingroup UCT_CONTEXT
 * @brief Destroy a worker object.
 *
 * @param [in]  worker        Worker object to destroy.
 */
void sct_worker_destroy(sct_worker_h worker);


/**
 * @ingroup UCT_RESOURCE
 * @brief Read transport-specific interface configuration.
 *
 * @param [in]  md            Memory domain on which the transport's interface
 *                            was registered.
 * @param [in]  tl_name       Transport name. If @e md supports
 *                            @ref UCT_MD_FLAG_SOCKADDR, the transport name
 *                            is allowed to be NULL. In this case, the configuration
 *                            returned from this routine should be passed to
 *                            @ref sct_iface_open with
 *                            @ref UCT_IFACE_OPEN_MODE_SOCKADDR_SERVER or
 *                            @ref UCT_IFACE_OPEN_MODE_SOCKADDR_CLIENT set in
 *                            @ref sct_iface_params_t.open_mode.
 *                            In addition, if tl_name is not NULL, the configuration
 *                            returned from this routine should be passed to
 *                            @ref sct_iface_open with @ref UCT_IFACE_OPEN_MODE_DEVICE
 *                            set in @ref sct_iface_params_t.open_mode.
 * @param [in]  env_prefix    If non-NULL, search for environment variables
 *                            starting with this UCT_<prefix>_. Otherwise, search
 *                            for environment variables starting with just UCT_.
 * @param [in]  filename      If non-NULL, read configuration from this file. If
 *                            the file does not exist, it will be ignored.
 * @param [out] config_p      Filled with a pointer to configuration.
 *
 * @return Error code.
 */
ucs_status_t sct_md_iface_config_read(sct_md_h md, const char *tl_name,
                                      const char *env_prefix, const char *filename,
                                      sct_iface_config_t **config_p);

/**
 * @ingroup UCT_RESOURCE
 * @brief Release configuration memory returned from uct_md_iface_config_read(),
 * uct_md_config_read(), or from uct_cm_config_read().
 *
 * @param [in]  config        Configuration to release.
 */
void sct_config_release(void *config);


/**
 * @ingroup UCT_CONTEXT
 * @brief Get value by name from interface configuration (@ref sct_iface_config_t),
 *        memory domain configuration (@ref uct_md_config_t)
 *        or connection manager configuration (@ref uct_cm_config_t).
 *
 * @param [in]  config        Configuration to get from.
 * @param [in]  name          Configuration variable name.
 * @param [out] value         Pointer to get value. Should be allocated/freed by
 *                            caller.
 * @param [in]  max           Available memory space at @a value pointer.
 *
 * @return UCS_OK if found, otherwise UCS_ERR_INVALID_PARAM or UCS_ERR_NO_ELEM
 *         if error.
 */
ucs_status_t sct_config_get(void *config, const char *name, char *value,
                            size_t max);

/**
 * @ingroup UCT_RESOURCE
 * @brief Open a communication interface.
 *
 * @param [in]  md            Memory domain to create the interface on.
 * @param [in]  worker        Handle to worker which will be used to progress
 *                            communications on this interface.
 * @param [in]  params        User defined @ref sct_iface_params_t parameters.
 * @param [in]  config        Interface configuration options. Should be obtained
 *                            from sct_md_iface_config_read() function, or point to
 *                            transport-specific structure which extends sct_iface_config_t.
 * @param [out] iface_p       Filled with a handle to opened communication interface.
 *
 * @return Error code.
 */
ucs_status_t sct_iface_open(sct_md_h md, sct_worker_h worker,
                            const sct_iface_params_t *params,
                            const sct_iface_config_t *config,
                            sct_iface_h *iface_p);


/**
 * @ingroup UCT_RESOURCE
 * @brief Close and destroy an interface.
 *
 * @param [in]  iface  Interface to close.
 */
void sct_iface_close(sct_iface_h iface);


/**
 * @ingroup UCT_RESOURCE
 * @brief Get interface attributes.
 *
 * @param [in]  iface      Interface to query.
 * @param [out] iface_attr Filled with interface attributes.
 */
ucs_status_t sct_iface_query(sct_iface_h iface, sct_iface_attr_t *iface_attr);


/**
 * @ingroup UCT_RESOURCE
 * @brief Get address of the device the interface is using.
 *
 *  Get underlying device address of the interface. All interfaces using the same
 * device would return the same address.
 *
 * @param [in]  iface       Interface to query.
 * @param [out] addr        Filled with device address. The size of the buffer
 *                          provided must be at least @ref sct_iface_attr_t::device_addr_len.
 */
ucs_status_t sct_iface_get_device_address(sct_iface_h iface, uct_device_addr_t *addr);


/**
 * @ingroup UCT_RESOURCE
 * @brief Get interface address.
 *
 * requires @ref UCT_IFACE_FLAG_CONNECT_TO_IFACE.
 *
 * @param [in]  iface       Interface to query.
 * @param [out] addr        Filled with interface address. The size of the buffer
 *                          provided must be at least @ref sct_iface_attr_t::iface_addr_len.
 */
ucs_status_t sct_iface_get_address(sct_iface_h iface, uct_iface_addr_t *addr);


/**
 * @ingroup UCT_RESOURCE
 * @brief Check if remote iface address is reachable.
 *
 * This function checks if a remote address can be reached from a local interface.
 * If the function returns true, it does not necessarily mean a connection and/or
 * data transfer would succeed, since the reachability check is a local operation
 * it does not detect issues such as network mis-configuration or lack of connectivity.
 *
 * @param [in]  iface      Interface to check reachability from.
 * @param [in]  dev_addr   Device address to check reachability to. It is NULL
 *                         if iface_attr.dev_addr_len == 0, and must be non-NULL otherwise.
 * @param [in]  iface_addr Interface address to check reachability to. It is
 *                         NULL if iface_attr.iface_addr_len == 0, and must
 *                         be non-NULL otherwise.
 *
 * @return Nonzero if reachable, 0 if not.
 */
int sct_iface_is_reachable(const sct_iface_h iface, const uct_device_addr_t *dev_addr,
                           const uct_iface_addr_t *iface_addr);


/**
 * @ingroup UCT_RESOURCE
 * @brief Allocate memory which can be used for zero-copy communications.
 *
 * Allocate a region of memory which can be used for zero-copy data transfer or
 * remote access on a particular transport interface.
 *
 * @param [in]  iface    Interface to allocate memory on.
 * @param [in]  length   Size of memory region to allocate.
 * @param [in]  flags    Memory allocation flags, see @ref uct_md_mem_flags.
 * @param [in]  name     Allocation name, for debug purposes.
 * @param [out] mem      Descriptor of allocated memory.
 *
 * @return UCS_OK if allocation was successful, error code otherwise.
 */
ucs_status_t sct_iface_mem_alloc(sct_iface_h iface, size_t length, unsigned flags,
                                 const char *name, sct_allocated_memory_t *mem);


/**
 * @ingroup UCT_RESOURCE
 * @brief Release memory allocated with @ref sct_iface_mem_alloc().
 *
 * @param [in]  mem      Descriptor of memory to release.
 */
void sct_iface_mem_free(const sct_allocated_memory_t *mem);


/**
 * @ingroup UCT_RESOURCE
 * @brief Create new endpoint.
 *
 * Create a UCT endpoint in one of the available modes:
 * -# Unconnected endpoint: If no any address is present in @ref sct_ep_params,
 *    this creates an unconnected endpoint. To establish a connection to a
 *    remote endpoint, @ref sct_ep_connect_to_ep will need to be called. Use of
 *    this mode requires @ref sct_ep_params_t::iface has the
 *    @ref UCT_IFACE_FLAG_CONNECT_TO_EP capability flag. It may be obtained by
 *    @ref sct_iface_query .
 * -# Connect to a remote interface: If @ref sct_ep_params_t::dev_addr and
 *    @ref sct_ep_params_t::iface_addr are set, this will establish an endpoint
 *    that is connected to a remote interface. This requires that
 *    @ref sct_ep_params_t::iface has the @ref UCT_IFACE_FLAG_CONNECT_TO_IFACE
 *    capability flag. It may be obtained by @ref sct_iface_query.
 * -# Connect to a remote socket address: If @ref sct_ep_params_t::sockaddr is
 *    set, this will create an endpoint that is connected to a remote socket.
 *    This requires that either @ref sct_ep_params::cm, or
 *    @ref sct_ep_params::iface will be set. In the latter case, the interface
 *    has to support @ref UCT_IFACE_FLAG_CONNECT_TO_SOCKADDR flag, which can be
 *    checked by calling @ref sct_iface_query.
 * @param [in]  params  User defined @ref sct_ep_params_t configuration for the
 *                      @a ep_p.
 * @param [out] ep_p    Filled with handle to the new endpoint.
 *
 * @return UCS_OK       The endpoint is created successfully. This does not
 *                      guarantee that the endpoint has been connected to
 *                      the destination defined in @a params; in case of failure,
 *                      the error will be reported to the interface error
 *                      handler callback provided to @ref sct_iface_open
 *                      via @ref sct_iface_params_t.err_handler.
 * @return              Error code as defined by @ref ucs_status_t
 */
ucs_status_t sct_ep_create(const sct_ep_params_t *params, sct_ep_h *ep_p);


/**
 * @ingroup UCT_CLIENT_SERVER
 * @brief Initiate a disconnection of an endpoint connected to a
 *        sockaddr by a connection manager @ref uct_cm_h.
 *
 * This non-blocking routine will send a disconnect notification on the endpoint,
 * so that @ref sct_ep_disconnect_cb_t will be called on the remote peer.
 * The remote side should also call this routine when handling the initiator's
 * disconnect.
 * After a call to this function, the given endpoint may not be used for
 * communications anymore.
 * The @ref sct_ep_flush / @ref sct_iface_flush routines will guarantee that the
 * disconnect notification is delivered to the remote peer.
 * @ref sct_ep_destroy should be called on this endpoint after invoking this
 * routine and @ref sct_ep_params::disconnect_cb was called.
 *
 * @param [in] ep       Endpoint to disconnect.
 * @param [in] flags    Reserved for future use.
 *
 * @return UCS_OK                Operation has completed successfully.
 *         UCS_ERR_BUSY          The @a ep is not connected yet (either
 *                               @ref uct_cm_ep_client_connect_callback_t or
 *                               @ref uct_cm_ep_server_conn_notify_callback_t
 *                               was not invoked).
 *         UCS_INPROGRESS        The disconnect request has been initiated, but
 *                               the remote peer has not yet responded to this
 *                               request, and consequently the registered
 *                               callback @ref sct_ep_disconnect_cb_t has not
 *                               been invoked to handle the request.
 *         UCS_ERR_NOT_CONNECTED The @a ep is disconnected locally and remotely.
 *         Other error codes as defined by @ref ucs_status_t .
 */
ucs_status_t sct_ep_disconnect(sct_ep_h ep, unsigned flags);


/**
 * @ingroup UCT_RESOURCE
 * @brief Destroy an endpoint.
 *
 * @param [in] ep       Endpoint to destroy.
 */
void sct_ep_destroy(sct_ep_h ep);


/**
 * @ingroup UCT_RESOURCE
 * @brief Get endpoint address.
 *
 * @param [in]  ep       Endpoint to query.
 * @param [out] addr     Filled with endpoint address. The size of the buffer
 *                       provided must be at least @ref sct_iface_attr_t::ep_addr_len.
 */
ucs_status_t sct_ep_get_address(sct_ep_h ep, uct_ep_addr_t *addr);


/**
 * @ingroup UCT_RESOURCE
 * @brief Connect endpoint to a remote endpoint.
 *
 * requires @ref UCT_IFACE_FLAG_CONNECT_TO_EP capability.
 *
 * @param [in] ep           Endpoint to connect.
 * @param [in] dev_addr     Remote device address.
 * @param [in] ep_addr      Remote endpoint address.
 */
ucs_status_t sct_ep_connect_to_ep(sct_ep_h ep, const uct_device_addr_t *dev_addr,
                                  const uct_ep_addr_t *ep_addr);


/**
 * @ingroup UCT_MD
 * @brief Query for memory domain attributes.
 *
 * @param [in]  md       Memory domain to query.
 * @param [out] md_attr  Filled with memory domain attributes.
 */
ucs_status_t sct_md_query(sct_md_h md, uct_md_attr_t *md_attr);


/**
 * @ingroup UCT_MD
 * @brief Parameters for allocating memory using @ref sct_mem_alloc
 */
typedef struct {
    /**
     * Mask of valid fields in this structure, using bits from
     * @ref uct_mem_alloc_params_field_t. Fields not specified in this mask will
     * be ignored.
     */
    uint64_t                     field_mask;

    /**
     * Memory allocation flags, see @ref uct_md_mem_flags
     * If UCT_MEM_ALLOC_PARAM_FIELD_FLAGS is not specified in field_mask, then
     * (UCT_MD_MEM_ACCESS_LOCAL_READ | UCT_MD_MEM_ACCESS_LOCAL_WRITE) is used by
     * default.
     */
    unsigned                     flags;

    /**
     * If @a address is NULL, the underlying allocation routine will
     * choose the address at which to create the mapping. If @a address
     * is non-NULL and UCT_MD_MEM_FLAG_FIXED is not set, the address
     * will be interpreted as a hint as to where to establish the mapping. If
     * @a address is non-NULL and UCT_MD_MEM_FLAG_FIXED is set, then the
     * specified address is interpreted as a requirement. In this case, if the
     * mapping to the exact address cannot be made, the allocation request
     * fails.
     */
    void                         *address;

    /**
     * Type of memory to be allocated.
     */
    ucs_memory_type_t            mem_type;

    struct {
        /**
         * Array of memory domains to attempt to allocate
         * the memory with, for MD allocation method.
         */
        const sct_md_h           *mds;

        /**
         *  Length of 'mds' array. May be empty, in such case
         *  'mds' may be NULL, and MD allocation method will
         *  be skipped.
         */
        unsigned                 count;
    } mds;

    /**
     * Name of the allocated region, used to track memory
     * usage for debugging and profiling.
     * If UCT_MEM_ALLOC_PARAM_FIELD_NAME is not specified in field_mask, then
     * "anonymous-sct_mem_alloc" is used by default.
     */
    const char                   *name;
} sct_mem_alloc_params_t;


/**
 * @ingroup UCT_MD
 * @brief Register memory for zero-copy sends and remote access.
 *
 *  Register memory on the memory domain. In order to use this function, MD
 * must support @ref UCT_MD_FLAG_REG flag.
 *
 * @param [in]     md        Memory domain to register memory on.
 * @param [out]    address   Memory to register.
 * @param [in]     length    Size of memory to register. Must be >0.
 * @param [in]     flags     Memory allocation flags, see @ref uct_md_mem_flags.
 * @param [out]    memh_p    Filled with handle for allocated region.
 */
ucs_status_t sct_md_mem_reg(sct_md_h md, void *address, size_t length,
                            unsigned flags, sct_mem_h *memh_p);

/**
 * @ingroup UCT_MD
 * @brief Undo the operation of @ref sct_md_mem_reg().
 *
 * @param [in]  md          Memory domain which was used to register the memory.
 * @param [in]  memh        Local access key to memory region.
 */
ucs_status_t sct_md_mem_dereg(sct_md_h md, sct_mem_h memh);


/**
 * @ingroup UCT_MD
 * @brief Allocate memory for zero-copy communications and remote access.
 *
 * Allocate potentially registered memory.
 *
 * @param [in]     length      The minimal size to allocate. The actual size may
 *                             be larger, for example because of alignment
 *                             restrictions. Must be >0.
 * @param [in]     methods     Array of memory allocation methods to attempt.
 *                             Each of the provided allocation methods will be
 *                             tried in array order, to perform the allocation,
 *                             until one succeeds. Whenever the MD method is
 *                             encountered, each of the provided MDs will be
 *                             tried in array order, to allocate the memory,
 *                             until one succeeds, or they are exhausted. In
 *                             this case the next allocation method from the
 *                             initial list will be attempted.
 * @param [in]     num_methods Length of 'methods' array.
 * @param [in]     params      Memory allocation characteristics, see
 *                             @ref sct_mem_alloc_params_t.
 * @param [out]    mem         In case of success, filled with information about
 *                             the allocated memory. @ref sct_allocated_memory_t
 */
ucs_status_t sct_mem_alloc(size_t length, const uct_alloc_method_t *methods,
                           unsigned num_methods,
                           const sct_mem_alloc_params_t *params,
                           sct_allocated_memory_t *mem);


ucg_status_t sct_ofd_req_init(sct_ofd_req_h req, void *handle);
void sct_ofd_req_clean(sct_ofd_req_h req);


/**
 * @ingroup UCT_MD
 * @info get/release info
 */
SCS_STACK_INIT(sct_event, sct_event_t);

#define events_pool_t scs_stack_t(sct_event)
#define events_pool_h scs_stack_h(sct_event)

typedef enum scs_eid_pool_status {
    SCS_EVENT_POOL_EMPTY,
    SCS_EVENT_POOL_FULL,
    SCS_EVENT_POOL_NO_RESOURCE,
    SCS_EVENT_POOL_OK
} scs_eid_pool_status_t;

UCT_INLINE_API ucs_status_t
sct_ep_alloc_event(sct_ep_h ep, sct_event_h event, uint8_t flag)
{
    return ep->iface->ops.ep_alloc_event(ep, event, flag);
}

ucs_status_t sct_ep_free_event(sct_event_h event, uint8_t flag);

UCT_INLINE_API scs_eid_pool_status_t
sct_get_event_from_pool(sct_ep_h ep, events_pool_h *eid_pool, sct_event_h event)
{
    ucs_assert(event != NULL);
    int ret;
    uint8_t stars_dev_id = ep->iface->ops.iface_get_stars_dev_id(ep->iface);
    events_pool_h my_eid_pool = eid_pool[stars_dev_id];

    if (scs_stack_empty(sct_event, my_eid_pool)) {
        return SCS_EVENT_POOL_EMPTY;
    }
    ret = scs_stack_top(sct_event, my_eid_pool, event);
    if (ret != 0) {
        return SCS_EVENT_POOL_NO_RESOURCE;
    }
    scs_stack_pop(sct_event, my_eid_pool);
    return SCS_EVENT_POOL_OK;
}

UCT_INLINE_API scs_eid_pool_status_t
sct_put_event_to_pool(sct_ep_h ep, events_pool_h *eid_pool, sct_event_h event)
{
    int ret;
    uint8_t stars_dev_id = ep->iface->ops.iface_get_stars_dev_id(ep->iface);
    events_pool_h my_eid_pool = eid_pool[stars_dev_id];

    ret = scs_stack_push(sct_event, my_eid_pool, event);
    if (ret == 1) {
        return SCS_EVENT_POOL_FULL;
    }
    return SCS_EVENT_POOL_OK;
}

/**
 * @ingroup UCT_MD
 * @brief Release allocated memory.
 *
 * Release the memory allocated by @ref sct_mem_alloc.
 *
 * @param [in]  mem         Description of allocated memory, as returned from
 *                          @ref sct_mem_alloc.
 */
ucs_status_t sct_mem_free(const sct_allocated_memory_t *mem);

/**
 * @ingroup UCT_MD
 * @brief Read the configuration for a memory domain.
 *
 * @param [in]  component     Read the configuration of this component.
 * @param [in]  env_prefix    If non-NULL, search for environment variables
 *                            starting with this UCT_<prefix>_. Otherwise, search
 *                            for environment variables starting with just UCT_.
 * @param [in]  filename      If non-NULL, read configuration from this file. If
 *                            the file does not exist, it will be ignored.
 * @param [out] config_p      Filled with a pointer to the configuration.
 *
 * @return Error code.
 */
ucs_status_t sct_md_config_read(sct_component_h component,
                                const char *env_prefix, const char *filename,
                                sct_md_config_t **config_p);


/**
 * @ingroup UCT_MD
 *
 * @brief Pack a remote key.
 *
 * @param [in]  md           Handle to memory domain.
 * @param [in]  memh         Local key, whose remote key should be packed.
 * @param [out] rkey_buffer  Filled with packed remote key.
 *
 * @return Error code.
 */
ucs_status_t sct_md_mkey_pack(sct_md_h md, sct_mem_h memh, void *rkey_buffer);


/**
 * @ingroup UCT_MD
 *
 * @brief Unpack a remote key.
 *
 * @param [in]  component    Component on which to unpack the remote key.
 * @param [in]  rkey_buffer  Packed remote key buffer.
 * @param [out] rkey_ob      Filled with the unpacked remote key and its type.
 *
 * @note The remote key must be unpacked with the same component that was used
 *       to pack it. For example, if a remote device address on the remote
 *       memory domain which was used to pack the key is reachable by a
 *       transport on a local component, then that component is eligible to
 *       unpack the key.
 *       If the remote key buffer cannot be unpacked with the given component,
 *       UCS_ERR_INVALID_PARAM will be returned.
 *
 * @return Error code.
 */
ucs_status_t sct_rkey_unpack(sct_component_h component, const void *rkey_buffer,
                             uct_rkey_bundle_t *rkey_ob);


/**
 * @ingroup UCT_MD
 *
 * @brief Release a remote key.
 *
 * @param [in]  component    Component which was used to unpack the remote key.
 * @param [in]  rkey_ob      Remote key to release.
 */
ucs_status_t sct_rkey_release(sct_component_h component, const uct_rkey_bundle_t *rkey_ob);

UCT_INLINE_API ucs_status_t sct_ep_put_with_notify(sct_ep_h ep, sct_ofd_req_h req, const sct_iov_t *iov)
{
    return ep->iface->ops.ep_put_with_notify(ep, req, iov);
}

UCT_INLINE_API ucs_status_t sct_ep_wait_notify(sct_ep_h ep, sct_ofd_req_h req, sct_wait_elem_h elem)
{
    return ep->iface->ops.ep_wait_notify(ep, req, elem);
}

UCT_INLINE_API ucs_status_t sct_ep_barrier(sct_ep_h ep, sct_ofd_req_h req,
                                           sct_event_h *notify_event, sct_event_h *wait_event,
                                           int event_num)
{
    return ep->iface->ops.ep_barrier(ep, req, notify_event, wait_event, event_num);
}

UCT_INLINE_API ucs_status_t sct_iface_submit_request(sct_iface_h iface, sct_ofd_req_h req)
{
    return iface->ops.iface_submit_req(iface, req);
}

UCT_INLINE_API ucs_status_t sct_iface_notify_progress(sct_iface_h iface)
{
    return iface->ops.iface_notify_progress(iface);
}

UCT_INLINE_API void sct_ofd_req_push_trans_tail(sct_ofd_req_h req, stars_trans_parm_t *elem)
{
    req->stars.trans_task.tail->next = elem;
    req->stars.trans_task.tail = elem;
    req->stars.trans_task.count++;
}

UCT_INLINE_API ucs_status_t sct_iface_create_stars_stream(sct_iface_h iface, void **handle_p, uint16_t stream_depth)
{
    return iface->ops.iface_create_stars_stream(iface, handle_p, stream_depth);
}

UCT_INLINE_API ucs_status_t sct_iface_delete_stars_stream(sct_iface_h iface, void *handle_p)
{
    return iface->ops.iface_delete_stars_stream(iface, handle_p);
}

END_C_DECLS

#endif
