/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2020.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_COMPONENT_H_
#define UCG_COMPONENT_H_

#include <ucg/api/ucg.h>
#include <ucs/type/status.h>
#include <ucs/datastruct/list.h>
#include <ucs/datastruct/queue.h>
#include <ucs/datastruct/mpool.h>
#include <ucs/sys/compiler_def.h>
#include <stdint.h>

#define UCG_PLAN_COMPONENT_NAME_MAX 16
#define UCG_PLANNER_NAME_MAX 16

typedef uint16_t ucg_group_id_t; /* unique */
typedef uint8_t  ucg_coll_id_t;  /* cyclic */
typedef uint8_t  ucg_step_idx_t;
typedef uint32_t ucg_offset_t;
typedef uint16_t ucg_step_idx_ext_t;

typedef struct ucg_op ucg_op_t;
typedef struct ucg_plan ucg_plan_t;
typedef struct ucg_plan* ucg_plan_h;
typedef struct ucg_planner ucg_planner_t;
typedef struct ucg_planner* ucg_planner_h;
typedef struct ucg_planner_ctx ucg_planner_ctx_t;
typedef struct ucg_plan_component ucg_plan_component_t;
typedef struct ucg_plan_component* ucg_plan_component_h;
typedef void* ucg_planner_ctx_h;
typedef void* ucg_planc_ctx_h;

enum ucg_request_common_flags {
    UCG_REQUEST_COMMON_FLAG_COMPLETED = UCS_BIT(0),
    UCG_REQUEST_COMMON_FLAG_MASK      = UCS_MASK(1),
};

typedef struct ucg_request {
    volatile uint32_t        flags;      /**< @ref enum ucg_request_common_flags */
    volatile ucs_status_t    status;     /**< Operation status */
} ucg_request_t;

struct ucg_op {
    /* Trigger an operation to start, generate a request handle for updates */
    ucs_status_t           (*trigger) (ucg_op_t *op,
                                       ucg_coll_id_t coll_id,
                                       ucg_request_t **request);
    /* Discard an operation previously prepared */
    void                   (*discard) (ucg_op_t *op);

    union {
        ucs_list_link_t      list;        /**< cache list member */
        struct {
            ucs_queue_elem_t queue;       /**< pending queue member */
            ucg_request_t  **pending_req; /**< original invocation request */
        };
    };

    ucg_plan_t              *plan;        /**< The plan this belongs to */
    ucg_collective_params_t  params;      /**< original parameters for it */
};

struct ucg_plan {
    /* Check whether this plan supports the collective or not, 1-support, 0-not support*/
    int                    (*check)(const ucg_plan_t *plan,
                                    const ucg_collective_params_t *params);
    /* Destroy this plan */
    void                   (*destroy)(ucg_plan_t *plan);
    
    /* Prepare an operation to follow the given plan */
    ucs_status_t           (*prepare)(ucg_plan_t *plan, 
                                      const ucg_collective_params_t *params,
                                      ucg_op_t **op);
                                      
    /* Plan lookup - caching mechanism */
    ucg_collective_type_t    type;
    ucs_list_link_t          op_head;   /**< List of ucg_op following this plan */

    /* Plan progress */
    ucg_planner_t           *planner;
    ucg_group_id_t           group_id;
    ucg_group_member_index_t my_index;
    ucg_group_h              group;
    ucs_mpool_t             *am_mp;
};

struct ucg_planner {
    /* create planner context for specific ucg group */
    ucs_status_t         (*create)(ucg_group_h group,  
                                  ucg_planc_ctx_h planc_ctx, 
                                  ucg_planner_ctx_h *planner_ctx_p);
    /* destroy planner context */
    void                (*destroy)(ucg_planner_ctx_h planner_ctx_p);
    /* check planner context */
    unsigned            (*progress)(ucg_planner_ctx_h planner_ctx_p);
    /* create plan for specific collective operation */
    ucs_status_t        (*plan)(const ucg_planner_ctx_h planner_ctx_p,
                                const ucg_collective_params_t *params,
                                ucg_plan_t **plan);
    /* print a plan object, for debugging purpose */
    void                (*print)(const ucg_plan_t *plan,
                                 const ucg_collective_params_t *params);

    ucg_plan_component_t *plan_component;
    char                  name[UCG_PLANNER_NAME_MAX];
    unsigned              modifiers_supported; /* < @ref enum ucg_collective_modifiers */
    int                   priority;

    ucs_list_link_t       list; /* List entry in ucg context's planners list */
};

struct ucg_plan_component {
    /* query a usable planner */
    ucs_status_t           (*query)(ucg_planner_h *planner);
    /* init component context */
    ucs_status_t           (*init)(ucg_context_h ctx, 
                                   ucg_planc_ctx_h *planc_ctx_p);
    /* cleanup component context */
    void                   (*cleanup)(ucg_planc_ctx_h ctx);

    ucs_list_link_t          list;
    char                     name[UCG_PLAN_COMPONENT_NAME_MAX];
};

/** 
 * @ingroup UCG_RESOURCE
 * @brief register plan component
 */
void ucg_register_component(ucg_plan_component_h plan_component);

/** 
 * @ingroup UCG_RESOURCE
 * @brief Query for list of plan components
 */
ucs_status_t ucg_query_components(ucg_plan_component_h **plan_components_p,
                                  unsigned *num_plan_components_p);

/** 
 * @ingroup UCG_RESOURCE
 * @brief Release the list of plan components returned from @ref ucg_query_components
 */
void ucg_release_components(ucg_plan_component_h *plan_components);

/**
 * Define a planning component.
 *
 * @param _planc         Planning component structure to initialize.
 * @param _name          Planning component name.
 * @param _sz            Planning component context size.
 * @param _init          Function to init planning component.
 * @param _cleanup       Function to cleanup planning component.
 * @param _query         Function to query planning resources.
 * @param _cfg_prefix    Prefix for configuration environment variables.
 * @param _cfg_table     Defines the planning component's configuration values.
 * @param _cfg_struct    Planning component configuration structure.
 */
#define UCG_PLAN_COMPONENT_DEFINE(_planc, _name, _query, _init, _cleanup, \
                                  _cfg_prefix, _cfg_table, _cfg_struct) \
    ucg_plan_component_t _planc = { \
        .query              = (_query), \
        .init               = (_init), \
        .cleanup            = (_cleanup), \
        .name               = (_name) \
    }; \
    UCS_STATIC_INIT { \
        ucg_register_component(&(_planc)); \
    } \
    UCS_CONFIG_REGISTER_TABLE(_cfg_table, _name" component", _cfg_prefix, \
                              _cfg_struct)
                              
#endif /* UCG_COMPONENT_H_ */
