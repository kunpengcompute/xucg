/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_H_
#define UCG_PLANC_H_

#include "ucg/api/ucg.h"

#include "core/ucg_def.h"
#include "core/ucg_vgroup.h"
#include "util/ucg_component.h"
#include "util/ucg_class.h"

#include "ucg_planc_def.h"

/**
 * @defgroup UCG_PLANC UCG Plan Component.
 * @{
 * Data structures and routines.
 * @}
 */


/**
 * @ingroup UCG_PLANC
 * @brief Symbol name of PlanC object.
 *
 * The length of _name should not exceed @ref UCG_COMPONENT_OBJNAME_MAX_LEN - 10
 */
#define UCG_PLANC_OBJNAME(_name) ucg_planc_ ## _name

/**
 * @ingroup UCG_PLANC
 * @brief PlanC library naming format
 *
 * "*" represents any number of characters.
 */
#define UCG_PLANC_LIBNAME_PATTERN "libucg_planc_*.so"

/**
 * @ingroup UCG_PLANC
 * @brief PlanC context attributes field mask.
 */
typedef enum {
    UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR = UCG_BIT(0), /**< Context address. */
    UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR_LEN = UCG_BIT(1), /**< Length of Context address. */
    UCG_PLANC_CONTEXT_ATTR_FIELD_THREAD_MODE = UCG_BIT(2), /**< Thread mode of Context. */
} ucg_planc_context_attr_field_t;

/**
 * @ingroup UCG_PLANC
 * @brief Parameters for initializing PlanC context.
 */
typedef struct ucg_planc_params {
    /** User initialized context. */
    ucg_context_t *context;
    ucg_thread_mode_t thread_mode;
} ucg_planc_params_t;

/**
 * @ingroup UCG_PLANC
 * @brief PlanC context attributes.
 */
typedef struct ucg_planc_context_attr {
    /**
     * Mask of the information to be queried,
     * using bits from @ref ucg_planc_context_attr_field_t
     */
    uint64_t field_mask;

    /** PlanC context address. Corresponding bit is UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR */
    void *addr;

    /**
     * Length of PlanC context address. Corresponding bit is
     * UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR_LEN
     */
    uint32_t addr_len;

    /**
     * Thread mode currently in use. Corresponding bit is
     * UCG_PLANC_CONTEXT_ATTR_FIELD_THREAD_MODE
     */
    ucg_thread_mode_t thread_mode;
} ucg_planc_context_attr_t;

/**
 * @ingroup UCG_PLANC
 * @brief Parameters for creating PlanC group.
 */
typedef struct ucg_planc_group_params {
    ucg_group_t *group;
} ucg_planc_group_params_t;

/**
 * @ingroup UCG_PLANC
 * @brief Base PlanC group structure
 */
typedef struct ucg_planc_group {
    ucg_vgroup_t super;
} ucg_planc_group_t;
UCG_CLASS_DECLARE(ucg_planc_group_t, UCG_CLASS_CTOR_ARGS(ucg_group_t *group));

/**
 * @ingroup UCG_PLANC
 * @brief Plan Component
 *
 * A plan is an abstraction of the steps to perform a collective operation. A
 * PlanC(short for plan component) contains plans for different scenarios.
 *
 * PlanC supports dynamic loading through inheriting @ref ucg_component_t. PlanC
 * provides interfaces similar to the UCG API. The main difference is that PlanC
 * does not provide interfaces for creating collective operation. Instead, it
 * provides an interface to get a selector that contains plans and selection
 * policy. Therefore, up-layer can select the best plan from all PlanC plans.
 *
 * Each specific PlanC's structure should put @ref ucg_planc_t in the first
 * place as super.
 *
 * PlanC implementers should use @ref UCG_PLANC_OBJNAME to generate the name of
 * derived object for proper loading.
 */
typedef struct ucg_planc {
    ucg_component_t super;

    ucg_planc_mem_query_func_t mem_query;

    /* Global resources */
    ucg_planc_global_init_func_t global_init;
    ucg_planc_global_cleanup_func_t global_cleanup;

    /* Configuration */
    ucg_planc_config_read_func_t config_read;
    ucg_planc_config_modify_func_t config_modify;
    ucg_planc_config_release_func_t config_release;

    /* Context */
    ucg_planc_context_init_func_t context_init;
    ucg_planc_context_cleanup_func_t context_cleanup;
    ucg_planc_context_query_func_t context_query;

    /* Group */
    ucg_planc_group_create_func_t group_create;
    ucg_planc_group_destroy_func_t group_destroy;

    /* Plan */
    ucg_planc_get_plans_func_t get_plans;
} ucg_planc_t;

/**
 * @ingroup UCG_PLANC
 * @brief Load all plan component.
 *
 * This routine loads library that match @b UCG_PLANC_LIBNAME(*) in @b UCG_PLANC_PATH.
 *
 * @note It can be called only once.
 */
ucg_status_t ucg_planc_load();

/**
 * @ingroup UCG_PLANC
 * @brief Unload plan component.
 *
 * This routine is the inverse of @ref ucg_planc_load. Once this routine is
 * invoked, the behaviour of using returned ucg_planc_t* is undefined.
 */
void ucg_planc_unload();

/**
 * @ingroup UCG_PLANC
 * @brief Get the number of loaded PlanC.
 */
int32_t ucg_planc_count();

/**
 * @ingroup UCG_PLANC
 * @brief Get PlanC by index.
 *
 * Used together with @ref ucg_planc_count to obtain all PlanC.
 *
 * @param [out] idx     Index, [0, ucg_planc_count)
 */
ucg_planc_t* ucg_planc_get_by_idx(int32_t idx);

/**
 * @ingroup UCG_PLANC
 * @brief Get PlanC by name.
 *
 * @param [in] name     PlanC name, such as "ucx"
 */
ucg_planc_t* ucg_planc_get_by_name(const char *name);

#endif