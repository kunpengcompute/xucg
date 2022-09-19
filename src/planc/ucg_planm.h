/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANM_H_
#define UCG_PLANM_H_

#include "ucg/api/ucg.h"

#include "core/ucg_def.h"
#include "core/ucg_vgroup.h"
#include "util/ucg_component.h"
#include "util/ucg_class.h"

#include "ucg_planc_def.h"

/**
 * @defgroup UCG_PLANM UCG Plan Module.
 * @{
 * Data structures and routines.
 * @}
 */

typedef ucg_planc_get_plans_func_t ucg_planm_get_plans_func_t;

/**
 * @ingroup UCG_PLANM
 * @brief Symbol name of PlanM object.
 *
 * The length of _component and _name should not exceed @ref UCG_COMPONENT_OBJNAME_MAX_LEN - 10
 */
#define UCG_PLANM_OBJNAME(_component_name, _module_name) \
            ucg_planm_ ## _component_name ## _ ## _module_name

/**
 * @ingroup UCG_PLANM
 * @brief PlanM library naming prefix format
 *
 * "*" represents any number of characters.
 */
#define UCG_PLANM_LIBNAME_PREFIX "libucg_planm_"

/**
 * @ingroup UCG_PLANM
 * @brief Plan Module
 *
 * A plan is an abstraction of the steps to perform a collective operation. A
 * PlanM(short for plan module) contains plans for different scenarios.
 *
 * PlanM supports dynamic loading through inheriting @ref ucg_component_t. PlanM
 * is a subset of PlanC which provides interfaces to get a selector that contains
 * plans and selection policy.
 *
 * PlanM implementers should use @ref UCG_PLANM_OBJNAME to generate the name of
 * derived object for proper loading.
 */
typedef struct ucg_planm {
    ucg_component_t super;
    /* Op size */
    int op_size;
    /* Plan */
    ucg_planm_get_plans_func_t get_plans;
} ucg_planm_t;

/**
 * @ingroup UCG_PLANM
 * @brief Load all plan modules in a specific component.
 *
 * This routine loads library that match @b UCG_PLANM_LIBNAME_PREFIX(*) in @b UCG_PLANM_PATH.
 *
 * @param [in]    component_name    The name of the component.
 * @param [inout] ucg_planm         Plan modules.
 *
 * @note It can be called only once.
 */
ucg_status_t ucg_planm_load(const char* component_name, ucg_components_t *ucg_planm);

/**
 * @ingroup UCG_PLANM
 * @brief Unload plan modules in a specific component.
 *
 * This routine is the inverse of @ref ucg_planm_load. Once this routine is
 * invoked, the behaviour of using returned ucg_planm_t* is undefined.
 *
 * @param [inout] ucg_planm         Plan modules.
 */
void ucg_planm_unload(ucg_components_t *ucg_planm);

/**
 * @ingroup UCG_PLANM
 * @brief Get the number of loaded PlanM in a specific component.
 *
 * @param [inout] ucg_planm         Plan modules.
 */
int32_t ucg_planm_count(ucg_components_t *ucg_planm);

/**
 * @ingroup UCG_PLANM
 * @brief Get PlanM by index in a specific component.
 *
 * Used together with @ref ucg_planm_count to obtain all PlanM in a specific component.
 *
 * @param [out]   idx               Index, [0, ucg_planm_count)
 * @param [inout] ucg_planm         Plan modules.
 */
ucg_planm_t* ucg_planm_get_by_idx(int32_t idx, ucg_components_t *ucg_planm);

/**
 * @ingroup UCG_PLANM
 * @brief Get PlanM by name.
 *
 * @param [in]    name              PlanM name, such as "ucx"
 * @param [inout] ucg_planm         Plan modules.
 */
ucg_planm_t* ucg_planm_get_by_name(const char *name, ucg_components_t *ucg_planm);

#endif