/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_DEF_H_
#define UCG_PLANC_DEF_H_

#include "ucg/api/ucg.h"

#include "core/ucg_def.h"

/**
 * @ingroup UCG_PLANC
 * @brief PlanC configuration descriptor.
 */
typedef void* ucg_planc_config_h;

/**
 * @ingroup UCG_PLANC
 * @brief PlanC context descriptor.
 */
typedef void* ucg_planc_context_h;

/**
 * @ingroup UCG_PLANC
 * @brief PlanC group descriptor.
 */
typedef struct ucg_planc_group* ucg_planc_group_h;

/**
 * @ingroup UCG_PLANC
 * @brief PlanC structure.
 */
typedef struct ucg_planc ucg_planc_t;

/**
 * @ingroup UCG_PLANC
 * @brief Parameters for initializing PlanC context.
 */
typedef struct ucg_planc_params ucg_planc_params_t;

/**
 * @ingroup UCG_PLANC
 * @brief PlanC context attributes.
 */
typedef struct ucg_planc_context_attr ucg_planc_context_attr_t;

/**
 * @ingroup UCG_PLANC
 * @brief Parameters for creating PlanC group.
 */
typedef struct ucg_planc_group_params ucg_planc_group_params_t;

/**
 * @ingroup UCG_PLANC
 * @brief Function that query memory type of the @b ptr from PlanC.
 */
typedef ucg_status_t (*ucg_planc_mem_query_func_t)(const void *ptr,
                                                   ucg_mem_attr_t *attr);

/**
 * @ingroup UCG_PLANC
 * @brief Function that init global resources of PlanC.
 */
typedef ucg_status_t (*ucg_planc_global_init_func_t)(const ucg_global_params_t *params);

/**
 * @ingroup UCG_PLANC
 * @brief Function that clean up global resources of PlanC.
 */
typedef void (*ucg_planc_global_cleanup_func_t)();

/**
 * @ingroup UCG_PLANC
 * @brief Function that read configuration of PlanC.
 */
typedef ucg_status_t (*ucg_planc_config_read_func_t)(const char *env_prefix,
                                                     const char *filename,
                                                     ucg_planc_config_h *config);

/**
 * @ingroup UCG_PLANC
 * @brief Function that modify configuration of PlanC.
 */
typedef ucg_status_t (*ucg_planc_config_modify_func_t)(ucg_planc_config_h config,
                                                       const char *name,
                                                       const char *value);

/**
 * @ingroup UCG_PLANC
 * @brief Function that release configuration of PlanC.
 */
typedef void (*ucg_planc_config_release_func_t)(ucg_planc_config_h config);

/**
 * @ingroup UCG_PLANC
 * @brief Function that initialize PlanC context.
 */
typedef ucg_status_t (*ucg_planc_context_init_func_t)(const ucg_planc_params_t *params,
                                                      const ucg_planc_config_h config,
                                                      ucg_planc_context_h *context);

/**
 * @ingroup UCG_PLANC
 * @brief Function that cleanup PlanC context.
 */
typedef void (*ucg_planc_context_cleanup_func_t)(ucg_planc_context_h context);

/**
 * @ingroup UCG_PLANC
 * @brief Function that query attributes of PlanC context.
 */
typedef ucg_status_t (*ucg_planc_context_query_func_t)(ucg_planc_context_h context,
                                                       ucg_planc_context_attr_t *attr);

/**
 * @ingroup UCG_PLANC
 * @brief Function that create PlanC group.
 */
typedef ucg_status_t (*ucg_planc_group_create_func_t)(ucg_planc_context_h context,
                                                      const ucg_planc_group_params_t *params,
                                                      ucg_planc_group_h *planc_group);

/**
 * @ingroup UCG_PLANC
 * @brief Function that destroy PlanC group.
 */
typedef void (*ucg_planc_group_destroy_func_t)(ucg_planc_group_h planc_group);

/**
 * @ingroup UCG_PLANC
 * @brief Function that get plans provided by PlanC group.
 */
typedef ucg_status_t (*ucg_planc_get_plans_func_t)(ucg_planc_group_h planc_group,
                                                   ucg_plans_t *plans);

#endif