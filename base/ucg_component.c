/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2020.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "ucg_component.h"

#include <ucg/api/ucg_mpi.h>
#include <ucs/debug/memtrack.h>
#include <ucs/sys/module.h>

static UCS_LIST_HEAD(ucg_plan_components_list);

void ucg_register_component(ucg_plan_component_h plan_component)
{
    ucs_list_add_tail(&ucg_plan_components_list, &plan_component->list);
    return;
}

ucs_status_t ucg_query_components(ucg_plan_component_h **plan_components_p,
                                  unsigned *num_plan_components_p)
{
    UCS_MODULE_FRAMEWORK_DECLARE(ucg);
    UCS_MODULE_FRAMEWORK_LOAD(ucg, 0);

    unsigned num_plan_components;
    ucg_plan_component_h *plan_components;
    
    num_plan_components = ucs_list_length(&ucg_plan_components_list);
    plan_components = ucs_malloc(sizeof(ucg_plan_component_h) * num_plan_components, 
                                 "ucg plan components");
    if (plan_components == NULL) {
        return UCS_ERR_NO_MEMORY;
    }
    *plan_components_p = plan_components;
    *num_plan_components_p = num_plan_components;
    
    ucg_plan_component_h plan_component;
    ucs_list_for_each(plan_component, &ucg_plan_components_list, list) {
        (*plan_components++) = plan_component;
    }

    return UCS_OK;
}

void ucg_release_components(ucg_plan_component_h *plan_components)
{
    ucs_free(plan_components);
    return;
}