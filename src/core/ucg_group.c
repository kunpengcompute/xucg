/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ucg_group.h"
#include "ucg_context.h"
#include "ucg_rank_map.h"
#include "ucg_plan.h"
#include "ucg_topo.h"

#include "util/ucg_helper.h"
#include "util/ucg_malloc.h"
#include "util/ucg_log.h"
#include "planc/ucg_planc.h"

#define UCG_GROUP_COPY_REQUIRED_FIELD(_field, _copy, _dst, _src, _err_label) \
    UCG_COPY_REQUIRED_FIELD(UCG_TOKENPASTE(UCG_GROUP_PARAMS_FIELD_, _field), \
                            _copy, _dst, _src, _err_label)

#define UCG_GROUP_COPY_OPTIONAL_FIELD(_field, _copy, _dst, _src, _default, _err_label) \
    UCG_COPY_OPTIONAL_FIELD(UCG_TOKENPASTE(UCG_GROUP_PARAMS_FIELD_, _field), \
                            _copy, _dst, _src, _default, _err_label)

static void ucg_group_free_params(ucg_group_t *group)
{
    ucg_rank_map_cleanup(&group->rank_map);
    return;
}

static ucg_status_t ucg_group_apply_params(ucg_group_t *group,
                                           const ucg_group_params_t *params)
{
    uint64_t field_mask = params->field_mask;

    UCG_GROUP_COPY_REQUIRED_FIELD(ID, UCG_COPY_VALUE,
                                  group->id, params->id,
                                  err);
    UCG_GROUP_COPY_REQUIRED_FIELD(SIZE, UCG_COPY_VALUE,
                                  group->size, params->size,
                                  err);
    UCG_GROUP_COPY_REQUIRED_FIELD(MYRANK, UCG_COPY_VALUE,
                                  group->myrank, params->myrank,
                                  err);
    UCG_GROUP_COPY_REQUIRED_FIELD(RANK_MAP, ucg_rank_map_copy,
                                  &group->rank_map, &params->rank_map,
                                  err);
    if (group->size != group->rank_map.size) {
        ucg_error("rank map size(%u) isn't equal to group size(%u)",
                  group->rank_map.size, group->size);
        goto err_cleanup_rank_map;
    }

    UCG_GROUP_COPY_REQUIRED_FIELD(OOB_GROUP, UCG_COPY_VALUE,
                                  group->oob_group, params->oob_group,
                                  err_cleanup_rank_map);
    if (group->size != group->oob_group.size) {
        ucg_error("oob group size(%u) isn't equal to group size(%u)",
                  group->oob_group.size, group->size);
        goto err_cleanup_rank_map;
    }
    return UCG_OK;

err_cleanup_rank_map:
    ucg_rank_map_cleanup(&group->rank_map);
err:
    return UCG_ERR_INVALID_PARAM;
}

static void ucg_group_destroy_planc_group(ucg_group_t *group)
{
    int32_t num_planc_groups = group->num_planc_groups;
    ucg_resource_planc_t *planc_rscs = group->context->planc_rscs;
    for (int i = 0; i < num_planc_groups; ++i) {
        ucg_planc_t *planc = planc_rscs[i].planc;
        planc->group_destroy(group->planc_groups[i]);
    }
    ucg_free(group->planc_groups);
    return;
}

static ucg_status_t ucg_group_create_planc_group(ucg_group_t *group)
{
    ucg_status_t status = UCG_OK;

    int32_t num_planc_rscs = group->context->num_planc_rscs;
    ucg_assert(num_planc_rscs > 0);
    group->planc_groups = ucg_calloc(num_planc_rscs, sizeof(ucg_planc_group_h), "planc groups");
    if (group->planc_groups == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    ucg_planc_group_params_t params = {
        .group = group,
    };
    ucg_resource_planc_t *planc_rscs = group->context->planc_rscs;
    for (int i = 0; i < num_planc_rscs; ++i) {
        ucg_planc_t *planc = planc_rscs[i].planc;
        status = planc->group_create(planc_rscs[i].ctx, &params, &group->planc_groups[i]);
        if (status != UCG_OK) {
            ucg_error("Failed to create group of planc %s", planc->super.name);
            goto err_destroy_planc_group;
        }
        ++group->num_planc_groups;
    }

    return UCG_OK;

err_destroy_planc_group:
    ucg_group_destroy_planc_group(group);
    return status;
}

static void ucg_group_free_plans(ucg_group_t *group)
{
    ucg_plans_cleanup(group->plans);
    return;
}

static ucg_status_t ucg_group_fill_plans(ucg_group_t *group)
{
    ucg_status_t status;

    status = ucg_plans_init(&group->plans);
    if (status != UCG_OK) {
        ucg_error("Failed to init plans");
        return status;
    }

    int32_t num_planc_groups = group->num_planc_groups;
    ucg_resource_planc_t *planc_rscs = group->context->planc_rscs;
    for (int i = 0; i < num_planc_groups; ++i) {
        ucg_planc_t *planc = planc_rscs[i].planc;
        status = planc->get_plans(group->planc_groups[i], group->plans);
        if (status != UCG_OK) {
            ucg_error("Failed to get plans from planc %s", planc->super.name);
            goto err_free_plans;
        }
    }

    return UCG_OK;

err_free_plans:
    ucg_group_free_plans(group);
    return status;
}

static ucg_status_t ucg_group_init_topo(ucg_group_t *group)
{
    ucg_topo_params_t params;
    params.group = group;
    params.myrank = group->myrank;
    params.rank_map = &group->rank_map;
    params.get_location = ucg_group_get_location;
    return ucg_topo_init(&params, &group->topo);
}

ucg_status_t ucg_group_create(ucg_context_h context, const ucg_group_params_t *params,
                              ucg_group_h *group)
{
    UCG_CHECK_NULL_INVALID(context, params, group);

    ucg_context_lock(context);

    ucg_status_t status = UCG_OK;
    ucg_group_t *grp = ucg_calloc(1, sizeof(ucg_group_t), "ucg group");
    if (grp == NULL) {
        status = UCG_ERR_NO_MEMORY;
        goto out;
    }
    grp->context = context;
    grp->unique_req_id = 0;

    status = ucg_group_apply_params(grp, params);
    if (status != UCG_OK) {
        goto err_free_grp;
    }

    status = ucg_group_create_planc_group(grp);
    if (status != UCG_OK) {
        goto err_free_params;
    }

    status = ucg_group_init_topo(grp);
    if (status != UCG_OK) {
        goto err_destroy_planc_group;
    }

    status = ucg_group_fill_plans(grp);
    if (status != UCG_OK) {
        goto err_destroy_planc_group;
    }

    ucg_debug("Group id %d, size %u, myrank %d", grp->id, grp->size, grp->myrank);
    *group = grp;
    goto out;

err_destroy_planc_group:
    ucg_group_destroy_planc_group(grp);
err_free_params:
    ucg_group_free_params(grp);
err_free_grp:
    ucg_free(grp);
out:
    ucg_context_unlock(context);
    return status;
}

void ucg_group_destroy(ucg_group_h group)
{
    UCG_CHECK_NULL_VOID(group);

    ucg_context_t *context = group->context;
    ucg_context_lock(context);

    ucg_topo_cleanup(group->topo);
    ucg_group_free_plans(group);
    ucg_group_destroy_planc_group(group);
    ucg_group_free_params(group);
    ucg_free(group);

    ucg_context_unlock(context);
    return;
}