/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ucg_info.h"

#include "ucg/api/ucg.h"
#include "core/ucg_group.h"
#include "core/ucg_plan.h"

static ucg_status_t oob_allgather(const void *sendbuf, void *recvbuf, int count, void *group)
{
    memcpy(recvbuf, sendbuf, count);
    return UCG_OK;
}

static ucg_status_t get_location(ucg_rank_t rank, ucg_location_t *location)
{
    location->field_mask = UCG_LOCATION_FIELD_NODE_ID | UCG_LOCATION_FIELD_SOCKET_ID;
    location->node_id = 0;
    location->socket_id = 1;
    return UCG_OK;
}

static void init_context(ucg_context_h *context)
{
    ucg_config_h config;
    UCG_CHECK(ucg_config_read(NULL, NULL, &config));

    ucg_params_t params;
    params.field_mask = UCG_PARAMS_FIELD_OOB_GROUP |
                        UCG_PARAMS_FIELD_LOCATION_CB;
    params.oob_group.allgather = oob_allgather;
    params.oob_group.myrank = 0;
    params.oob_group.size = 1;
    params.oob_group.group = NULL;
    params.get_location = get_location;

	 UCG_CHECK(ucg_init(&params, config, context));
    return;
}

static void create_group(ucg_context_h context, ucg_group_h *group)
{
    ucg_group_params_t params;
    params.field_mask = UCG_GROUP_PARAMS_FIELD_ID |
                        UCG_GROUP_PARAMS_FIELD_SIZE |
                        UCG_GROUP_PARAMS_FIELD_MYRANK |
                        UCG_GROUP_PARAMS_FIELD_RANK_MAP |
                        UCG_GROUP_PARAMS_FIELD_OOB_GROUP;
    params.id = 0;
    params.size = 1;
    params.myrank = 0;
    params.rank_map.size = params.size;
    params.rank_map.type = UCG_RANK_MAP_TYPE_FULL;
    params.oob_group.allgather = oob_allgather;
    params.oob_group.myrank = 0;
    params.oob_group.size = 1;
    params.oob_group.group = NULL;

	 UCG_CHECK(ucg_group_create(context, &params, group));
}

void print_plans()
{
    ucg_context_h context;
    init_context(&context);

    ucg_group_h group;
    create_group(context, &group);

    ucg_plans_print(group->plans, stdout);

    ucg_group_destroy(group);
    ucg_cleanup(context);

    return;
}