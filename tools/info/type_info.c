/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "core/ucg_context.h"
#include "core/ucg_group.h"
#include "core/ucg_request.h"
#include "planc_ucx_context.h"
#include "planc_ucx_group.h"
#include "planc_ucx_plan.h"

#ifdef UCG_BUILD_PLANC_HCCL
#include "planc_hccl_context.h"
#include "planc_hccl_group.h"
#include "planc_hccl_plan.h"
#endif

static void print_size(const char *name, size_t size)
{
    int i;
    printf("    sizeof(%s)%n = ", name, &i);
    while (i++ < 40) {
        printf(".");
    }
    printf(" %-6lu\n", size);
    return;
}

#define UCG_PRINT_SIZE(type) print_size(#type, sizeof(type))

void print_types()
{
    printf("CORE:\n");
    UCG_PRINT_SIZE(ucg_config_t);
    UCG_PRINT_SIZE(ucg_context_t);
    UCG_PRINT_SIZE(ucg_group_t);
    UCG_PRINT_SIZE(ucg_request_t);

#ifdef UCG_BUILD_PLANC_UCX
    printf("\nPLANC UCX:\n");
    UCG_PRINT_SIZE(ucg_planc_ucx_context_t);
    UCG_PRINT_SIZE(ucg_planc_ucx_group_t);
    UCG_PRINT_SIZE(ucg_planc_ucx_op_t);
#endif

#ifdef UCG_BUILD_PLANC_HCCL
    printf("\nPLANC HCCL:\n");
    UCG_PRINT_SIZE(ucg_planc_hccl_context_t);
    UCG_PRINT_SIZE(ucg_planc_hccl_group_t);
    UCG_PRINT_SIZE(ucg_planc_hccl_op_t);
#endif
    return;
}