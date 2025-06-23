/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

#include "numa.h"

#include "sys/sys.h"


#define PATH_SYS_NUMA_ONLINE "/sys/devices/system/node/online"
#define PATH_SYS_NUMA_CPU_LIST "/sys/devices/system/node/node%d/cpulist"
#define PATH_SYS_NUMA_DISTANCE "/sys/devices/system/node/node%d/distance"
#define PATH_SYS_IB_DEV_NUMA_NODE "/sys/class/infiniband/%s/device/numa_node"

#define PATH_CTX_SIZE 100

static uint8_t scs_numa_get_numa_num()
{
    char curr_ctx[PATH_CTX_SIZE] = {0};

    ssize_t size = ucs_read_file(curr_ctx, sizeof(curr_ctx), 1, PATH_SYS_NUMA_ONLINE);
    if (size < 3 || size > 5) { /* range from 3 to 5 as numa online format 0-254 */
        ucg_error("failed to read file: %s", PATH_SYS_NUMA_ONLINE);
        return 0;
    }
    curr_ctx[size - 1] = '\0';

    char *position;
    char *ptr = strtok_r(curr_ctx, "-", &position);
    if (ucg_unlikely(ptr == NULL)) {
        goto invalid;
    }

    ptr = strtok_r(NULL, "-", &position);
    if (ptr != NULL && ptr < curr_ctx + size) {
        return atoi(ptr) + 1; /* 1 means start by 0 */
    }

invalid:
    ucg_error("invalid context %s", curr_ctx);
    return 0;
}

static ucg_status_t scs_numa_get_distance(uint8_t node_id, uint8_t *distance, uint8_t num)
{
    if (ucg_unlikely(distance == NULL || num == 0)) {
        ucg_error("invalid distance or array num %d", num);
        return UCG_ERR_INVALID_PARAM;
    }

    char curr_ctx[PATH_CTX_SIZE] = {0};
    char real_path[PATH_CTX_SIZE] = {0};
    sprintf(real_path, PATH_SYS_NUMA_DISTANCE, node_id);
    ssize_t size = ucs_read_file(curr_ctx, sizeof(curr_ctx), 1, PATH_SYS_NUMA_DISTANCE, node_id);
    if (ucg_unlikely(size < 2)) {
        ucg_error("failed to read file: %s", real_path);
        return UCG_ERR_INVALID_PARAM;
    }
    curr_ctx[size - 1] = '\0';

    int value, idx = 0;
    char *position;
    char *ptr = strtok_r(curr_ctx, " ", &position);
    while (ptr != NULL) {
        if (!(value = atoi(ptr)) || idx >= num) {
            goto invalid;
        }
        distance[idx++] = value;
        ptr = strtok_r(NULL, " ", &position);
    }

    return (idx == num) ? UCG_OK : UCG_ERR_INVALID_PARAM;

invalid:
    ucg_error("invalid real_path %s context %s", real_path, curr_ctx);
    return UCG_ERR_INVALID_PARAM;
}

static uint8_t scs_numa_get_node_id_by_core(uint16_t core_id, scs_numa_cpu_list_t *cpu_list,
                                            uint8_t number)
{
    scs_numa_cpu_list_t *current = NULL;
    for (uint8_t idx = 0; idx < number; ++idx) {
        current = &cpu_list[idx];
        if (core_id >= current->first && core_id <= current->last) {
            return idx;
        }
    }

    return UINT8_MAX;
}

uint8_t scs_numa_get_node_id_by_dev(char *dev_name)
{
    if (ucg_unlikely(!dev_name)) {
        ucg_error("invalid ib device name");
        return UINT8_MAX;
    }

    char curr_ctx[PATH_CTX_SIZE] = {0};
    char real_path[PATH_CTX_SIZE] = {0};
    sprintf(real_path, PATH_SYS_IB_DEV_NUMA_NODE, dev_name);
    ssize_t size = ucs_read_file(curr_ctx, sizeof(curr_ctx), 1, PATH_SYS_IB_DEV_NUMA_NODE, dev_name);
    if (ucg_unlikely(size < 2)) {
        ucg_error("failed to read file: %s", real_path);
        return UINT8_MAX;
    }
    /* last char is LF in ASCII Table */
    curr_ctx[size - 1] = '\0';

    int value = atoi(curr_ctx);
    if (ucg_unlikely(value > UINT8_MAX ||
        (!value && curr_ctx[0] != '0'))) {
        goto invalid;
    }

    return (uint8_t)value;
invalid:
    ucg_error("invalid context: <%s>", curr_ctx);
    return UINT8_MAX;
}

static ucg_status_t scs_numa_get_cpu_list(uint8_t node_id, scs_numa_cpu_list_t *cpu_list)
{
    char curr_ctx[PATH_CTX_SIZE] = {0};
    char real_path[PATH_CTX_SIZE] = {0};
    sprintf(real_path, PATH_SYS_NUMA_CPU_LIST, node_id);
    ssize_t size = ucs_read_file(curr_ctx, sizeof(curr_ctx), 1, PATH_SYS_NUMA_CPU_LIST, node_id);
    if (ucg_unlikely(size < 3)) {
        ucg_info("failed to read file: %s", real_path);
        cpu_list->first = 0;
        cpu_list->last = 0;
        return UCG_OK;
    }
    curr_ctx[size - 1] = '\0';

    char *position = NULL;
    char *ptr = strtok_r(curr_ctx, "-", &position);
    if (ucg_unlikely(ptr == NULL)) {
        goto invalid;
    }

    /* atoi return 0 when input invalid */
    int value = atoi(curr_ctx);
    if (value > UINT16_MAX || (!value && *ptr != '0')) {
        goto invalid;
    }
    cpu_list->first = (uint16_t)value;

    ptr = strtok_r(NULL, "-", &position);
    if (!(ptr != NULL && ptr < curr_ctx + size)) {
        goto invalid;
    }

    value = atoi(ptr);
    if (!value || value > UINT16_MAX) {
        goto invalid;
    }
    cpu_list->last = (uint16_t)value;

    return UCG_OK;
invalid:
    ucg_error("invalid context %s", curr_ctx);
    return UCG_ERR_INVALID_PARAM;
}

ucg_status_t scs_numa_get_information(uint16_t core_id, scs_numa_info_t *numa_info)
{
    numa_info->numa_num = scs_numa_get_numa_num();
    if (ucg_unlikely(!numa_info->numa_num)) {
        ucg_error("get invalid numa number");
        return UCG_ERR_INVALID_PARAM;
    }

    scs_numa_cpu_list_t *cpu_list;
    cpu_list = ucg_malloc(numa_info->numa_num * sizeof(scs_numa_cpu_list_t),
                          "numa cpu list");
    if (ucg_unlikely(!cpu_list)) {
        ucg_error("Failed to alloc numa cpu list");
        return UCG_ERR_NO_MEMORY;
    }

    ucg_status_t status;
    for (uint8_t idx = 0; idx < numa_info->numa_num; ++idx) {
        status = scs_numa_get_cpu_list(idx, &cpu_list[idx]);
        if (ucg_unlikely(status != UCG_OK)) {
            goto err_free_cpu_list;
        }
    }

    numa_info->node_id = scs_numa_get_node_id_by_core(core_id, cpu_list,
                                                      numa_info->numa_num);
    if (ucg_unlikely(numa_info->node_id == UINT8_MAX)) {
        status = UCG_ERR_INVALID_PARAM;
        ucg_error("get invalid numa node id for core_id %d", core_id);
        goto err_free_cpu_list;
    }

    uint8_t *distance = ucg_calloc(numa_info->numa_num, sizeof(uint8_t),
                                   "numa distance");
    if (ucg_unlikely(!distance)) {
        ucg_error("failed to alloc numa distance");
        status = UCG_ERR_NO_MEMORY;
        goto err_free_cpu_list;
    }

    status = scs_numa_get_distance(numa_info->node_id, distance,
                                   numa_info->numa_num);
    if (ucg_unlikely(status != UCG_OK)) {
        ucg_error("failed to get numa distance");
        goto err_free_distance;
    }

    numa_info->distance = distance;
    numa_info->cpus_list = cpu_list;
    return UCG_OK;

err_free_distance:
    ucg_free(distance);
err_free_cpu_list:
    ucg_free(cpu_list);
    return status;
}

ucg_status_t scs_numa_get_distance_by_node_id(scs_numa_info_t *numa_info,
                                              uint8_t node_id, uint8_t *distance)
{
    if (numa_info == NULL || numa_info->distance == NULL ||
        node_id >= numa_info->numa_num) {
        return UCG_ERR_INVALID_PARAM;
    }

    *distance = numa_info->distance[node_id];
    return UCG_OK;
}
