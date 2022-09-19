/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ucg_rank_map.h"

#include "util/ucg_malloc.h"
#include "util/ucg_helper.h"
#include "util/ucg_log.h"

ucg_status_t ucg_rank_map_init_by_array(ucg_rank_map_t *map, ucg_rank_t **ranks,
                                        uint32_t size, int take_over)
{
    UCG_CHECK_NULL_INVALID(map, ranks, *ranks);

    map->type = UCG_RANK_MAP_TYPE_ARRAY;
    map->size = size;
    map->array = *ranks; // temporary takeover

    ucg_rank_t *array = NULL;
    ucg_rank_map_optimize(map, &array);
    if (array != NULL) {
        ucg_assert(array == *ranks);
        /* optimize successfully. */
        if (take_over) {
            ucg_free(array);
            *ranks = NULL;
        }
        return UCG_OK;
    }

    if (take_over) {
        *ranks = NULL;
        return UCG_OK;
    }

    // copy the original ranks
    int length = map->size * sizeof(ucg_rank_t);
    map->array = ucg_malloc(length, "ucg rank-map array");
    if (map->array == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    memcpy(map->array, *ranks, length);

    return UCG_OK;
}

void ucg_rank_map_optimize(ucg_rank_map_t *map, ucg_rank_t **array)
{
    UCG_CHECK_NULL_VOID(map, array);

    if (map->type == UCG_RANK_MAP_TYPE_FULL || map->type == UCG_RANK_MAP_TYPE_CB) {
        return;
    }

    if (map->type == UCG_RANK_MAP_TYPE_STRIDE) {
        if (map->strided.start == 0 && map->strided.stride == 1) {
            map->type = UCG_RANK_MAP_TYPE_FULL;
        }
        return;
    }

    *array = NULL;
    ucg_rank_t *original_array = map->array;
    uint32_t size = map->size;
    int32_t stride = 1;
    int32_t is_same_stride = 1;
    if (size > 1) {
        stride = original_array[1] - original_array[0];
        for (uint32_t i = 2; i < size; ++i) {
            if (stride != (original_array[i] - original_array[i - 1])) {
                is_same_stride = 0;
                break;
            }
        }
    }

    if (!is_same_stride) {
        return;
    }

    if (stride == 1 && original_array[0] == 0) {
        map->type = UCG_RANK_MAP_TYPE_FULL;
    } else {
        map->type = UCG_RANK_MAP_TYPE_STRIDE;
        map->strided.start = original_array[0];
        map->strided.stride = stride;
    }
    *array = original_array;
    return;
}

ucg_status_t ucg_rank_map_copy(ucg_rank_map_t *dst, const ucg_rank_map_t *src)
{
    UCG_CHECK_NULL_INVALID(dst, src);

    // shallow copy
    ucg_rank_t *ranks = NULL;
    memcpy(dst, src, sizeof(ucg_rank_map_t));
    if (dst->type != UCG_RANK_MAP_TYPE_ARRAY) {
        ucg_rank_map_optimize(dst, &ranks);
        return UCG_OK;
    }
    // deep copy array
    ranks = dst->array;
    return ucg_rank_map_init_by_array(dst, &ranks, dst->size, 0);
}

void ucg_rank_map_cleanup(ucg_rank_map_t *map)
{
    UCG_CHECK_NULL_VOID(map);

    if (map->type == UCG_RANK_MAP_TYPE_ARRAY) {
        ucg_free(map->array);
        map->array = NULL;
    }

    map->size = 0;
    return;
}

ucg_rank_t ucg_rank_map_eval(const ucg_rank_map_t *map, ucg_rank_t src_rank)
{
    UCG_CHECK_NULL(UCG_INVALID_RANK, map);
    UCG_CHECK_OUT_RANGE(UCG_INVALID_RANK, src_rank, 0, (ucg_rank_t)map->size);

    ucg_rank_t dest_rank = UCG_INVALID_RANK;
    switch (map->type) {
        case UCG_RANK_MAP_TYPE_FULL:
            dest_rank = src_rank;
            break;
        case UCG_RANK_MAP_TYPE_ARRAY:
            dest_rank = map->array[src_rank];
            break;
        case UCG_RANK_MAP_TYPE_STRIDE:
            dest_rank = map->strided.start + src_rank * map->strided.stride;
            break;
        case UCG_RANK_MAP_TYPE_CB:
            dest_rank = map->cb.mapping(map->cb.arg, src_rank);
            break;
        default:
            ucg_error("Unknown rank-map type %d", map->type);
            break;
    }

    return dest_rank;
}