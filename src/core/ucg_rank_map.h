/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_RANK_MAP_H_
#define UCG_RANK_MAP_H_

#include "ucg/api/ucg.h"

/**
 * @brief Initialize rank-map
 *
 * Initialize the rank-map by array @b ranks. If @b take_over is 1, rank-map
 * will take over the ownership of memory that @b *ranks points to, and
 * set @b *ranks to NULL. If @b take_over is 0, it will deep copy @b *ranks.
 *
 * @param [out] map         Rank map.
 * @param [in]  ranks       Mapping array.
 * @param [in]  size        Size of mapping array.
 * @param [in]  take_over   0 means not take over ownership, otherwise take over.
 */
ucg_status_t ucg_rank_map_init_by_array(ucg_rank_map_t *map, ucg_rank_t **ranks,
                                        uint32_t size, int take_over);

/**
 * @brief Optimize rank-map.
 *
 * When the condition is met, the array-type can be optimized to full-type or
 * stride-type, which reduces the space occupation and fetch overhead. Also,
 * stride-type can be optimized to full-type when stride is 1, which reduces the
 * compute overhead.
 *
 * @param [inout] map       Rank map.
 * @param [out]   array     Ingored if not array-type. Otherwise when array-type
 *                          is optimized, it will points to original array address.
 */
void ucg_rank_map_optimize(ucg_rank_map_t *map, ucg_rank_t **array);

/**
 * @brief Copy rank-map deeply.
 *
 * @note This routine will optimize the dst rank-map.
 */
ucg_status_t ucg_rank_map_copy(ucg_rank_map_t *dst, const ucg_rank_map_t *src);

/**
 * @brief Cleanup rank-map.
 *
 * This routine is used to cleanup the rank-map from @ref ucg_rank_map_init_by_array
 * and @ref ucg_rank_map_copy
 */
void ucg_rank_map_cleanup(ucg_rank_map_t *map);

/**
 * @brief Map src-rank to dest-rank.
 *
 * @param [in] map          the rank map
 * @param [in] src_rank     the source rank
 *
 * @return UCG_INVALID_RANK for failed, other for success.
 */
ucg_rank_t ucg_rank_map_eval(const ucg_rank_map_t *map, ucg_rank_t src_rank);

#endif