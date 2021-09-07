/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 * Description: Rabenseifner algorithm for MPI_Allreduce
 */

#include <math.h>
#include <ucs/debug/log.h>
#include <ucs/debug/assert.h>
#include <ucs/debug/memtrack.h>
#include <uct/api/uct_def.h>
#include <ucs/arch/bitops.h>
#include "builtin_plan.h"

typedef struct ucg_builtin_binary_block_params {
    ucg_builtin_base_params_t super;
} ucg_builtin_binary_block_params_t;

/****************************************************************************
 *                                                                          *
 *                        Binary block's Algorithm                          *
 *                allreduce = reduce_scatter + allgather                    *
 *          reduce_scatter phase   0: Recursive Having   1:  tree           *
 *          allgather      phase   0: Recursive Doubling                    *
 *                                                                          *
 ****************************************************************************/

/*
 * @brief   Only keep the lowest 1 in the binary
 * e.g. 5(101) -> 1(001)
 */
STATIC_GTEST unsigned ucg_builtin_keep_lowest_1_bit(unsigned num)
{
    return num & (~num + 1);
}

/*
 * @brief   Only keep the highest 1 in the binary
 * e.g. 5(101) -> 4(100)
 */
STATIC_GTEST unsigned ucg_builtin_keep_highest_1_bit(unsigned num)
{
    unsigned high = 0x0;
    while (num) {
        high = num & (~num + 1);
        num = num & (~high);
    }
    return high;
}

/*
 * @brief   Get the number of 1 in the binary
 * e.g. 5(101) -> 2
 */
STATIC_GTEST unsigned ucg_builtin_get_1bit_cnt(unsigned num)
{
    unsigned cnt = 0;
    while (num > 0) {
        num &= (num - 1);   // clear the lowest 1
        cnt++;
    }
    return cnt;
}

/*
 * @brief   Keep the least bits of the CNT that are the same as those of the NUM.
 * e.g. NUM: 0010 0100
 *      CNT: 1011 1110
 *      RET: 0011 1110
 */
STATIC_GTEST unsigned ucg_builtin_get_low_all(unsigned num, unsigned cnt)
{
    unsigned high = ucg_builtin_keep_highest_1_bit(num);
    unsigned lowMask = (high - 1) | high;
    unsigned ret = cnt & lowMask;
    return ret;
}

/*
 * @brief  Get the process_cnt and begin_index of previous group
 * @param  member_cnt                   the number of all process
 * @param  previous_group_process_cnt   the process number of previous group
 * @param  previous_group_begin_index   the begin process index of previous group
 * e.g.my_index:6, member_cnt:15:(1) (2 3) (4 5 6 7) (8 9 ... 15)
 *     ==> previous_group_process_cnt:2, previous_group_begin_index:2
 */
STATIC_GTEST void ucg_builtin_get_binaryblocks_previous_group(unsigned my_index,
                                                              unsigned member_cnt,
                                                              unsigned *previous_group_process_cnt,
                                                              unsigned *previous_group_begin_index)
{
    unsigned ret = ucg_builtin_get_low_all(my_index, member_cnt);
    if (my_index > ret) {
        *previous_group_begin_index = ret;
        *previous_group_process_cnt = ucg_builtin_keep_highest_1_bit(ret);
    } else {
        *previous_group_begin_index = (~ucg_builtin_keep_highest_1_bit(ret)) & ret;
        *previous_group_process_cnt = ucg_builtin_keep_highest_1_bit(*previous_group_begin_index);
    }
    *previous_group_begin_index = (~ucg_builtin_keep_highest_1_bit(*previous_group_begin_index)) &
                                  (*previous_group_begin_index);
}

/*
 * @brief  Get the process_cnt and begin_index of current group
 * @param  member_cnt                  the number of all process
 * @param  current_group_process_cnt   the process number of current group
 * @param  current_group_begin_index   the begin process index of current group
 * e.g.my_index:6, member_cnt:15:(1) (2 3) (4 5 6 7) (8 9 ... 15)
 *     ==> current_group_process_cnt:4, current_group_begin_index:4
 */
STATIC_GTEST void ucg_builtin_get_binaryblocks_current_group(unsigned my_index,
                                                             unsigned member_cnt,
                                                             unsigned *current_group_process_cnt,
                                                             unsigned *current_group_begin_index)
{
    unsigned ret = ucg_builtin_get_low_all(my_index, member_cnt);
    if (my_index > ret) {
        *current_group_process_cnt = ucg_builtin_keep_lowest_1_bit(member_cnt - ret);
        *current_group_begin_index = ret;
    } else {
        *current_group_process_cnt = ucg_builtin_keep_highest_1_bit(ret);
        *current_group_begin_index = (~ucg_builtin_keep_highest_1_bit(ret)) & ret;
    }
}

/*
 * @brief  Get the process_cnt and begin_index of next group
 * @param  member_cnt                  the number of all process
 * @param  next_group_process_cnt      the process number of next group
 * @param  next_group_begin_index      the begin process index of next group
 * e.g.my_index:6, member_cnt:15:(1) (2 3) (4 5 6 7) (8 9 ... 15)
 *     ==> next_group_process_cnt:8, next_group_begin_index:8
 */
STATIC_GTEST void ucg_builtin_get_binaryblocks_next_group(unsigned my_index,
                                                          unsigned member_cnt,
                                                          unsigned *next_group_process_cnt,
                                                          unsigned *next_group_begin_index)
{
    unsigned ret = ucg_builtin_get_low_all(my_index, member_cnt);
    if (my_index <= ret) {
        *next_group_process_cnt = ucg_builtin_keep_lowest_1_bit(member_cnt - ret);
        *next_group_begin_index = ret;
    } else {
        *next_group_process_cnt = ucg_builtin_keep_lowest_1_bit(member_cnt - ret);
        *next_group_process_cnt = ucg_builtin_keep_lowest_1_bit(member_cnt - ret - *next_group_process_cnt);
        *next_group_begin_index = (*next_group_process_cnt - 1) & member_cnt;
    }
}

/*
 * @brief  Get the number of group before current group
 * @param  current_group_begin_index   the begin process index of current group
 * @param  ahead_group_cnt             the number of group in front of current group
 * e.g.  current_group_begin_index:4, member_cnt:15:(1) (2 3) (4 5 6 7) (8 9 ... 15)
 *       ==> ahead_group_cnt:2
 */
STATIC_GTEST void ucg_builtin_get_binaryblocks_ahead_group_cnt(unsigned member_cnt,
                                                               unsigned current_group_begin_index,
                                                               unsigned *ahead_group_cnt)
{
    if (current_group_begin_index == 0) {
        *ahead_group_cnt = 0;
    } else {
        unsigned previous_sum_group_process_cnt = member_cnt - (~current_group_begin_index & member_cnt);
        *ahead_group_cnt = ucg_builtin_get_1bit_cnt(previous_sum_group_process_cnt);
    }
}

/*
 * @brief  Get the number of group after current group
 * @param  next_group_begin_index      the begin process index of next group
 * @param  behind_group_cnt            the number of group in front of current group
 * e.g.  next_group_begin_index:4, member_cnt:15:(1) (2 3) (4 5 6 7) (8 9 ... 15)
 *       ==> behind_group_cnt:2
 */
STATIC_GTEST void ucg_builtin_get_binaryblocks_behind_group_cnt(unsigned member_cnt,
                                                                unsigned next_group_begin_index,
                                                                unsigned *behind_group_cnt)
{
    if (next_group_begin_index >= member_cnt) {
        *behind_group_cnt = 0;
    } else {
        unsigned after_sum_group_process_cnt = member_cnt - next_group_begin_index;
        *behind_group_cnt = ucg_builtin_get_1bit_cnt(after_sum_group_process_cnt);
    }
}

/*
 * @brief   Get the peer process index in extra_reduction phase
 */
STATIC_GTEST void ucg_builtin_get_extra_reduction_peer_index(unsigned my_index,
                                                             unsigned member_cnt,
                                                             unsigned *peer_index)
{
    unsigned previous_group_process_cnt, previous_group_begin_index;
    ucg_builtin_get_binaryblocks_previous_group(my_index, member_cnt,
                                                &previous_group_process_cnt, &previous_group_begin_index);
    if (previous_group_process_cnt == 0) {
        *peer_index = my_index - 1;
    } else {
        unsigned offset = (my_index - previous_group_begin_index) % previous_group_process_cnt;
        offset = (offset == 0) ? previous_group_process_cnt : offset;
        *peer_index = previous_group_begin_index + offset - 1;
    }
}

/*
 * @brief  Obtains the index of the block to be sent
 * @param  ep_cnt  the number of endpoint
 * @param  ep_idx  the index of endpoint
 * @return the index of the block to be sent
 * e.g. ep_cnt:8, ep_idx:3
 *      ==> arr:[0 4 2 6 1 5 3 7], ret:a[3-1]=6
 */
STATIC_GTEST ucs_status_t ucg_builtin_get_recv_block_index(unsigned ep_cnt, unsigned ep_idx, unsigned* ret)
{
    size_t alloc_size = ep_cnt * sizeof(unsigned);
    unsigned *arr = (unsigned *)ucs_malloc(alloc_size, "arr");
    if (arr == NULL) {
        return UCS_ERR_NO_MEMORY;
    }
    memset(arr, 0, alloc_size);
    unsigned i;
    const unsigned power_of_two = 2;
    for (i = 0; i < ep_cnt; i++) {
        if (!(i & (i - 1))) {
            arr[i] = i ? (ep_cnt / power_of_two / i) : 0;
        }
    }
    for (i = 0; i < ep_cnt; i++) {
        unsigned distance = 1;
        unsigned value = ep_cnt / power_of_two;
        while (i + distance < ep_cnt && i + distance < i * power_of_two && !arr[i + distance]) {
            arr[i + distance] = arr[i] + value;
            distance *= power_of_two;
            value /= power_of_two;
        }
    }
    *ret = arr[ep_idx];
    ucs_free(arr);
    arr = NULL;
    return UCS_OK;
}

STATIC_GTEST void ucg_builtin_block_buffer(unsigned buffer_cnt,
                                           unsigned block_cnt,
                                           unsigned * const block_buffer)
{
    unsigned idx;
    unsigned tmp = block_cnt;
    for (idx = 0; idx < block_cnt; idx++, tmp--) {
        block_buffer[idx] = buffer_cnt / tmp;
        if (buffer_cnt % tmp) {
            block_buffer[idx]++;
        }
        buffer_cnt -= block_buffer[idx];
    }
}

STATIC_GTEST UCS_F_ALWAYS_INLINE unsigned ucg_builtin_calc_disp(unsigned *block_num, unsigned start, unsigned cnt)
{
    unsigned idx;
    unsigned sum = 0;
    for (idx = start; idx < start + cnt; idx++) {
        sum += block_num[idx];
    }
    return sum;
}

STATIC_GTEST ucs_status_t ucg_builtin_divide_block_buffers(unsigned block_cnt,
                                                           unsigned total_group_process_cnt,
                                                           unsigned total_group_cnt,
                                                           unsigned **block_buffers)
{
    unsigned previous_group_process_cnt = 0;
    unsigned previous_group_begin_index = 0;
    unsigned current_group_process_cnt, current_group_begin_index;
    unsigned group_idx, temp;
    ucs_status_t status = UCS_OK;
    for (group_idx = 0; group_idx < total_group_cnt; group_idx++) {
        if (group_idx == 0) {
            ucg_builtin_get_binaryblocks_current_group(1, total_group_process_cnt,
                                                       &current_group_process_cnt, &current_group_begin_index);
            unsigned *block_buffer = (unsigned *)ucs_malloc(sizeof(unsigned) *
                                current_group_process_cnt, "allocate block");
            if (block_buffer == NULL) {
                status = UCS_ERR_NO_MEMORY;
                goto cleanup_buffer;
            }
            ucg_builtin_block_buffer(block_cnt, current_group_process_cnt, block_buffer);
            block_buffers[group_idx] = block_buffer;
        } else {
            ucg_builtin_get_binaryblocks_next_group(previous_group_begin_index + 1, total_group_process_cnt,
                                                    &current_group_process_cnt, &current_group_begin_index);
            unsigned *block_buffer = (unsigned *)ucs_malloc(sizeof(unsigned) *
                                current_group_process_cnt, "allocate block");
            if (block_buffer == NULL) {
                status = UCS_ERR_NO_MEMORY;
                goto cleanup_buffer;
            }
            unsigned idx;
            unsigned step = current_group_process_cnt / previous_group_process_cnt;
            for (idx = 0; idx < previous_group_process_cnt; idx++) {
                unsigned previous_group_block_cnt = block_buffers[group_idx - 1][idx];
                ucg_builtin_block_buffer(previous_group_block_cnt, step, &block_buffer[idx * step]);
            }
            block_buffers[group_idx] = block_buffer;
        }
        previous_group_process_cnt = current_group_process_cnt;
        previous_group_begin_index = current_group_begin_index;
    }
    return status;

cleanup_buffer:
    for (temp = 0; temp < group_idx; temp++) {
        ucs_free(block_buffers[temp]);
        block_buffers[temp] = NULL;
    }
    return status;
}

STATIC_GTEST void ucg_builtin_destory_block_buffers(unsigned total_group_cnt, unsigned **block_buffers)
{
    unsigned group_idx;
    for (group_idx = 0; group_idx < total_group_cnt; group_idx++) {
        ucs_free(block_buffers[group_idx]);
        block_buffers[group_idx] = NULL;
    }
    ucs_free(block_buffers);
    block_buffers = NULL;
}

STATIC_GTEST ucs_status_t ucg_builtin_init_block_buffers(unsigned block_cnt,
                                                         unsigned total_group_process_cnt,
                                                         unsigned total_group_cnt,
                                                         unsigned ***block_buffers)
{
    *block_buffers = (unsigned**)ucs_malloc(sizeof(unsigned *)*total_group_cnt, "allocate blocks");
    if (*block_buffers == NULL) {
        return UCS_ERR_NO_MEMORY;
    }

    ucs_status_t status = ucg_builtin_divide_block_buffers(block_cnt, total_group_process_cnt,
                                                           total_group_cnt, *block_buffers);
    if (status != UCS_OK) {
        ucs_free(*block_buffers);
        *block_buffers = NULL;
    }
    return status;
}

STATIC_GTEST ucs_status_t ucg_builtin_reduce_scatter_phase_cb(ucg_builtin_plan_phase_t *phase,
                                                              const ucg_collective_params_t *coll_params)
{
    ucs_assert(phase != NULL && coll_params != NULL);

    static unsigned next_start_block = 0;
    // first phase: static variable reset
    if (phase->raben_extend.first_step_flag) {
        next_start_block = 0;
    }

    unsigned block_cnt                  = coll_params->send.count;
    unsigned total_group_cnt            = phase->raben_extend.index_group.total_group_cnt;
    unsigned total_group_process_cnt    = phase->raben_extend.index_group.total_group_process_cnt;
    unsigned **block_buffers            = NULL;
    ucs_status_t status = ucg_builtin_init_block_buffers(block_cnt, total_group_process_cnt,
                                                         total_group_cnt, &block_buffers);
    if (status != UCS_OK) {
        return status;
    }

    unsigned cur_group_begin_index = phase->raben_extend.index_group.cur_group_begin_index;
    unsigned cur_group_process_cnt = phase->raben_extend.index_group.cur_group_process_cnt;
    unsigned ahead_group_cnt       = phase->raben_extend.index_group.ahead_group_cnt;
    const unsigned factor = 2;
    /* local peer index */
    unsigned local_group_index   = phase->raben_extend.index_group.local_group_index - cur_group_begin_index;
    unsigned step_size           = 1 << phase->raben_extend.step_index;
    unsigned step_base           = local_group_index - local_group_index % (step_size * factor);
    unsigned local_group_peer    = step_base + (local_group_index - step_base + step_size) % (step_size * factor);

    /* send && receive blocks index */
    unsigned send_num_blocks     = (cur_group_process_cnt / factor) >> phase->raben_extend.step_index;
    unsigned send_start_block    = next_start_block + ((local_group_index < local_group_peer) ? send_num_blocks : 0);
    unsigned recv_num_blocks     = send_num_blocks;
    unsigned recv_start_block    = next_start_block + ((local_group_index < local_group_peer) ? 0 : send_num_blocks);
    next_start_block            += ((local_group_index < local_group_peer) ? 0 : send_num_blocks);

    /* send && receive real blocks */
    phase->ex_attr.start_block       = ucg_builtin_calc_disp(block_buffers[ahead_group_cnt], 0, send_start_block);
    phase->ex_attr.num_blocks        = ucg_builtin_calc_disp(block_buffers[ahead_group_cnt],
                                                             send_start_block, send_num_blocks);
    phase->ex_attr.peer_start_block  = ucg_builtin_calc_disp(block_buffers[ahead_group_cnt], 0, recv_start_block);
    phase->ex_attr.peer_block        = ucg_builtin_calc_disp(block_buffers[ahead_group_cnt],
                                                             recv_start_block, recv_num_blocks);
    phase->ex_attr.total_num_blocks  = block_cnt;
    phase->ex_attr.is_inequal        = 1;
    phase->ex_attr.is_partial        = 1;
    /* free */
    ucg_builtin_destory_block_buffers(total_group_cnt, block_buffers);
    return UCS_OK;
}

STATIC_GTEST ucs_status_t ucg_builtin_intra_reduce_scatter(ucg_builtin_index_group_t *index_group,
                                                           ucg_builtin_plan_phase_t **phase,
                                                           ucg_builtin_plan_t *binary_block,
                                                           ucg_builtin_group_ctx_t *ctx,
                                                           ucg_step_idx_t *step_idx)
{
    ucs_status_t status = UCS_OK;
    unsigned idx;
    unsigned step_size = 1;
    const unsigned factor = 2;
    ucg_group_member_index_t local_group_index = index_group->local_group_index - index_group->cur_group_begin_index;
    unsigned step_cnt = ucs_ilog2(index_group->cur_group_process_cnt);
    unsigned high = ucg_builtin_keep_highest_1_bit(index_group->total_group_process_cnt);
    for (idx = 0; idx < step_cnt && status == UCS_OK; idx++, (*phase)++, step_size *= factor) {
        (*phase)->step_index = *step_idx + idx;
        (*phase)->method     = UCG_PLAN_METHOD_REDUCE_SCATTER_RECURSIVE;
        (*phase)->ep_cnt     = 1;
    #if ENABLE_DEBUG_DATA
        (*phase)->indexes = UCS_ALLOC_CHECK(sizeof(ucg_group_member_index_t), "binary block indexes");
    #endif
        binary_block->ep_cnt++;
        binary_block->phs_cnt++;

        /* reduce-scatter phase peer index */
        unsigned step_base              = local_group_index-local_group_index % (step_size *factor);
        unsigned local_group_peer_index = step_base + (local_group_index - step_base + step_size) %
                                           (step_size * factor);
        /* Calculate relative real process ID using local index, only continuous process IDs are supported. */
        ucg_group_member_index_t real_peer_index = index_group->my_index + local_group_peer_index - local_group_index;
        status = ucg_builtin_connect(ctx, real_peer_index, *phase, UCG_BUILTIN_CONNECT_SINGLE_EP);
        (*phase)->raben_extend.step_index      = idx;
        (*phase)->raben_extend.first_step_flag = (idx ? 0 : 1);
        (*phase)->raben_extend.index_group     = *index_group;
        (*phase)->init_phase_cb                = ucg_builtin_reduce_scatter_phase_cb;
    }
    *step_idx += ucs_ilog2(high);
    return status;
}

STATIC_GTEST ucs_status_t ucg_builtin_extra_reduce_receive_cb(ucg_builtin_plan_phase_t *phase,
                                                             const ucg_collective_params_t *coll_params)
{
    if (phase == NULL || coll_params == NULL) {
        return UCS_ERR_INVALID_PARAM;
    }
    unsigned block_cnt                  = coll_params->send.count;
    unsigned total_group_cnt            = phase->raben_extend.index_group.total_group_cnt;
    unsigned total_group_process_cnt    = phase->raben_extend.index_group.total_group_process_cnt;
    unsigned **block_buffers            = NULL;
    ucs_status_t status = ucg_builtin_init_block_buffers(block_cnt, total_group_process_cnt,
                                                         total_group_cnt, &block_buffers);
    if (status != UCS_OK) {
        return status;
    }

    unsigned ahead_group_cnt            = phase->raben_extend.index_group.ahead_group_cnt;
    unsigned recv_block_index           = phase->raben_extend.index_group.recv_block_index;

    phase->ex_attr.start_block          = ucg_builtin_calc_disp(block_buffers[ahead_group_cnt], 0, recv_block_index);
    phase->ex_attr.num_blocks           = block_buffers[ahead_group_cnt][recv_block_index];
    phase->ex_attr.peer_start_block     = phase->ex_attr.start_block;
    phase->ex_attr.peer_block           = phase->ex_attr.num_blocks;
    phase->ex_attr.total_num_blocks     = coll_params->send.count;
    phase->ex_attr.is_partial           = 1;

    ucg_builtin_destory_block_buffers(total_group_cnt, block_buffers);
    return UCS_OK;
}

STATIC_GTEST ucs_status_t ucg_builtin_extra_reduce_send_cb(ucg_builtin_plan_phase_t *phase,
                                                           const ucg_collective_params_t *coll_params)
{
    ucs_assert(phase != NULL && coll_params != NULL);

    unsigned block_cnt                  = coll_params->send.count;
    unsigned total_group_cnt            = phase->raben_extend.index_group.total_group_cnt;
    unsigned total_group_process_cnt    = phase->raben_extend.index_group.total_group_process_cnt;
    unsigned **block_buffers            = NULL;
    ucs_status_t status = ucg_builtin_init_block_buffers(block_cnt, total_group_process_cnt,
                                                         total_group_cnt, &block_buffers);
    if (status != UCS_OK) {
        return status;
    }

    unsigned cur_group_process_cnt   = phase->raben_extend.index_group.cur_group_process_cnt;
    unsigned next_group_process_cnt  = phase->raben_extend.index_group.next_group_process_cnt;
    unsigned next_group_idx          = phase->raben_extend.index_group.ahead_group_cnt + 1;
    unsigned recv_block_index        = phase->raben_extend.index_group.recv_block_index;
    unsigned ep_cnt                  = next_group_process_cnt / cur_group_process_cnt;
    unsigned idx                     = recv_block_index * ep_cnt;

    phase->ex_attr.peer_start_block  = ucg_builtin_calc_disp(block_buffers[phase->raben_extend.index_group.ahead_group_cnt],
                                                             0, recv_block_index);
    phase->ex_attr.peer_block        = block_buffers[phase->raben_extend.index_group.ahead_group_cnt][recv_block_index];
    phase->ex_attr.start_block       = phase->ex_attr.peer_start_block;
    phase->ex_attr.start_block      += ucg_builtin_calc_disp(block_buffers[next_group_idx], idx,
                                                             phase->raben_extend.step_index);
    phase->ex_attr.num_blocks         = block_buffers[next_group_idx][idx + phase->raben_extend.step_index];
    phase->ex_attr.total_num_blocks  = block_cnt;
    phase->ex_attr.is_partial        = 1;

    ucg_builtin_destory_block_buffers(total_group_cnt, block_buffers);
    return UCS_OK;
}

STATIC_GTEST ucs_status_t ucg_builtin_intra_extra_reduction(ucg_builtin_index_group_t *index_group,
                                                            ucg_builtin_plan_phase_t **phase,
                                                            ucg_builtin_plan_t *binary_block,
                                                            ucg_builtin_group_ctx_t  *ctx,
                                                            ucg_step_idx_t *step_idx)
{
    ucs_status_t status = UCS_OK;

    /* receive first, intra socket or node */
    if (index_group->local_group_index != index_group->local_peer_ahead_group) {
        (*phase)->step_index = *step_idx + (index_group->ahead_group_cnt - 1);
        (*phase)->method = UCG_PLAN_METHOD_REDUCE_TERMINAL;
        (*phase)->ep_cnt = 1;
#if ENABLE_DEBUG_DATA
        (*phase)->indexes = UCS_ALLOC_CHECK(sizeof(ucg_group_member_index_t), "binary block indexes");
#endif
        binary_block->ep_cnt++;
        binary_block->phs_cnt++;
        (*phase)->raben_extend.index_group = *index_group;
        (*phase)->init_phase_cb            = ucg_builtin_extra_reduce_receive_cb;
        ucg_group_member_index_t real_peer_index = index_group->my_index +
                                                   index_group->local_peer_ahead_group -
                                                   index_group->local_group_index;
        status = ucg_builtin_connect(ctx, real_peer_index, *phase, UCG_BUILTIN_CONNECT_SINGLE_EP);
        (*phase)++;
    }

    /* then send, intra socket or node */
    if (status == UCS_OK && index_group->next_group_process_cnt > 0) {
        unsigned idx;
        unsigned ep_cnt = index_group->next_group_process_cnt / index_group->cur_group_process_cnt;
        for (idx = 0; idx < ep_cnt && status == UCS_OK; ++idx) {
            (*phase)->step_index = *step_idx + index_group->ahead_group_cnt;
            (*phase)->method     = UCG_PLAN_METHOD_SEND_TERMINAL;
            (*phase)->ep_cnt     = 1;
#if ENABLE_DEBUG_DATA
            (*phase)->indexes = UCS_ALLOC_CHECK(sizeof(ucg_group_member_index_t), "binary block indexes");
#endif
            binary_block->ep_cnt++;
            binary_block->phs_cnt++;
            unsigned recv_block_index;
            status = ucg_builtin_get_recv_block_index(ep_cnt, idx, &recv_block_index);
            if (status != UCS_OK) {
                return status;
            }
            unsigned peer_index = index_group->next_group_begin_index + index_group->local_group_index -
                                  index_group->cur_group_begin_index + recv_block_index *
                                  index_group->cur_group_process_cnt;
            (*phase)->raben_extend.step_index        = idx;
            (*phase)->raben_extend.index_group       = *index_group;
            (*phase)->init_phase_cb                  = ucg_builtin_extra_reduce_send_cb;
            ucg_group_member_index_t real_peer_index = index_group->my_index + peer_index -
                                                       index_group->local_group_index;
            status = ucg_builtin_connect(ctx, real_peer_index, *phase, UCG_BUILTIN_CONNECT_SINGLE_EP);
            (*phase)++;
        }
    }
    *step_idx += index_group->total_group_cnt - 1;
    return status;
}

STATIC_GTEST ucs_status_t ucg_builtin_intra_node_allreduce_cb(ucg_builtin_plan_phase_t *phase,
                                                              const ucg_collective_params_t *coll_params)
{
    ucs_assert(phase != NULL && coll_params != NULL);

    unsigned block_cnt                  = coll_params->send.count;
    unsigned total_group_cnt            = phase->raben_extend.index_group.total_group_cnt;
    unsigned total_group_process_cnt    = phase->raben_extend.index_group.total_group_process_cnt;
    unsigned **block_buffers            = NULL;
    ucs_status_t status = ucg_builtin_init_block_buffers(block_cnt, total_group_process_cnt,
                                                         total_group_cnt, &block_buffers);
    if (status != UCS_OK) {
        return status;
    }

    unsigned ahead_group_cnt            = phase->raben_extend.index_group.ahead_group_cnt;
    unsigned recv_block_index           = phase->raben_extend.index_group.recv_block_index;

    phase->ex_attr.start_block          = ucg_builtin_calc_disp(block_buffers[ahead_group_cnt], 0, recv_block_index);
    phase->ex_attr.num_blocks           = block_buffers[ahead_group_cnt][recv_block_index];
    phase->ex_attr.peer_start_block     = phase->ex_attr.start_block;
    phase->ex_attr.peer_block           = phase->ex_attr.num_blocks;
    phase->ex_attr.total_num_blocks     = block_cnt;
    phase->ex_attr.is_partial           = 1;

    ucg_builtin_destory_block_buffers(total_group_cnt, block_buffers);
    return UCS_OK;
}

STATIC_GTEST ucs_status_t ucg_builtin_extra_receive_bcast_cb(ucg_builtin_plan_phase_t *phase,
                                                             const ucg_collective_params_t *coll_params)
{
    ucs_assert(phase != NULL && coll_params != NULL);

    unsigned block_cnt                  = coll_params->send.count;
    unsigned total_group_cnt            = phase->raben_extend.index_group.total_group_cnt;
    unsigned total_group_process_cnt    = phase->raben_extend.index_group.total_group_process_cnt;
    unsigned **block_buffers            = NULL;
    ucs_status_t status = ucg_builtin_init_block_buffers(block_cnt, total_group_process_cnt,
                                                         total_group_cnt, &block_buffers);
    if (status != UCS_OK) {
        return status;
    }

    unsigned current_group_process_cnt   = phase->raben_extend.index_group.cur_group_process_cnt;
    unsigned next_group_process_cnt      = phase->raben_extend.index_group.next_group_process_cnt;
    unsigned next_group_idx              = phase->raben_extend.index_group.ahead_group_cnt + 1;
    unsigned idx                         = phase->raben_extend.index_group.recv_block_index *
                                           (next_group_process_cnt / current_group_process_cnt);

    /* receive previous phase address */
    phase->ex_attr.start_block          = ucg_builtin_calc_disp(block_buffers[next_group_idx],
                                                                0, idx + phase->raben_extend.step_index);
    phase->ex_attr.num_blocks           = block_buffers[next_group_idx][idx + phase->raben_extend.step_index];
    phase->ex_attr.peer_start_block     = phase->ex_attr.start_block;
    phase->ex_attr.peer_block           = phase->ex_attr.num_blocks;
    phase->ex_attr.total_num_blocks     = block_cnt;
    phase->ex_attr.is_partial           = 1;

    ucg_builtin_destory_block_buffers(total_group_cnt, block_buffers);
    return UCS_OK;
}

STATIC_GTEST ucs_status_t ucg_builtin_extra_send_bcast_cb(ucg_builtin_plan_phase_t *phase,
                                                             const ucg_collective_params_t *coll_params)
{
    ucs_assert(phase != NULL && coll_params != NULL);

    unsigned block_cnt                  = coll_params->send.count;
    unsigned total_group_cnt            = phase->raben_extend.index_group.total_group_cnt;
    unsigned total_group_process_cnt    = phase->raben_extend.index_group.total_group_process_cnt;
    unsigned **block_buffers            = NULL;
    ucs_status_t status = ucg_builtin_init_block_buffers(block_cnt, total_group_process_cnt,
                                                         total_group_cnt, &block_buffers);
    if (status != UCS_OK) {
        return status;
    }

    unsigned ahead_group_cnt            = phase->raben_extend.index_group.ahead_group_cnt;
    unsigned start_block                = phase->raben_extend.index_group.recv_block_index;

    phase->ex_attr.start_block          = ucg_builtin_calc_disp(block_buffers[ahead_group_cnt], 0, start_block);
    phase->ex_attr.num_blocks           = block_buffers[ahead_group_cnt][start_block];
    phase->ex_attr.peer_start_block     = phase->ex_attr.start_block;
    phase->ex_attr.peer_block           = phase->ex_attr.num_blocks;
    phase->ex_attr.total_num_blocks     = block_cnt;
    phase->ex_attr.is_partial           = 1;

    ucg_builtin_destory_block_buffers(total_group_cnt, block_buffers);
    return UCS_OK;
}

STATIC_GTEST ucs_status_t ucg_builtin_intra_bcast(ucg_builtin_index_group_t *index_group,
                                                  ucg_builtin_plan_phase_t **phase,
                                                  ucg_builtin_plan_t *binary_block,
                                                  ucg_builtin_group_ctx_t  *ctx,
                                                  ucg_step_idx_t *step_idx)
{
    ucs_status_t status = UCS_OK;

    /* receive first */
    if (index_group->next_group_process_cnt > 0) {
        unsigned idx, peer_idx;
        unsigned step_cnt = index_group->next_group_process_cnt / index_group->cur_group_process_cnt;
        for (idx = 0; idx < step_cnt && status == UCS_OK; idx++) {
            (*phase)->step_index = *step_idx + (index_group->behind_group_cnt - 1);
            (*phase)->method     = UCG_PLAN_METHOD_RECV_TERMINAL;
            (*phase)->ep_cnt     = 1;
#if ENABLE_DEBUG_DATA
            (*phase)->indexes    = UCS_ALLOC_CHECK(sizeof(ucg_group_member_index_t), "binary block indexes");
#endif
            binary_block->ep_cnt++;
            binary_block->phs_cnt++;
            unsigned recv_block_index;
            status = ucg_builtin_get_recv_block_index(step_cnt, idx, &recv_block_index);
            if (status != UCS_OK) {
                return status;
            }
            peer_idx = index_group->next_group_begin_index + index_group->local_group_index -
                       index_group->cur_group_begin_index + recv_block_index *
                       index_group->cur_group_process_cnt;
            (*phase)->raben_extend.step_index        = idx;
            (*phase)->raben_extend.index_group       = *index_group;
            (*phase)->init_phase_cb                  = ucg_builtin_extra_receive_bcast_cb;
            ucg_group_member_index_t real_peer_index = index_group->my_index + peer_idx -
                                                       index_group->local_group_index;
            status = ucg_builtin_connect(ctx, real_peer_index, *phase, UCG_BUILTIN_CONNECT_SINGLE_EP);
            (*phase)++;
        }
    }

    /* then send */
    if (status == UCS_OK && index_group->local_peer_ahead_group != index_group->local_group_index) {
        (*phase)->step_index = *step_idx + index_group->behind_group_cnt;
        (*phase)->method     = UCG_PLAN_METHOD_SEND_TERMINAL;
        (*phase)->ep_cnt     = 1;
#if ENABLE_DEBUG_DATA
        (*phase)->indexes = UCS_ALLOC_CHECK(sizeof(ucg_group_member_index_t), "binary block indexes");
#endif
        binary_block->ep_cnt++;
        binary_block->phs_cnt++;

        (*phase)->raben_extend.index_group = *index_group;
        (*phase)->init_phase_cb            = ucg_builtin_extra_send_bcast_cb;
        ucg_group_member_index_t real_peer_index = index_group->my_index +
                                                   index_group->local_peer_ahead_group -
                                                   index_group->local_group_index;
        status = ucg_builtin_connect(ctx, real_peer_index, *phase, UCG_BUILTIN_CONNECT_SINGLE_EP);
        (*phase)++;
    }
    *step_idx += index_group->total_group_cnt - 1;
    return status;
}

STATIC_GTEST ucs_status_t ucg_builtin_extra_allgather_cb(ucg_builtin_plan_phase_t *phase,
                                                         const ucg_collective_params_t *coll_params)
{
    ucs_assert(phase != NULL && coll_params != NULL);

    unsigned block_cnt                  = coll_params->send.count;
    unsigned total_group_cnt            = phase->raben_extend.index_group.total_group_cnt;
    unsigned total_group_process_cnt    = phase->raben_extend.index_group.total_group_process_cnt;
    unsigned **block_buffers            = NULL;
    ucs_status_t status = ucg_builtin_init_block_buffers(block_cnt, total_group_process_cnt,
                                                         total_group_cnt, &block_buffers);
    if (status != UCS_OK) {
        return status;
    }
    const unsigned factor = 2;
    unsigned cur_group_begin_index   = phase->raben_extend.index_group.cur_group_begin_index;
    unsigned cur_group_process_cnt   = phase->raben_extend.index_group.cur_group_process_cnt;
    unsigned ahead_group_cnt         = phase->raben_extend.index_group.ahead_group_cnt;
    unsigned local_group_idx         = phase->raben_extend.index_group.local_group_index - cur_group_begin_index;
    unsigned step_size               = (cur_group_process_cnt / factor) >> phase->raben_extend.step_index;
    unsigned step_base               = local_group_idx -local_group_idx % (step_size * factor);
    unsigned local_group_peer        = step_base + (local_group_idx - step_base + step_size) % (step_size * factor);

    static unsigned send_start_block = 0;
    if (phase->raben_extend.step_index == 0) {
        send_start_block = phase->raben_extend.index_group.recv_block_index;
    }

    /* send && receive block */
    unsigned recv_start_block    = send_start_block;
    unsigned num_blocks          = 1 << phase->raben_extend.step_index;
    recv_start_block             = (local_group_idx < local_group_peer) ? (recv_start_block + num_blocks) :
                                   (recv_start_block - num_blocks);

    phase->ex_attr.start_block       = ucg_builtin_calc_disp(block_buffers[ahead_group_cnt],
                                                             0, send_start_block);
    phase->ex_attr.num_blocks        = ucg_builtin_calc_disp(block_buffers[ahead_group_cnt],
                                                             send_start_block, num_blocks);
    phase->ex_attr.peer_start_block  = ucg_builtin_calc_disp(block_buffers[ahead_group_cnt],
                                                             0, recv_start_block);
    phase->ex_attr.peer_block        = ucg_builtin_calc_disp(block_buffers[ahead_group_cnt],
                                                             recv_start_block, num_blocks);
    phase->ex_attr.total_num_blocks  = block_cnt;
    phase->ex_attr.is_inequal        = 1;
    phase->ex_attr.is_partial        = 1;

    if (local_group_idx > local_group_peer) {
        send_start_block = recv_start_block;
    }

    ucg_builtin_destory_block_buffers(total_group_cnt, block_buffers);
    return UCS_OK;
}

STATIC_GTEST ucs_status_t ucg_builtin_intra_allgather(ucg_builtin_index_group_t *index_group,
                                                      ucg_builtin_plan_phase_t **phase,
                                                      ucg_builtin_plan_t *binary_block,
                                                      ucg_builtin_group_ctx_t *ctx,
                                                      ucg_step_idx_t *step_idx)
{
    ucs_status_t status = UCS_OK;

    unsigned idx;
    const unsigned factor = 2;
    unsigned step_cnt = ucs_ilog2(index_group->cur_group_process_cnt);
    unsigned step_size = index_group->cur_group_process_cnt / factor;
    unsigned high = ucg_builtin_keep_highest_1_bit(index_group->total_group_process_cnt);
    ucg_group_member_index_t local_group_index = index_group->local_group_index - index_group->cur_group_begin_index;
    for (idx = 0; idx < step_cnt && status == UCS_OK; idx++, step_size/= factor) {
        (*phase)->step_index = *step_idx + idx;
        (*phase)->method     = UCG_PLAN_METHOD_EXCHANGE;
        (*phase)->ep_cnt     = 1;
#if ENABLE_DEBUG_DATA
        (*phase)->indexes = UCS_ALLOC_CHECK(sizeof(ucg_group_member_index_t), "binary block indexes");
#endif
        binary_block->ep_cnt++;
        binary_block->phs_cnt++;

        unsigned step_base              = local_group_index - local_group_index % (step_size *factor);
        unsigned local_group_peer       = step_base + (local_group_index - step_base + step_size) %
                                          (step_size * factor);
        ucg_group_member_index_t real_peer_index = index_group->my_index + local_group_peer - local_group_index;
        status = ucg_builtin_connect(ctx, real_peer_index, *phase, UCG_BUILTIN_CONNECT_SINGLE_EP);

        (*phase)->raben_extend.step_index      = idx;
        (*phase)->raben_extend.index_group     = *index_group;
        (*phase)->init_phase_cb                = ucg_builtin_extra_allgather_cb;
        (*phase)++;
    }
    *step_idx += ucs_ilog2(high);
    return status;
}

STATIC_GTEST ucs_status_t ucg_builtin_binary_block_init(unsigned local_idx,
                                                        unsigned total_group_process_cnt,
                                                        ucg_builtin_plan_t *binary_block,
                                                        ucg_builtin_plan_phase_t **phase,
                                                        ucg_builtin_group_ctx_t *ctx,
                                                        ucg_step_idx_t *step_idx,
                                                        ucg_builtin_index_group_t *index_group)
{
    unsigned ahead_group_cnt, behind_group_cnt;
    unsigned cur_group_process_cnt, cur_group_begin_index;
    unsigned next_group_process_cnt, next_group_begin_index;
    unsigned local_ahead_peer;
    ucs_status_t status;

    ucg_builtin_get_binaryblocks_current_group(local_idx + 1, total_group_process_cnt,
                                               &cur_group_process_cnt, &cur_group_begin_index);
    ucg_builtin_get_binaryblocks_ahead_group_cnt(total_group_process_cnt, cur_group_begin_index,
                                                 &ahead_group_cnt);
    ucg_builtin_get_binaryblocks_next_group(local_idx + 1, total_group_process_cnt,
                                            &next_group_process_cnt, &next_group_begin_index);
    ucg_builtin_get_binaryblocks_behind_group_cnt(total_group_process_cnt, next_group_begin_index,
                                                  &behind_group_cnt);
    ucg_builtin_get_extra_reduction_peer_index(local_idx + 1, total_group_process_cnt,
                                               &local_ahead_peer);

    index_group->my_index                 = binary_block->super.my_index;
    index_group->cur_group_begin_index    = cur_group_begin_index;
    index_group->cur_group_process_cnt    = cur_group_process_cnt;
    index_group->next_group_begin_index   = next_group_begin_index;
    index_group->next_group_process_cnt   = next_group_process_cnt;
    index_group->total_group_process_cnt  = total_group_process_cnt;
    index_group->ahead_group_cnt          = ahead_group_cnt;
    index_group->behind_group_cnt         = behind_group_cnt;
    index_group->total_group_cnt          = ucg_builtin_get_1bit_cnt(total_group_process_cnt);
    index_group->local_group_index        = local_idx;
    index_group->local_peer_ahead_group   = local_ahead_peer;
    index_group->recv_block_index         = 0;

    status = ucg_builtin_get_recv_block_index(cur_group_process_cnt, local_idx - cur_group_begin_index,
                                              &index_group->recv_block_index);
    if (status != UCS_OK) {
        return status;
    }
    *phase = &binary_block->phss[binary_block->phs_cnt];
    *step_idx         = binary_block->step_cnt;

    /* 1st part: intra-node/socket reduce-scatter, use local index */
    status = ucg_builtin_intra_reduce_scatter(index_group, phase, binary_block, ctx, step_idx);
    return status;
}

STATIC_GTEST ucs_status_t ucg_builtin_binary_block_build(ucg_builtin_plan_t *binary_block,
                                                         ucg_builtin_group_ctx_t *ctx,
                                                         const ucg_builtin_config_t *config,
                                                         ucg_builtin_topo_aware_params_t *params,
                                                         const ucg_group_member_index_t member_cnt)
{
    ucs_status_t status;
    ucg_step_idx_t step_idx                = 0;
    unsigned total_group_process_cnt       = member_cnt;
    unsigned local_idx                     = binary_block->super.my_index;
    ucg_builtin_plan_phase_t  *phase = NULL;
    ucg_builtin_index_group_t  index_group;

    /* 1st part: intra-node/socket reduce-scatter, use local index */
    status = ucg_builtin_binary_block_init(local_idx, total_group_process_cnt, binary_block,
                                           &phase, ctx, &step_idx, &index_group);
    if (status != UCS_OK) {
        ucs_error("Binary-blocks algorithm failed in intra reduce-scatter phase");
        return status;
    }
    binary_block->step_cnt = step_idx;

    /* 2nd part: intra-node/socket rabenseifner extra reduction */
    phase = &binary_block->phss[binary_block->phs_cnt];
    status = ucg_builtin_intra_extra_reduction(&index_group, &phase, binary_block, ctx, &step_idx);
    if (status != UCS_OK) {
        ucs_error("Binary-blocks algorithm failed in intra reduction phase");
        return status;
    }
    binary_block->step_cnt = step_idx;

    /* 5th part: intra-node broadcast */
    phase = &binary_block->phss[binary_block->phs_cnt];
    status = ucg_builtin_intra_bcast(&index_group, &phase, binary_block, ctx, &step_idx);
    if (status != UCS_OK) {
        ucs_error("Binary-blocks algorithm failed in intra-node broadcast phase");
        return status;
    }
    binary_block->step_cnt = step_idx;

    /* 6th part: intra-node allgather */
    phase = &binary_block->phss[binary_block->phs_cnt];
    status = ucg_builtin_intra_allgather(&index_group, &phase, binary_block, ctx, &step_idx);
    if (status != UCS_OK) {
        ucs_error("Binary-blocks algorithm failed in intra-node allgather phase");
        return status;
    }
    binary_block->step_cnt = step_idx;
    return status;
}

STATIC_GTEST void ucg_builtin_modify_buffer(ucg_builtin_plan_t *binary_block,
                                              unsigned phs_start,
                                              ucg_builtin_index_group_t *index_group)
{
    unsigned phs_end = binary_block->phs_cnt;

    ucg_builtin_plan_phase_t *local_phase = &binary_block->phss[phs_start];
    /* modify buffer */
    unsigned phs_idx;
    for (phs_idx = phs_start; phs_idx < phs_end; phs_idx++, local_phase++) {
        local_phase->raben_extend.index_group  = *index_group;
        local_phase->init_phase_cb             = ucg_builtin_intra_node_allreduce_cb;
        local_phase->ex_attr.is_partial        = 1;
    }
}

STATIC_GTEST ucs_status_t ucg_builtin_topo_aware_binary_block_build(ucg_builtin_plan_t *binary_block,
                                                                    ucg_builtin_group_ctx_t *ctx,
                                                                    const ucg_builtin_config_t *config,
                                                                    ucg_builtin_topo_aware_params_t *params,
                                                                    const ucg_group_member_index_t member_cnt)
{
    ucs_status_t status;

    /* Ensure that continuous process IDs in a node */
    unsigned ppn                            = params->topo_params->num_local_procs;
    unsigned node_cnt                       = params->topo_params->node_cnt;
    ucg_group_member_index_t* node_leaders  = params->topo_params->node_leaders;
    unsigned node_leaders_shift             = binary_block->super.my_index -
                                              *(params->topo_params->local_members);

    unsigned node_idx;
    for (node_idx = 0; node_idx < node_cnt; ++node_idx) {
        node_leaders[node_idx] += node_leaders_shift;
    }

    /* Ensure that continuous process IDs in a socket */
    unsigned pps                             = params->topo_params->local.socket.member_cnt;
    unsigned local_socket_cnt                = params->topo_params->local.socket.num;
    ucg_group_member_index_t* socket_leaders = params->topo_params->local.socket.leaders;
    unsigned socket_leaders_shift            = binary_block->super.my_index -
                                              *(params->topo_params->local.socket.members);

    ucg_step_idx_t step_idx                = 0;
    ucg_builtin_plan_phase_t *phase = NULL;
    ucg_builtin_index_group_t index_group;
    unsigned total_group_process_cnt       = 0;
    unsigned local_idx                     = 0;
    switch (ucg_algo.topo_level) {
        case UCG_GROUP_HIERARCHY_LEVEL_NODE:
            total_group_process_cnt  = ppn;
            local_idx                = node_leaders_shift;
            break;

        case UCG_GROUP_HIERARCHY_LEVEL_SOCKET:
            total_group_process_cnt  = pps;
            local_idx                = socket_leaders_shift;
            break;

        case UCG_GROUP_HIERARCHY_LEVEL_L3CACHE:
            break;

        default:
            ucs_error("The current topolevel is not supported");
            break;
    }

    /* 1st part: intra-node/socket reduce-scatter, use local index */
    status = ucg_builtin_binary_block_init(local_idx, total_group_process_cnt, binary_block, &phase,
                                           ctx, &step_idx, &index_group);
    if (status != UCS_OK) {
        ucs_error("Binary-blocks topology-aware algorithm failed in intra reduce-scatter phase");
        return status;
    }
    binary_block->step_cnt = step_idx;

    /* 2nd part: intra-node/socket rabenseifner extra reduction */
    phase = &binary_block->phss[binary_block->phs_cnt];
    status = ucg_builtin_intra_extra_reduction(&index_group, &phase, binary_block, ctx, &step_idx);
    if (status != UCS_OK) {
        ucs_error("Binary-blocks topology-aware algorithm failed in intra reduction phase");
        return status;
    }
    binary_block->step_cnt = step_idx;

    if (ucg_algo.topo_level == UCG_GROUP_HIERARCHY_LEVEL_SOCKET) {
        if (index_group.next_group_begin_index == total_group_process_cnt && local_socket_cnt > 1) {
            /* 3rd part: intra-socket allreduce */
            /* Only the largest group in socket participates in the recursive operation. */
            unsigned phs_start = binary_block->phs_cnt;
            status = ucg_builtin_recursive_binary_build(binary_block, params->super.ctx, config,
                                                        socket_leaders, local_socket_cnt, UCG_PLAN_BUILD_PARTIAL,
                                                        UCG_PLAN_RECURSIVE_TYPE_ALLREDUCE);
            if (status != UCS_OK) {
                ucs_error("Binary-blocks topology-aware algorithm failed in intra reduce phase");
                return status;
            }
            ucg_builtin_modify_buffer(binary_block, phs_start, &index_group);
        }
        /*update step idx */
        step_idx += local_socket_cnt;
        binary_block->step_cnt = step_idx;
    }

    /* 4th part: inter-node allreduce */
    /* Only the largest group in socket participates in the recursive operation. */
    if (index_group.next_group_begin_index == total_group_process_cnt && node_cnt > 1) {
        unsigned phs_start = binary_block->phs_cnt;
        status = ucg_builtin_recursive_binary_build(binary_block, params->super.ctx, config,
                                                    node_leaders, node_cnt, UCG_PLAN_BUILD_PARTIAL,
                                                    UCG_PLAN_RECURSIVE_TYPE_ALLREDUCE);
        if (status != UCS_OK) {
            ucs_error("Binary-blocks topology-aware algorithm failed in inter reduce phase");
            return status;
        }
        ucg_builtin_modify_buffer(binary_block, phs_start, &index_group);
    }
    /*update step idx */
    step_idx += node_cnt;
    binary_block->step_cnt = step_idx;

    /* 5th part: intra-node broadcast */
    phase = &binary_block->phss[binary_block->phs_cnt];
    status = ucg_builtin_intra_bcast(&index_group, &phase, binary_block, ctx, &step_idx);
    if (status != UCS_OK) {
        ucs_error("Binary-blocks topology-aware algorithm failed in intra-node broadcast phase");
        return status;
    }
    binary_block->step_cnt = step_idx;

    /* 6th part: intra-node allgather */
    phase = &binary_block->phss[binary_block->phs_cnt];
    status = ucg_builtin_intra_allgather(&index_group, &phase, binary_block, ctx, &step_idx);
    if (status != UCS_OK) {
        ucs_error("Binary-blocks topology-aware algorithm failed in intra-node allgather phase");
        return status;
    }
    binary_block->step_cnt = step_idx;

    return status;
}

static ucg_builtin_plan_t *ucg_builtin_binary_block_allocate_plan(const ucg_group_params_t *group_params,
                                                                  const ucg_builtin_topo_params_t *topo_params)
{
    unsigned local_idx = 0;
    unsigned member_cnt = 0;

    unsigned total_phs_cnt = 0;
    unsigned intra_node_phs_cnt = 0;
    unsigned inter_node_phs_cnt = 0;

    if (ucg_algo.topo == 0) {
        local_idx = group_params->member_index;
        member_cnt = group_params->member_count;
    } else {
        if (ucg_algo.topo_level == UCG_GROUP_HIERARCHY_LEVEL_NODE) {
            local_idx = group_params->member_index - *topo_params->local_members;
            member_cnt = topo_params->num_local_procs;
            intra_node_phs_cnt = topo_params->local.socket.num;
            inter_node_phs_cnt = topo_params->node_cnt;
        } else if (ucg_algo.topo_level == UCG_GROUP_HIERARCHY_LEVEL_SOCKET) {
            local_idx = group_params->member_index - *topo_params->local.socket.members;
            member_cnt = topo_params->local.socket.member_cnt;
            inter_node_phs_cnt = topo_params->node_cnt;
        }
    }
    total_phs_cnt += intra_node_phs_cnt;
    total_phs_cnt += inter_node_phs_cnt;

    unsigned pre_group_proc_cnt, pre_group_begin_idx;
    unsigned cur_group_proc_cnt, cur_group_begin_idx;
    unsigned next_group_proc_cnt, next_group_begin_idx;

    ucg_builtin_get_binaryblocks_previous_group(local_idx, member_cnt, &pre_group_proc_cnt, &pre_group_begin_idx);
    ucg_builtin_get_binaryblocks_current_group(local_idx + 1, member_cnt, &cur_group_proc_cnt, &cur_group_begin_idx);
    ucg_builtin_get_binaryblocks_next_group(local_idx + 1, member_cnt, &next_group_proc_cnt, &next_group_begin_idx);

    total_phs_cnt += ucs_ilog2(cur_group_proc_cnt);
    total_phs_cnt += 1;
    total_phs_cnt += next_group_proc_cnt / cur_group_proc_cnt;
    total_phs_cnt += 1;
    total_phs_cnt += next_group_proc_cnt / cur_group_proc_cnt;
    total_phs_cnt += ucs_ilog2(cur_group_proc_cnt);

    size_t alloc_size = sizeof(ucg_builtin_plan_t) +
        total_phs_cnt * (sizeof(ucg_builtin_plan_phase_t) + total_phs_cnt * sizeof(uct_ep_h));

    ucg_builtin_plan_t *binary_block = ucs_malloc(alloc_size, "rabenseifner algorithm");
    if (binary_block == NULL) {
        return NULL;
    }

    memset(binary_block, 0, alloc_size);

    return binary_block;
}

ucs_status_t ucg_builtin_binary_block_create(ucg_builtin_group_ctx_t *ctx,
                                             enum ucg_builtin_plan_topology_type plan_topo_type,
                                             const ucg_builtin_config_t *config,
                                             const ucg_group_params_t *group_params,
                                             const ucg_collective_type_t *coll_type,
                                             ucg_builtin_plan_t **plan_p)
{
    ucs_status_t status;

    /* topology information obtain from ompi layer */
    ucg_builtin_topo_params_t *topo_params =
            (ucg_builtin_topo_params_t *)UCS_ALLOC_CHECK(sizeof(ucg_builtin_topo_params_t), "topo params");

    status = ucg_builtin_query_topo(group_params, topo_params);
    if (status != UCS_OK) {
        ucs_error("query topo failed");
        ucs_free(topo_params);
        topo_params = NULL;
        return status;
    }

    ucg_builtin_base_params_t base = {
        .ctx = ctx,
        .coll_type = coll_type,
        .topo_type = plan_topo_type,
        .group_params = group_params,
    };

    ucg_builtin_topo_aware_params_t params = {
        .super = base,
        .root  = base.coll_type->root,
        .topo_params = topo_params,
    };

    ucg_builtin_plan_t *binary_block = ucg_builtin_binary_block_allocate_plan(group_params, topo_params);
    if (binary_block == NULL) {
        ucs_error("allocate binary blocks plan failed");
        status = UCS_ERR_NO_MEMORY;
        goto err;
    }

    /**
     * set my_index firstly,
     * it should be set in ucg_collective_create after plan is created
     **/
    binary_block->super.my_index = group_params->member_index;

    if (ucg_algo.topo == 0) {
            status = ucg_builtin_binary_block_build(binary_block, params.super.ctx, config,
                                                    &params, group_params->member_count);
            if (status != UCS_OK) {
            ucs_error("binary blocks method failed");
            ucg_builtin_free((void **)&binary_block);
            goto err;
        }
    } else if (ucg_algo.topo == 1) {
        status = ucg_builtin_topo_aware_binary_block_build(binary_block, params.super.ctx, config,
                                                           &params, group_params->member_count);
        if (status != UCS_OK) {
            ucs_error("Topo-aware binary blocks method failed");
            ucg_builtin_free((void **)&binary_block);
            goto err;
        }
    } else {
        ucs_error("Invalid parameters for binary blocks method");
        ucg_builtin_free((void **)&binary_block);
        status = UCS_ERR_INVALID_PARAM;
        goto err;
    }

    *plan_p = (ucg_builtin_plan_t*)binary_block;
err:
    ucg_builtin_destroy_topo(topo_params);
    return status;
}
