/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_UCX_P2P_H_
#define UCG_PLANC_UCX_P2P_H_

#include "ucg/api/ucg.h"
#include "planc_ucx_def.h"
#include "core/ucg_dt.h"
#include "core/ucg_vgroup.h"

#include <ucp/api/ucp.h>

#define UCG_PLANC_UCX_CHECK_GOTO(_cmd, _op, _label) \
    do { \
        ucg_status_t _status = (_cmd); \
        if (_status != UCG_OK) { \
            (_op)->super.super.status = _status; \
            goto _label; \
        } \
    } while(0)

/**
 * UCG tag structure:
 *
 * 01234567 01234567 | 01234567 01234567 01234567 | 01234567 01234567 01234567
 *                   |                            |
 *     op seq (16)   |      source rank (24)      |        group id (24)
 *                   |                            |
 */
#define UCG_PLANC_UCX_SEQ_BITS 16
#define UCG_PLANC_UCX_RANK_BITS 24
#define UCG_PLANC_UCX_GROUP_BITS 24

#define UCG_PLANC_UCX_SEQ_BITS_OFFSET   (UCG_PLANC_UCX_RANK_BITS + UCG_PLANC_UCX_GROUP_BITS)
#define UCG_PLANC_UCX_RANK_BITS_OFFSET  (UCG_PLANC_UCX_GROUP_BITS)
#define UCG_PLANC_UCX_ID_BITS_OFFSET    0

#define UCG_PLANC_UCX_TAG_MASK          -1

#define UCG_PLANC_UCX_TAG_SENDER_MASK   UCG_MASK(UCG_PLANC_UCX_RANK_BITS + UCG_PLANC_UCX_GROUP_BITS)

typedef struct ucg_planc_ucx_p2p_req {
    /* trade-off, sizeof(ompi_request_t)=160 */
    uint8_t prev[160];
    int free_in_cb;
} ucg_planc_ucx_p2p_req_t;

typedef struct ucg_planc_ucx_p2p_state {
    /** It's only going to be UCG_OK or UCG_ERR_IO_ERROR */
    ucg_status_t status;
    int inflight_send_cnt;
    int inflight_recv_cnt;
} ucg_planc_ucx_p2p_state_t;

typedef struct ucg_planc_ucx_p2p_params {
    /** The real ucx group on which the vgroup depends, can not be NULL. */
    ucg_planc_ucx_group_t *ucx_group;
    /** Recording isend/irecv state, can not be NULL. */
    ucg_planc_ucx_p2p_state_t *state;
    /** Saving the pending p2p request, can be NULL. */
    ucg_planc_ucx_p2p_req_t **request;
} ucg_planc_ucx_p2p_params_t;

/**
 * @brief Send and immediate return
 *
 * @param [in] buffer       The buffer to send.
 * @param [in] count        The number of elements to send.
 * @param [in] dt           The type of one buffer element.
 * @param [in] vrank        The rank of recipient process.
 * @param [in] tag          Message tag.
 * @param [in] vgroup       The vgroup in which the isend takes place.
 * @param [in] params       Additional information.
 * @retval UCG_OK Success
 * @retval Otherwise Failed
 */
ucg_status_t ucg_planc_ucx_p2p_isend(const void *buffer, int32_t count,
                                     ucg_dt_t *dt, ucg_rank_t vrank,
                                     uint16_t tag, ucg_vgroup_t *vgroup,
                                     ucg_planc_ucx_p2p_params_t *params);

/**
 * @brief Receive and immediate return
 *
 * @param [out] buffer      The buffer in which receive the message.
 * @param [in]  count       The number of elements in the buffer given.
 * @param [in]  dt          The type of one buffer element.
 * @param [in]  vrank       The rank of recipient process.
 * @param [in]  tag         Message tag.
 * @param [in]  vgroup      The vgroup in which the isend takes place.
 * @param [in]  params      Additional information.
 * @retval UCG_OK Success
 * @retval Otherwise Failed
 */
ucg_status_t ucg_planc_ucx_p2p_irecv(void *buffer, int32_t count,
                                     ucg_dt_t *dt, ucg_rank_t vrank,
                                     uint16_t tag, ucg_vgroup_t *vgroup,
                                     ucg_planc_ucx_p2p_params_t *params);

/**
 * @brief Check whether the p2p request is done.
 *
 * Refer to the isend/irecv prototype, using ucx group as an argument instead
 * of vgroup can be a bit confused. However, we cannot get enough
 * information from the vgroup, so we can only use ucx group.
 *
 * @param [in]    ucx_group     UCX group in which the p2p take places.
 * @param [inout] req           P2P request, if the request is complete,
 *                              it is set to NULL.
 * @retval UCG_INPROGRESS The request is in progress.
 * @retval UCG_OK The request completed successfully.
 * @retval Otherwise The request is failed.
 */
ucg_status_t ucg_planc_ucx_p2p_test(ucg_planc_ucx_group_t *ucx_group,
                                    ucg_planc_ucx_p2p_req_t **req);

/**
 * @brief Check whether all p2p requests are done.
 *
 * @param [in] ucx_group        UCX group in which the p2p take places.
 * @param [in] state            Send/Receive state.
 * @retval UCG_INPROGRESS Some requests are in progress.
 * @retval UCG_OK All requests completed successfully.
 * @retval Otherwise Some requests are failed.
 */
ucg_status_t ucg_planc_ucx_p2p_testall(ucg_planc_ucx_group_t *ucx_group,
                                       ucg_planc_ucx_p2p_state_t *state);

/**
 * @brief Request initialization function registered with ucp
 */
void ucg_planc_ucx_p2p_req_init(void *request);

static inline void ucg_planc_ucx_p2p_state_reset(ucg_planc_ucx_p2p_state_t *state)
{
    state->status = UCG_OK;
    state->inflight_send_cnt = 0;
    state->inflight_recv_cnt = 0;
    return;
}

#endif