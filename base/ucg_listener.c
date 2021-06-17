/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "ucg_listener.h"
#include "ucg_group.h"

#include "ucp/core/ucp_ep.h"

static uint8_t ucg_listener_am_id = 0;

static void ucg_group_listener_accept_cb(ucp_ep_h ep, void *arg)
{
    ucg_group_h group              = (ucg_group_h)arg;
    ucp_request_param_t params     = {
            .op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS,
            .flags        = UCP_AM_SEND_FLAG_EAGER
    };
    ucg_listener_group_info_t info = {
            .id           = group->params.id,
            .member_count = group->params.member_count,
            .member_index = group->params.member_index
    };

    /* Send back the group information (blocking, but likely immediate) */
    ucs_status_ptr_t status = ucp_am_send_nbx(ep, ucg_listener_am_id, 0, 0,
                                              &info, sizeof(info), &params);
    if (UCS_PTR_IS_PTR(status)) {
        do {
            ucp_worker_progress(ep->worker);
        } while (ucp_request_check_status(status) == UCS_INPROGRESS);
    }
}

ucs_status_t ucg_group_listener_create(ucg_group_h group,
                                       ucs_sock_addr_t *bind_address,
                                       ucg_listener_h *listener_p)
{
    ucp_listener_h super;
    ucp_listener_params_t params = {
            .sockaddr       = *bind_address,
            .accept_handler = {
                    .cb  = ucg_group_listener_accept_cb,
                    .arg = group
            }
    };

    return ucp_listener_create(group->worker, &params, &super);
}

ucs_status_t ucg_group_listener_connect(ucg_group_h group,
                                        ucs_sock_addr_t *listener_addr)
{
    ucp_ep_h ep;
    ucs_status_t status;
    ucp_ep_params_t params = {
            .field_mask = UCP_EP_PARAM_FIELD_SOCK_ADDR |
                          UCP_EP_PARAM_FIELD_FLAGS,
            .sockaddr   = *listener_addr,
            .flags      = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER
    };

    ucs_assert(group->params.member_count == 0);

    status = ucp_ep_create(group->worker, &params, &ep);
    if (status != UCS_OK) {
        return status;
    }

    /* wait for the group information to arrive (via Active Message) */
    while (group->params.member_count == 0) {
        ucp_worker_progress(group->worker);
    }

    return UCS_OK;
}

void ucg_group_listener_destroy(ucg_listener_h listener)
{
    ucp_listener_destroy(listener->super);
}

static ucs_status_t ucg_group_listener_set_info_cb(void *arg, void *data,
                                                   size_t length,
                                                   unsigned flags)
{
    ucg_group_h group;
    ucs_list_link_t *groups_head    = (ucs_list_link_t*)arg;
    ucg_listener_group_info_t *info = (ucg_listener_group_info_t*)data;

    ucs_assert(length == sizeof(*info));

    ucs_list_for_each(group, groups_head, list) {
        if (group->params.id == info->id) {
            group->params.member_index = info->member_index;
            group->params.member_count = info->member_count;

            return UCS_OK;
        }
    }

    return UCS_ERR_NO_ELEM;
}

static void ucg_group_listener_trace_info_cb(void *arg,
                                             uct_am_trace_type_t type,
                                             uint8_t id, const void *data,
                                             size_t length, char *buffer,
                                             size_t max)
{
}

ucs_status_t ucg_listener_am_init(uint8_t am_id, ucs_list_link_t *groups_head)
{
    ucg_listener_am_id = am_id;
    return ucg_context_set_am_handler(groups_head, am_id,
                                      ucg_group_listener_set_info_cb,
                                      ucg_group_listener_trace_info_cb);
}
