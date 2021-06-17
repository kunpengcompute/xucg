/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_LISTENER_H_
#define UCG_LISTENER_H_

#include <ucg/api/ucg.h>
#include <ucs/datastruct/list.h>

struct ucg_listener {
    ucp_listener_h super;
};

typedef struct ucg_listener_group_info {
    ucg_group_id_t           id;
    ucg_group_member_index_t member_count;
    ucg_group_member_index_t member_index;
} ucg_listener_group_info_t;

ucs_status_t ucg_listener_am_init(uint8_t am_id, ucs_list_link_t *groups_head);

#endif /* UCG_LISTENER_H_ */
