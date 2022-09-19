/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_LIST_H_
#define UCG_LIST_H_

#include <ucs/datastruct/list.h>

/* use the UCS link type */
#define ucg_list_link_t     ucs_list_link_t

#define ucg_list_head_init(_head)       ucs_list_head_init(_head)
#define ucg_list_add_tail(_head, _item) ucs_list_add_tail(_head, _item)
#define ucg_list_del(_elem)             ucs_list_del(_elem)
#define ucg_list_is_empty(_head)        ucs_list_is_empty(_head)
#define ucg_list_extract_head(_head, _type, _member) ucs_list_extract_head(_head, _type, _member)
#define ucg_list_length(_head)          ucs_list_length(_head)
#define ucg_list_head(_elem, _type, _member)  ucs_list_head(_elem, _type, _member)
#define ucg_list_next(_elem, _type, _member)  ucs_list_next(_elem, _type, _member)
#define ucg_list_insert_after(_pos, _item)    ucs_list_insert_after(_pos, _item)
#define ucg_list_insert_before(_pos, _item)   ucs_list_insert_before(_pos, _item)
#define ucg_list_splice_tail(_head, _newlist) ucs_list_splice_tail(_head, _newlist)

#define ucg_list_for_each_safe(_elem, _tmp_elem, _head, _member) \
            ucs_list_for_each_safe(_elem, _tmp_elem, _head, _member)
#define ucg_list_for_each(_elem, _head, _member) ucs_list_for_each(_elem, _head, _member)
#endif