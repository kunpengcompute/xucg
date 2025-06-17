/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef UCG_PLANC_STARS_OFFLOAD_H_
#define UCG_PLANC_STARS_OFFLOAD_H_

#include "planc_stars_plan.h"


typedef void (*ucg_planc_stars_construct_send_desc_func_t)(ucg_plan_op_t *ucg_op,
                                                           stars_comm_plan_h plan,
                                                           uint32_t rank_index,
                                                           scp_ofd_req_elem_h io_desc);


ucg_status_t ucg_planc_stars_rank_dep_alloc(stars_comm_dep_h comm_dep);
ucg_status_t ucg_planc_stars_rank_dep_init(ucg_planc_stars_op_t *op, stars_rank_info_h peer,
                                           ucg_rank_t peer_id, uint32_t elem_len);


ucg_status_t ucg_planc_stars_fill_ofd_put_req_elem(uint32_t eid_idx, stars_rank_info_h peer_rank,
                                                   scp_ofd_req_elem_h request);
ucg_status_t ucg_planc_stars_fill_ofd_wait_req_elem(uint32_t eid_idx, stars_rank_info_h peer,
                                                    scp_ofd_req_elem_h request, stars_event_elem_h event_elem);
ucg_status_t ucg_planc_stars_fill_ofd_barrier_req_elem(stars_rank_info_h peer, scp_ofd_req_elem_h request);
#endif