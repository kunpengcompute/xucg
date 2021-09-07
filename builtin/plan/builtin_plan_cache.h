/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2021.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_BUILTIN_PLAN_CACHE_H
#define UCG_BUILTIN_PLAN_CACHE_H

#include <ucs/sys/compiler.h>
#include <ucg/base/ucg_group.h>

BEGIN_C_DECLS

ucs_status_t ucg_builtin_pcache_init(ucg_group_h group);

void ucg_builtin_pcache_destroy(ucg_group_h group);
 
ucg_plan_t *ucg_builtin_pcache_find(const ucg_group_h group, int algo,
                                    const ucg_collective_params_t *coll_params);

void ucg_builtin_pcache_update(ucg_group_h group, ucg_plan_t *plan, int algo,
                                    const ucg_collective_params_t *coll_params);
END_C_DECLS
#endif