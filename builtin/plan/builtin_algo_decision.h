/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2021.  ALL RIGHTS RESERVED.
 * Description: Algorithm decision for collective operation
 */

#ifndef UCG_BUILTIN_ALGO_DECISION_H
#define UCG_BUILTIN_ALGO_DECISION_H

#include <ucs/sys/compiler.h>
#include <ucg/api/ucg.h>

BEGIN_C_DECLS

coll_type_t ucg_builtin_get_coll_type(const ucg_collective_type_t *coll_type);

int ucg_builtin_algo_auto_select(const ucg_group_params_t *group_params,
                                const ucg_collective_params_t *coll_params);

int ucg_builtin_algo_check_fallback(const ucg_group_params_t *group_params,
                                    const ucg_collective_params_t *coll_params,
                                    int algo);

int ucg_builtin_algo_decision(const ucg_group_params_t *group_params,
                              const ucg_collective_params_t *coll_params);

END_C_DECLS
#endif