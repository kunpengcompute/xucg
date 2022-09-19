/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLAN_H_
#define UCG_PLAN_H_

#include "ucg_request.h"
#include "ucg_def.h"

#include "util/ucg_list.h"
#include "util/ucg_class.h"
#include "util/ucg_helper.h"
#include "util/ucg_mpool.h"
#include "planc/ucg_planc_def.h"

#include <limits.h>
#include <stdio.h>


#define UCG_PLAN_RANGE_MAX (ULONG_MAX)
#define UCG_PLAN_OPS_MAX 8

#define UCG_PLAN_ATTR_DESC \
    "Plan attribute that determines when to use the plan.\n" \
    "Syntax: <id>[<attributes>][<id>[<attributes>][...]]\n" \
    " - <id> : plan id(also called algorithm id), start with 'I:' followed by a\n" \
    "          non-negative integer\n" \
    " - <attributes> can be\n" \
    " - - score: plan priority, start with 'S:' followed by a non-negative integer\n" \
    "            larger number indicates a higher priority.\n" \
    " - - range: message size range, start with 'R:' followed by 'start-end'\n" \
    "            if there is no '-end', it means no upper limit.\n" \
    " - - group: applicable group size, start with 'G:' followed by 'start-end'\n" \
    "            if there is no '-end', it means no upper limit.\n" \
    "Example: I:1S:10R:0-G:0-40I:2S:9R:0-1024G:0-\n"

/* Define a plan attribute table */
#define UCG_PLAN_ATTR_TABLE_DEFINE(_scope) \
    ucg_plan_attr_t *_scope##_plan_attr_table[UCG_COLL_TYPE_LAST];

/* Declare a plan attribute table */
#define UCG_PLAN_ATTR_TABLE_DECLARE(_scope) \
    extern ucg_plan_attr_t *_scope##_plan_attr_table[UCG_COLL_TYPE_LAST];

/* Register a plan attribute array to table, the array should be NULL-terminate */
#define UCG_PLAN_ATTR_REGISTER_TABLE(_scope, _coll_type, _plan_attr_array) \
    UCG_STATIC_INIT { \
        _scope##_plan_attr_table[_coll_type] = _plan_attr_array; \
        return; \
    } \
    UCG_STATIC_CLEANUP { \
        _scope##_plan_attr_table[_coll_type] = NULL; \
        return; \
    }

#define UCG_PLAN_ATTR_ARRAY(_scope, _coll_type) \
    _scope##_plan_attr_table[_coll_type]

#define UCG_PLAN_ATTR_IS_LAST(_plan_attr) \
    ((_plan_attr)->prepare == NULL)


typedef enum ucg_plan_type {
    UCG_PLAN_TYPE_FIRST_CLASS,
    UCG_PLAN_TYPE_FALLBACK,
} ucg_plan_type_t;

/**
 * @brief Plan operation
 */
typedef struct ucg_plan_op ucg_plan_op_t;

/**
 * @brief Type of plan operation routine
 */
typedef ucg_status_t (*ucg_plan_op_func_t)(ucg_plan_op_t *op);

/**
 * @brief Type of preparing plan operation
 */
typedef ucg_status_t (*ucg_plan_prepare_func_t)(ucg_vgroup_t *vgroup,
                                                const ucg_coll_args_t *args,
                                                ucg_plan_op_t **op);

/**
 * @brief Plan operation.
 */
typedef struct ucg_plan_op {
    ucg_request_t super;
    /** Progress the operation. */
    ucg_plan_op_func_t progress;
    /** Start the operation. */
    ucg_plan_op_func_t trigger;
    /** Release the operation. */
    ucg_plan_op_func_t discard;
    /** Group that executes the op. */
    ucg_vgroup_t *vgroup;
} ucg_plan_op_t;
UCG_CLASS_DECLARE(ucg_plan_op_t,
                  UCG_CLASS_CTOR_ARGS(
                      ucg_vgroup_t *vgroup,
                      ucg_plan_op_func_t trigger,
                      ucg_plan_op_func_t progress,
                      ucg_plan_op_func_t discard,
                      const ucg_coll_args_t *args));

/**
 * @brief Meta plan operation.
 * @details Meta op is an op that consist of multiple plan ops. All ops are
 * executed in the order in which they are added. It can easily implement a
 * collective operation by combining multiple exist plan ops.
 */
typedef struct ucg_plan_meta_op {
    ucg_plan_op_t super;
    int n_ops;
    int n_completed_ops;
    uint8_t triggered;
    ucg_plan_op_t *ops[UCG_PLAN_OPS_MAX];
} ucg_plan_meta_op_t;

typedef struct ucg_plan_range {
    uint64_t start;
    uint64_t end;
} ucg_plan_range_t;

/**
 * @brief Plan attributes.
 */
typedef struct ucg_plan_attr {
    /** Function for preparing operation. */
    ucg_plan_prepare_func_t prepare;
    /** Unique id in its domain. */
    int32_t id;
    /** Plan's name */
    const char *name;
    /** Domain which the plan belongs to. Each domain has a set of plans. */
    const char *domain;
    /** If it's 1, the plan will not be used. */
    int8_t deprecated;
    /** Supported range of message size. */
    ucg_plan_range_t range;
    /** Plan provider. */
    ucg_vgroup_t *vgroup;
    /** Plan score, larger value indicate higher priority. */
    uint32_t score;
} ucg_plan_attr_t;

/**
 * @brief Plan structure.
 *
 * Plan represents the method of performing the collective operation. It can
 * generate executable operation through @ref ucg_plan_attr_t::prepare.
 */
typedef struct ucg_plan {
    ucg_plan_attr_t attr;

    /* The following are administrative fields */

    ucg_plan_type_t type;
    /** Element of plan container linked list. */
    ucg_list_link_t list;
    /** For first-class plan, it's the linked list header. */
    ucg_list_link_t fallback;
} ucg_plan_t;

/**
 * @brief Plan container
 */
typedef struct ucg_plans {
    ucg_list_link_t plans[UCG_COLL_TYPE_LAST][UCG_MEM_TYPE_LAST];
} ucg_plans_t;

/**
 * @brief Parameters of adding a new plan.
 */
typedef struct ucg_plan_params {
    /** Supported memory type. */
    ucg_mem_type_t mem_type;
    /** Supported collective type. */
    ucg_coll_type_t coll_type;
    /** Attribute of plan. */
    ucg_plan_attr_t attr;
} ucg_plan_params_t;

/**
 * @brief Initialize plan container
 */
ucg_status_t ucg_plans_init(ucg_plans_t **plans);

/**
 * @brief Cleanup plan container
 */
void ucg_plans_cleanup(ucg_plans_t *plans);

/**
 * @brief Print detail of all plans for debug purpose.
 *
 * @param [in] plans        Plan container
 * @param [in] stream       Output
 */
void ucg_plans_print(const ucg_plans_t *plans, FILE *stream);

/**
 * @brief Add one plan
 *
 * The first-class plans have diffrent mem_type, coll_type and range. If the
 * new plan has the same mem_type, coll_type and overlaping range with the
 * existing plans, this routine will split the plans. After splitting, there are
 * two cases.
 * 1. Plans with diffrent range: all plans become first-class citizen.
 * 2. Plans with same range: high-score plan becomes first-class citizen and take
 *    the low-score as fallback.
 *
 * This routine can be invoked to add deprecated plan. However, the deprecated
 * plan is not added in fact.
 *
 * @param [in] plans    Plan container.
 * @param [in] params   Parameters of plan.
 * @retval UCG_OK Success.
 * @retval Otherwise Failure.
 */
ucg_status_t ucg_plans_add(ucg_plans_t *plans, const ucg_plan_params_t *params);

/**
 * @brief Merge plan container
 *
 * @param [inout] dst   Dest plan container.
 * @param [in]    src   Source plan container.
 * @retval UCG_OK Success.
 * @retval Otherwise Failure.
 */
ucg_status_t ucg_plans_merge(ucg_plans_t **dst, const ucg_plans_t *src);

/**
 * @brief Select the best plan and prepare the operation.
 *
 * @param [in]  plans   Plan container.
 * @param [in]  args    Arguments of collective operation.
 * @param [in]  size    Group size.
 * @param [out] op      Plan operation.
 * @retval UCG_OK Success.
 * @retval Otherwise Failure.
 */
ucg_status_t ucg_plans_prepare(const ucg_plans_t *plans,
                               const ucg_coll_args_t *args,
                               const uint32_t size,
                               ucg_plan_op_t **op);

/**
 * @brief Update the plan attribute
 *
 * @param [inout] attr              Plan attribute
 * @param [in]    update            Description of updating plan attribute,
 *                                  @ref UCG_PLAN_ATTR_DESC
 *
 * @retval UCG_OK                   Updated successfully; the update is NULL
 *                                  or empty string; No matching ID exists in
 *                                  the update.
 * @retval UCG_ERR_INVALID_PARAMS   The format of update is incorrect.
 */
ucg_status_t ucg_plan_attr_update(ucg_plan_attr_t *attr, const char *update);

/**
 * @brief Update the i-th plan attribute by range and score
 *
 * @param [inout] attr              Plan attribute array
 * @param [in]    id                Algorithm id
 * @param [in]    start             Start range
 * @param [in]    end               End range
 * @param [in]    score             Plan score
 */
static inline
void ucg_plan_attr_array_update(ucg_plan_attr_t *attr, int32_t id,
                                uint64_t start, uint64_t end, int32_t score)
{
    UCG_CHECK_NULL_VOID(attr);
    for (ucg_plan_attr_t *tmp_attr = attr; !UCG_PLAN_ATTR_IS_LAST(tmp_attr); ++tmp_attr) {
        if (tmp_attr->id == id) {
            tmp_attr->range = (ucg_plan_range_t){start, end};
            tmp_attr->score = score;
        }
    }
    return;
}

/**
 * @brief Create one meta op
 */
ucg_plan_meta_op_t *ucg_plan_meta_op_new(ucg_group_t *group,
                                         ucg_vgroup_t *vgroup,
                                         const ucg_coll_args_t *args);

/**
 * @brief Add one op
 */
static inline ucg_status_t ucg_plan_meta_op_add(ucg_plan_meta_op_t *meta_op,
                                                ucg_plan_op_t *op)
{
    UCG_CHECK_NULL_INVALID(meta_op, op);

    if (meta_op->n_ops >= UCG_PLAN_OPS_MAX) {
        return UCG_ERR_NO_MEMORY;
    }
    meta_op->ops[meta_op->n_ops++] = op;
    return UCG_OK;
}

#endif