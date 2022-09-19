/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ucg_plan.h"
#include "ucg_base.h"
#include "ucg_vgroup.h"
#include "ucg_group.h"

#include "util/ucg_log.h"
#include "util/ucg_math.h"
#include "util/ucg_malloc.h"
#include "util/ucg_mpool.h"


static ucg_status_t ucg_plan_op_ctor(ucg_plan_op_t *self,
                                     ucg_vgroup_t *vgroup,
                                     ucg_plan_op_func_t trigger,
                                     ucg_plan_op_func_t progress,
                                     ucg_plan_op_func_t discard,
                                     const ucg_coll_args_t *args)
{
    ucg_status_t status = UCG_CLASS_CONSTRUCT(ucg_request_t, &self->super, args);
    if (status != UCG_OK) {
        return status;
    }
    self->vgroup = vgroup;
    self->trigger = trigger;
    self->progress = progress;
    self->discard = discard;
    return UCG_OK;
}

static void ucg_plan_op_dtor(ucg_plan_op_t *self)
{
    UCG_CLASS_DESTRUCT(ucg_request_t, &self->super);
    return;
}
UCG_CLASS_DEFINE(ucg_plan_op_t, ucg_plan_op_ctor, ucg_plan_op_dtor);

static ucg_status_t ucg_plan_meta_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_plan_meta_op_t *meta_op = ucg_derived_of(ucg_op, ucg_plan_meta_op_t);

    int cur_op_idx = meta_op->n_completed_ops;
    ucg_plan_op_t *curr_op = meta_op->ops[cur_op_idx];

    if (!meta_op->triggered) {
        /* To ensure that requests of multiple members in the same collection op
           can be matched, all subops must have the same request ID. */
        curr_op->super.id = meta_op->super.super.id;
        status = curr_op->trigger(curr_op);
        meta_op->triggered = 1;
    }

    status = curr_op->progress(curr_op);
    if (status == UCG_OK) {
        ++meta_op->n_completed_ops;
        meta_op->triggered = 0;
        if (meta_op->n_completed_ops == meta_op->n_ops) {
            meta_op->super.super.status = UCG_OK;
            return UCG_OK;
        }
    } else if (status != UCG_INPROGRESS) {
        meta_op->super.super.status = status;
    }
    return meta_op->super.super.status;
}

static ucg_status_t ucg_plan_meta_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_plan_meta_op_t *meta_op = ucg_derived_of(ucg_op, ucg_plan_meta_op_t);

    if (meta_op->n_ops == 0) {
        meta_op->super.super.status = UCG_OK;
        return UCG_OK;
    }

    meta_op->super.super.status = UCG_INPROGRESS;
    meta_op->n_completed_ops = 0;

    ucg_status_t status = ucg_plan_meta_op_progress(ucg_op);
    if (status == UCG_INPROGRESS) {
        /* op is progressing and request start successfully */
        status = UCG_OK;
    }
    return status;
}

static ucg_status_t ucg_plan_meta_op_discard(ucg_plan_op_t *ucg_op)
{
    ucg_plan_meta_op_t *meta_op = ucg_derived_of(ucg_op, ucg_plan_meta_op_t);
    int n_ops = meta_op->n_ops;
    for (int i = 0; i < n_ops; ++i) {
        ucg_plan_op_t *op = meta_op->ops[i];
        op->discard(op);
    }
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, &meta_op->super);
    ucg_mpool_put(meta_op);
    return UCG_OK;
}

ucg_plan_meta_op_t *ucg_plan_meta_op_new(ucg_group_t *group,
                                         ucg_vgroup_t *vgroup,
                                         const ucg_coll_args_t *args)
{
    UCG_CHECK_NULL(NULL, group);

    ucg_plan_meta_op_t *meta_op = ucg_mpool_get(&group->context->meta_op_mp);
    if (meta_op == NULL) {
        goto err;
    }

    ucg_status_t status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &meta_op->super,
                                              vgroup,
                                              ucg_plan_meta_op_trigger,
                                              ucg_plan_meta_op_progress,
                                              ucg_plan_meta_op_discard,
                                              args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of meta op");
        goto err_free_meta_op;
    }

    meta_op->n_ops = 0;
    meta_op->n_completed_ops = 0;
    meta_op->triggered = 0;

    return meta_op;

err_free_meta_op:
    ucg_mpool_put(meta_op);
err:
    return NULL;
}

static const char* ucg_plan_attr_update_find(ucg_plan_attr_t *attr, const char *update)
{
    char attr_key[8] = {0};
    snprintf(attr_key, 8, "I:%d", attr->id);
    const char *attr_str = update;
    int skip = 0;
    while (1) {
        attr_str = strstr(attr_str, attr_key);
        if (attr_str == NULL) {
            return NULL;
        }
        /* Avoid matching between "I:1" and "I:12". */
        int32_t id;
        if (sscanf(attr_str, "I:%d%n", &id, &skip) != 1) {
            return NULL;
        }

        attr_str += skip;
        if (id != attr->id) {
            continue;
        }
        break;
    }
    return attr_str;
}

static void ucg_plan_print(const ucg_plan_t *plan, FILE *stream)
{
    if (plan->type == UCG_PLAN_TYPE_FIRST_CLASS) {
        fprintf(stream, "#\n");
        fprintf(stream, "# first class plan\n");
    } else {
        fprintf(stream, "# fallback plan\n");
    }
    const ucg_plan_attr_t *attr = &plan->attr;
    fprintf(stream, "#    domain    : %s\n", attr->domain);
    fprintf(stream, "#    id        : %d\n", attr->id);
    fprintf(stream, "#    name      : %s\n", attr->name);
    fprintf(stream, "#    score     : %u\n", attr->score);
    fprintf(stream, "#    range     : [%lu, %lu)\n", attr->range.start, attr->range.end);
    fprintf(stream, "#    group     : %p\n", attr->vgroup);
    fprintf(stream, "#    prepare   : %p\n", attr->prepare);

    if (plan->type == UCG_PLAN_TYPE_FIRST_CLASS) {
        fprintf(stream, "#   n_fallback : %lu\n",
                ucg_list_length((ucg_list_link_t*)&plan->fallback));
        ucg_plan_t *plan_fb = NULL;
        ucg_list_for_each(plan_fb, &plan->fallback, fallback) {
            ucg_assert(plan_fb->type == UCG_PLAN_TYPE_FALLBACK);
            ucg_plan_print(plan_fb, stream);
        }
    }

    return;
}

static ucg_status_t ucg_plan_attr_check(const ucg_plan_attr_t *attr)
{
    /* Other fields do not cause errors and are not checked. */
    if (attr->prepare == NULL || attr->range.start >= attr->range.end ||
        attr->domain == NULL || attr->name == NULL) {
        return UCG_ERR_INVALID_PARAM;
    }
    return UCG_OK;
}

static void ucg_plan_add_fallback(ucg_plan_t *plan, ucg_plan_t *plan_fb)
{
    if (plan_fb->type == UCG_PLAN_TYPE_FIRST_CLASS) {
        ucg_plan_t *plan_fb_fb = NULL;
        ucg_plan_t *next_plan_fb_fb = NULL;
        ucg_list_for_each_safe(plan_fb_fb, next_plan_fb_fb, &plan_fb->fallback, fallback) {
            ucg_list_del(&plan_fb_fb->fallback);
            ucg_plan_add_fallback(plan, plan_fb_fb);
        }
        plan_fb->type = UCG_PLAN_TYPE_FALLBACK;
    }

    /* Sorted by score in reverse order */
    ucg_plan_t *exist_plan_fb = NULL;
    ucg_list_for_each(exist_plan_fb, &plan->fallback, fallback) {
        if (plan_fb->attr.score > exist_plan_fb->attr.score) {
            ucg_list_insert_before(&exist_plan_fb->fallback, &plan_fb->fallback);
            return;
        }
    }

    ucg_list_add_tail(&plan->fallback, &plan_fb->fallback);

    return;
}

static ucg_status_t ucg_plan_apply_attr(ucg_plan_t *plan, const ucg_plan_attr_t *attr)
{
    ucg_plan_attr_t *plan_attr = &plan->attr;
    plan_attr->prepare = attr->prepare;
    plan_attr->vgroup = attr->vgroup;
    plan_attr->score = attr->score;
    plan_attr->range = attr->range;
    plan_attr->id = attr->id;
    plan_attr->name = ucg_strdup(attr->name, "ucg plan name");
    if (plan_attr->name == NULL) {
        goto err;
    }

    plan_attr->domain = ucg_strdup(attr->domain, "ucg plan domain");
    if (plan_attr->domain == NULL) {
        goto err_free_name;
    }

    return UCG_OK;

err_free_name:
    if (plan_attr->name != NULL) {
        ucg_free((void*)plan_attr->name);
    }
err:
    return UCG_ERR_NO_MEMORY;
}

static void ucg_plan_free_attr(ucg_plan_t *plan)
{
    ucg_plan_attr_t *plan_attr = &plan->attr;
    ucg_free((void*)plan_attr->domain);
    ucg_free((void*)plan_attr->name);
    return;
}

static ucg_plan_t* ucg_plan_create(const ucg_plan_params_t *params)
{
    ucg_assert(params != NULL);

    ucg_plan_t *plan = ucg_calloc(1, sizeof(ucg_plan_t), "ucg plan");
    if (plan == NULL) {
        return NULL;
    }

    ucg_status_t status = ucg_plan_apply_attr(plan, &params->attr);
    if (status != UCG_OK) {
        goto err_free_plan;
    }

    plan->type = UCG_PLAN_TYPE_FIRST_CLASS;
    ucg_list_head_init(&plan->fallback);

    return plan;

err_free_plan:
    ucg_free(plan);
    return NULL;
}

static void ucg_plan_destroy(ucg_plan_t *plan)
{
    ucg_assert(plan != NULL);

    if (plan->type == UCG_PLAN_TYPE_FIRST_CLASS) {
        ucg_list_link_t *head = &plan->fallback;
        while (!ucg_list_is_empty(head)) {
            ucg_plan_t *plan_fb = ucg_list_extract_head(head, ucg_plan_t, fallback);
            ucg_plan_destroy(plan_fb);
        }
    }
    ucg_plan_free_attr(plan);
    ucg_free(plan);
    return;
}

static ucg_plan_t* ucg_plan_dup(const ucg_plan_t *plan)
{
    ucg_plan_t *new_plan = NULL;
    ucg_assert(plan != NULL);

    new_plan = ucg_calloc(1, sizeof(ucg_plan_t), "ucg plan dup");
    if (new_plan == NULL) {
        return NULL;
    }

    ucg_status_t status = ucg_plan_apply_attr(new_plan, &plan->attr);
    if (status != UCG_OK) {
        ucg_free(new_plan);
        return NULL;
    }

    new_plan->type = plan->type;
    if (new_plan->type == UCG_PLAN_TYPE_FIRST_CLASS) {
        ucg_list_head_init(&new_plan->fallback);
        ucg_plan_t *plan_fb = NULL;
        ucg_list_for_each(plan_fb, &plan->fallback, fallback) {
            ucg_plan_t *new_plan_fb = ucg_plan_dup(plan_fb);
            if (new_plan_fb == NULL) {
                goto err;
            }
            ucg_list_add_tail(&new_plan->fallback, &new_plan_fb->fallback);
        }
    }

    return new_plan;

err:
    ucg_plan_destroy(new_plan);
    return NULL;
}

/** check whether plan1 can be splitted by plan2. */
static int ucg_plan_need_split(ucg_plan_t *plan1,
                               ucg_plan_t *plan2,
                               uint64_t *split_pos)
{
    ucg_plan_range_t *range1 = &plan1->attr.range;
    ucg_plan_range_t *range2 = &plan2->attr.range;
    if (range2->start > range1->start && range2->start < range1->end) {
        *split_pos = range2->start;
        return 1;
    }

    if (range2->end > range1->start && range2->end < range1->end) {
        *split_pos = range2->end;
        return 1;
    }
    return 0;
}

/**
 * Split a plan into two, the range of the origin plan becomes [start, middle)
 * and the range of the new one is [middle, end). If the plan is the first class,
 * its fallback plans will also be splitted.
 */
static ucg_plan_t* ucg_plan_split(ucg_plan_t *plan, uint64_t middle)
{
    ucg_plan_t *new_plan = ucg_plan_dup(plan);
    if (new_plan == NULL) {
        return NULL;
    }

    new_plan->attr.range.start = middle;

    if (new_plan->type == UCG_PLAN_TYPE_FIRST_CLASS) {
        ucg_assert(ucg_list_length(&plan->fallback) == ucg_list_length(&new_plan->fallback));
        ucg_plan_t *plan_fb = NULL;
        ucg_plan_t *new_plan_fb = ucg_list_head(&new_plan->fallback, ucg_plan_t, fallback);
        ucg_list_for_each(plan_fb, &plan->fallback, fallback) {
            new_plan_fb->attr.range.start = middle;
            plan_fb->attr.range.end = middle;
            new_plan_fb = ucg_list_next(&new_plan_fb->fallback, ucg_plan_t, fallback);
        }
    }

    plan->attr.range.end = middle;
    return new_plan;
}

static int ucg_plan_is_compactible(const ucg_plan_t *plan1, const ucg_plan_t *plan2)
{
    ucg_assert(plan1 != NULL && plan1 != NULL);

    const ucg_plan_attr_t *plan1_attr = &plan1->attr;
    const ucg_plan_attr_t *plan2_attr = &plan2->attr;

    if (plan1_attr->vgroup != plan2_attr->vgroup ||
        plan1_attr->prepare != plan2_attr->prepare ||
        plan1_attr->score != plan2_attr->score ||
        plan1->type != plan2->type) {
        return 0;
    }

    if (plan1_attr->range.end != plan2_attr->range.start) {
        /* Range discontinuous */
        return 0;
    }

    if (plan1->type != UCG_PLAN_TYPE_FIRST_CLASS) {
        return 1;
    }

    /* check fallback */
    if (ucg_list_length((ucg_list_link_t*)&plan1->fallback) !=
        ucg_list_length((ucg_list_link_t*)&plan2->fallback)) {
        return 0;
    }

    ucg_plan_t *plan1_fb = NULL;
    ucg_plan_t *plan2_fb = ucg_list_head(&plan2->fallback, ucg_plan_t, fallback);
    ucg_list_for_each(plan1_fb, &plan1->fallback, fallback) {
        if (!ucg_plan_is_compactible(plan1_fb, plan2_fb)) {
            return 0;
        }
        plan2_fb = ucg_list_next(&plan2_fb->fallback, ucg_plan_t, fallback);
    }
    return 1;
}

static void ucg_plan_compact(ucg_plan_t *plan1, const ucg_plan_t *plan2)
{
    plan1->attr.range.end = plan2->attr.range.end;
    if (plan1->type == UCG_PLAN_TYPE_FIRST_CLASS) {
        ucg_assert(plan1->type == plan2->type);
        ucg_assert(ucg_list_length(&plan1->fallback) ==
                   ucg_list_length(&plan2->fallback));

        ucg_plan_t *plan1_fb = NULL;
        ucg_plan_t *plan2_fb = ucg_list_head(&plan2->fallback, ucg_plan_t, fallback);
        ucg_list_for_each(plan1_fb, &plan1->fallback, fallback) {
            plan1_fb->attr.range.end = plan2_fb->attr.range.end;
            plan2_fb = ucg_list_next(&plan2_fb->fallback, ucg_plan_t, fallback);
        }
    }
    return;
}

static void ucg_plans_print_one(const ucg_list_link_t *head, FILE *stream)
{
    ucg_plan_t *plan = NULL;
    ucg_list_for_each(plan, head, list) {
        ucg_plan_print(plan, stream);
    }
    return;
}

static void ucg_plans_cleanup_one(ucg_list_link_t *head)
{
    while (!ucg_list_is_empty(head)) {
        ucg_plan_t *plan = ucg_list_extract_head(head, ucg_plan_t, list);
        ucg_plan_destroy(plan);
    }
    return;
}

static void ucg_plans_compact_one(ucg_list_link_t *head)
{
    ucg_assert(head != NULL);

    ucg_plan_t *plan = NULL;
    ucg_plan_t *next_plan = NULL;
    ucg_list_for_each_safe(plan, next_plan, head, list) {
        if (&next_plan->list == head) {
            break;
        }
        ucg_trace("try compact plans [%lu, %lu) and [%lu, %lu)",
                  plan->attr.range.start, plan->attr.range.end,
                  next_plan->attr.range.start, next_plan->attr.range.end);

        if (ucg_plan_is_compactible(plan, next_plan)) {
            ucg_plan_compact(plan, next_plan);
            ucg_list_del(&next_plan->list);
            ucg_plan_destroy(next_plan);
            next_plan = plan;
        }
    }
    return;
}

/**
 * Make the ranges of plans in the two linked lists to be same or non-overlap.
 */
static ucg_status_t ucg_plans_normalise_one(ucg_list_link_t *head,
                                            ucg_plan_t *plan,
                                            ucg_list_link_t *new_head)
{
    ucg_plan_t *new_plan = plan;
    ucg_list_head_init(new_head);
    ucg_list_add_tail(new_head, &new_plan->list);

    ucg_plan_t *split_plan = NULL;
    ucg_plan_t *exist_plan = NULL;
    ucg_plan_t *next_plan = NULL;
    ucg_list_for_each_safe(exist_plan, next_plan, head, list) {
        ucg_trace("exist plan range [%lu, %lu), new plan range [%lu, %lu)",
                  exist_plan->attr.range.start, exist_plan->attr.range.end,
                  new_plan->attr.range.start, new_plan->attr.range.end);

        if (new_plan->attr.range.end <= exist_plan->attr.range.start) {
            /* new_plan is always the tail of new_head list */
            break;
        }

        uint64_t split_pos = 0;
        if (ucg_plan_need_split(exist_plan, new_plan, &split_pos)) {
            ucg_trace("split exist plan at %lu", split_pos);
            split_plan = ucg_plan_split(exist_plan, split_pos);
            if (split_plan == NULL) {
                goto err;
            }
            ucg_list_insert_after(&exist_plan->list, &split_plan->list);
        }

        if (ucg_plan_need_split(new_plan, exist_plan, &split_pos)) {
            ucg_trace("split new plan at %lu", split_pos);
            split_plan = ucg_plan_split(new_plan, split_pos);
            if (split_plan == NULL) {
                goto err;
            }
            new_plan = split_plan;
            ucg_list_add_tail(new_head, &new_plan->list);
        }
        if (split_pos != 0) {
            next_plan = exist_plan;
        }
    }
    return UCG_OK;

err:
    while (!ucg_list_is_empty(new_head)) {
        new_plan = ucg_list_extract_head(new_head, ucg_plan_t, list);
        if (new_plan != plan) {
            ucg_plan_destroy(new_plan);
        }
        // the input plan will be destroyed out side.
    }
    return UCG_ERR_NO_MEMORY;
}

static ucg_status_t ucg_plans_add_one(ucg_list_link_t *head, ucg_plan_t *plan)
{
    ucg_assert(head != NULL && plan != NULL);

    ucg_status_t status;
    ucg_list_link_t new_plan_head;
    status = ucg_plans_normalise_one(head, plan, &new_plan_head);
    if (status != UCG_OK) {
        ucg_error("Failed to normalise plan");
        return status;
    }

    ucg_plan_t *exist_plan = NULL;
    ucg_plan_t *next_plan = NULL;
    ucg_list_for_each_safe(exist_plan, next_plan, head, list) {
        if (ucg_list_is_empty(&new_plan_head)) {
            break;
        }
        ucg_plan_t *new_plan = ucg_list_head(&new_plan_head, ucg_plan_t, list);
        ucg_plan_attr_t *new_attr = &new_plan->attr;
        ucg_plan_attr_t *exist_attr = &exist_plan->attr;
        uint64_t new_start = new_attr->range.start;
        uint64_t exist_start = exist_attr->range.start;
        if (new_start == exist_start) {
            ucg_assert(new_attr->range.end == exist_attr->range.end);
            ucg_list_del(&new_plan->list);
            if (new_attr->score > exist_attr->score) {
                ucg_list_insert_after(&exist_plan->list, &new_plan->list);
                ucg_list_del(&exist_plan->list);
                ucg_plan_add_fallback(new_plan, exist_plan);
            } else {
                ucg_plan_add_fallback(exist_plan, new_plan);
            }
            continue;
        }

        if (new_start < exist_start) {
            ucg_assert(new_attr->range.end <= exist_attr->range.start);
            ucg_list_del(&new_plan->list);
            ucg_list_insert_before(&exist_plan->list, &new_plan->list);
            next_plan = exist_plan;
            continue;
        }
    }

    if (!ucg_list_is_empty(&new_plan_head)) {
        ucg_list_splice_tail(head, &new_plan_head);
    }

    return UCG_OK;
}

static ucg_status_t ucg_plans_dup_one(ucg_list_link_t *dst_head,
                                      const ucg_list_link_t *src_head)
{
    ucg_plan_t *plan = NULL;
    ucg_list_for_each(plan, src_head, list) {
        ucg_plan_t *new_plan = ucg_plan_dup(plan);
        if (new_plan == NULL) {
            return UCG_ERR_NO_MEMORY;
        }
        ucg_list_add_tail(dst_head, &new_plan->list);
    }
    return UCG_OK;
}

static ucg_status_t ucg_plans_merge_one(ucg_list_link_t *head,
                                        const ucg_list_link_t *new_head)
{
    ucg_assert(head != NULL && new_head != NULL);

    const ucg_plan_t *new_plan = NULL;
    ucg_list_for_each(new_plan, new_head, list) {
        ucg_plan_t *dup_plan = ucg_plan_dup(new_plan);
        if (dup_plan == NULL) {
            return UCG_ERR_NO_MEMORY;
        }

        ucg_status_t status = ucg_plans_add_one(head, dup_plan);
        if (status != UCG_OK) {
            return status;
        }
    }
    ucg_plans_compact_one(head);
    return UCG_OK;
}

ucg_status_t ucg_plans_init(ucg_plans_t **plans)
{
    UCG_CHECK_NULL_INVALID(plans);

    ucg_coll_type_t coll_type;
    ucg_mem_type_t mem_type;
    ucg_plans_t *p = NULL;

    p = ucg_malloc(sizeof(ucg_plans_t), "ucg plans");
    if (p == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    for (coll_type = 0; coll_type < UCG_COLL_TYPE_LAST; ++coll_type) {
        for (mem_type = 0; mem_type < UCG_MEM_TYPE_LAST; ++mem_type) {
            ucg_list_head_init(&p->plans[coll_type][mem_type]);
        }
    }

    *plans = p;
    return UCG_OK;
}

void ucg_plans_cleanup(ucg_plans_t *plans)
{
    UCG_CHECK_NULL_VOID(plans);

    ucg_coll_type_t coll_type;
    ucg_mem_type_t mem_type;
    for (coll_type = 0; coll_type < UCG_COLL_TYPE_LAST; ++coll_type) {
        for (mem_type = 0; mem_type < UCG_MEM_TYPE_LAST; ++mem_type) {
            ucg_plans_cleanup_one(&plans->plans[coll_type][mem_type]);
        }
    }

    ucg_free(plans);
    return;
}

void ucg_plans_print(const ucg_plans_t *plans, FILE *stream)
{
    fprintf(stream, "# Details of all plans\n");

    ucg_coll_type_t coll_type;
    ucg_mem_type_t mem_type;
    for (coll_type = 0; coll_type < UCG_COLL_TYPE_LAST; ++coll_type) {
        for (mem_type = 0; mem_type < UCG_MEM_TYPE_LAST; ++mem_type) {
            ucg_list_link_t *head = (ucg_list_link_t*)&plans->plans[coll_type][mem_type];
            if (ucg_list_is_empty(head)) {
                continue;
            }
            fprintf(stream, "######## [%s-%s]\n",
                    ucg_coll_type_string(coll_type),
                    ucg_mem_type_string(mem_type));

            ucg_plans_print_one(head, stream);
        }
    }

    return;
}

ucg_status_t ucg_plans_add(ucg_plans_t *plans, const ucg_plan_params_t *params)
{
    UCG_CHECK_NULL_INVALID(plans, params);

    if (params->coll_type >= UCG_COLL_TYPE_LAST ||
        params->mem_type >= UCG_MEM_TYPE_LAST ||
        ucg_plan_attr_check(&params->attr) != UCG_OK) {
        return UCG_ERR_INVALID_PARAM;
    }

    if (params->attr.deprecated) {
        /* drop it silently */
        return UCG_OK;
    }

    ucg_plan_t *new_plan = ucg_plan_create(params);
    if (new_plan == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    ucg_list_link_t *head = &plans->plans[params->coll_type][params->mem_type];
    ucg_status_t status = ucg_plans_add_one(head, new_plan);
    if (status != UCG_OK) {
        ucg_plan_destroy(new_plan);
        return status;
    }

    ucg_plans_compact_one(head);
    return UCG_OK;
}

ucg_status_t ucg_plans_merge(ucg_plans_t **dst, const ucg_plans_t *src)
{
    UCG_CHECK_NULL_INVALID(dst, *dst, src);

    if (*dst == src) {
        return UCG_OK;
    }

    /* New plan container are used to simplify rollback. */
    ucg_plans_t *new_plans = NULL;
    ucg_status_t status = ucg_plans_init(&new_plans);
    if (status != UCG_OK) {
        return status;
    }

    ucg_coll_type_t coll_type;
    ucg_mem_type_t mem_type;
    for (coll_type = 0; coll_type < UCG_COLL_TYPE_LAST; ++coll_type) {
        for (mem_type = 0; mem_type < UCG_MEM_TYPE_LAST; ++mem_type) {
            ucg_list_link_t *new_head = &new_plans->plans[coll_type][mem_type];
            const ucg_list_link_t *dst_head = &(*dst)->plans[coll_type][mem_type];
            status = ucg_plans_dup_one(new_head, dst_head);
            if (status != UCG_OK) {
                goto err;
            }

            const ucg_list_link_t *src_head = &src->plans[coll_type][mem_type];
            status = ucg_plans_merge_one(new_head, src_head);
            if (status != UCG_OK) {
                goto err;
            }
        }
    }

    ucg_plans_cleanup(*dst);
    *dst = new_plans;
    return UCG_OK;

err:
    ucg_plans_cleanup(new_plans);
    return status;
}

ucg_status_t ucg_plans_prepare(const ucg_plans_t *plans, const ucg_coll_args_t *args,
                               const uint32_t size, ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(plans, args, op);

    ucg_status_t status;
    uint32_t msg_size = 0;
    status = ucg_request_msg_size(args, size, &msg_size);
    if (status != UCG_OK) {
        return status;
    }

    int8_t found = 0;
    ucg_plan_t *plan = NULL;
    const ucg_list_link_t *head = &plans->plans[args->type][args->info.mem_type];
    ucg_list_for_each(plan, head, list) {
        ucg_plan_range_t *range = &plan->attr.range;
        if (msg_size < range->start) {
            break;
        }

        if (msg_size >= range->start && msg_size < range->end) {
            found = 1;
            break;
        }
    }

    if (!found) {
        return UCG_ERR_NOT_FOUND;
    }

    status = plan->attr.prepare(plan->attr.vgroup, args, op);
    if (status == UCG_OK) {
        ucg_info("select plan '%s' in '%s'", plan->attr.name, plan->attr.domain);
        return UCG_OK;
    }

    ucg_assert(plan->type == UCG_PLAN_TYPE_FIRST_CLASS);
    ucg_plan_t *plan_fb = NULL;
    ucg_list_for_each(plan_fb, &plan->fallback, fallback) {
        status = plan_fb->attr.prepare(plan_fb->attr.vgroup, args, op);
        if (status == UCG_OK) {
            ucg_info("select fallback plan '%s' in '%s', origin plan '%s'",
                     plan_fb->attr.name, plan_fb->attr.domain, plan->attr.name);
            return UCG_OK;
        }
    }

    return UCG_ERR_NOT_FOUND;
}

ucg_status_t ucg_plan_attr_update(ucg_plan_attr_t *attr, const char *update)
{
    if (update == NULL || update[0] == '\0') {
        return UCG_OK;
    }

    const char *attr_str = ucg_plan_attr_update_find(attr, update);
    if (attr_str == NULL) {
        return UCG_OK;
    }

    uint32_t min_group_size = 0;
    uint32_t max_group_size = (uint32_t) - 1;
    ucg_plan_range_t range = attr->range;
    uint32_t score = attr->score;
    while (attr_str[0] != 'I' && attr_str[0] != '\0') {
        int skip = 1;
        if (attr_str[0] == 'S') {
            if (sscanf(attr_str, "S:%u%n", &score, &skip) != 1) {
                return UCG_ERR_INVALID_PARAM;
            }
        } else if (attr_str[0] == 'R') {
            int rc = sscanf(attr_str, "R:%lu-%lu%n", &range.start, &range.end, &skip);
            if (rc != 1 && rc != 2) {
                return UCG_ERR_INVALID_PARAM;
            }
            if (rc == 1) {
                range.end = UCG_PLAN_RANGE_MAX;
            } else if (range.start >= range.end) {
                return UCG_ERR_INVALID_PARAM;
            }
        } else if (attr_str[0] == 'G') {
            int rc = sscanf(attr_str, "G:%u-%u%n", &min_group_size, &max_group_size, &skip);
            if (rc != 1 && rc != 2) {
                return UCG_ERR_INVALID_PARAM;
            }
            if (rc == 2 && min_group_size >= max_group_size) {
                return UCG_ERR_INVALID_PARAM;
            }
        }
        attr_str += skip;
    }

    /* update the attribute */
    uint32_t group_size = attr->vgroup->size;
    if (group_size < min_group_size || group_size >= max_group_size) {
        attr->deprecated = 1;
    }
    attr->range = range;
    attr->score = score;
    return UCG_OK;
}