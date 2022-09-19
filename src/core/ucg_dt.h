/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_DT_H_
#define UCG_DT_H_

#include "ucg/api/ucg.h"
#include "util/ucg_helper.h"

/* NOTE: Only support UCG_MEM_TYPE_HOST. */

typedef enum {
    UCG_DT_FLAG_IS_PREDEFINED  = UCG_BIT(0),
    UCG_DT_FLAG_IS_CONTIGUOUS  = UCG_BIT(1),
} ucg_dt_flag_t;

typedef enum {
    UCG_OP_FLAG_IS_PREDEFINED  = UCG_BIT(0),
    UCG_OP_FLAG_IS_COMMUTATIVE = UCG_BIT(1),
    UCG_OP_FLAG_IS_PERSISTENT  = UCG_BIT(2),
} ucg_op_flag_t;

typedef struct {
    /** opaque object */
    uint64_t obj;
    /** Callback to destroy opaque object */
    void (*destroy)(uint64_t obj);
} ucg_dt_opaque_t;

typedef struct ucg_dt {
    ucg_dt_type_t type;
    ucg_dt_flag_t flags;
    /** Size of data if it's put on a contiguous memory */
    uint32_t size;
    /** Actual memory size required to store this datatype */
    uint32_t extent;
    /** True lower bound of the data without user defined lb and ub */
    int32_t true_lb;
    /** True extent of the data without user defined lb and ub */
    uint32_t true_extent;
    /** Save an opaque object, e.g. PlanC datatype */
    ucg_dt_opaque_t opaque;
} ucg_dt_t;

typedef struct ucg_dt_generic {
    ucg_dt_t super;
    void *user_dt;
    ucg_dt_convertor_t conv;
} ucg_dt_generic_t;

/** Pack or unpack state */
typedef struct {
    const ucg_dt_t *dt;
    int32_t count;
    union {
        struct {
            void *buffer;
        } contig;
        struct {
            void *state;
        } generic;
    };
} ucg_dt_state_t;

typedef struct ucg_op {
    ucg_op_type_t type;
    ucg_op_flag_t flags;
    ucg_op_func_t func;
} ucg_op_t;

typedef struct ucg_op_generic {
    ucg_op_t super;
    void *user_op;
} ucg_op_generic_t;

/**
 * @brief Initialize UCG DT resources
 * @note It should be invoked only once.
 */
ucg_status_t ucg_dt_global_init();

/**
 * @brief Cleanup UCG DT resources
 * @note It should be invoked only once.
 */
void ucg_dt_global_cleanup();

/***************************************************************
 *                      Datatype routines
 ***************************************************************/
static inline uint32_t ucg_dt_size(const ucg_dt_t *dt)
{
    return dt->size;
}

static inline uint32_t ucg_dt_extent(const ucg_dt_t *dt)
{
    return dt->extent;
}

static inline int ucg_dt_is_predefined(const ucg_dt_t *dt)
{
    return !!(dt->flags & UCG_DT_FLAG_IS_PREDEFINED);
}

static inline int ucg_dt_is_contiguous(const ucg_dt_t *dt)
{
    return !!(dt->flags & UCG_DT_FLAG_IS_CONTIGUOUS);
}

static inline ucg_dt_type_t ucg_dt_type(const ucg_dt_t *dt)
{
    return dt->type;
}

static inline uint64_t ucg_dt_opaque_obj(const ucg_dt_t *dt)
{
    return dt->opaque.obj;
}

static inline void ucg_dt_set_opaque(ucg_dt_t *dt, ucg_dt_opaque_t *opaque)
{
    dt->opaque = *opaque;
    return;
}

ucg_dt_t* ucg_dt_get_predefined(ucg_dt_type_t type);

/**
 * @brief Copy src to dst
 *
 * If all data in src is copied to dst, it will return UCG_OK. If the dst buffer
 * is too small to contains all data in src, it will return UCG_ERR_TRUNCATE.
 * If other errors are returned, it means nothing was copied.
 */
ucg_status_t ucg_dt_memcpy(void *dst, int32_t dcount, ucg_dt_t *dst_dt,
                           const void *src, int32_t scount, ucg_dt_t *src_dt);

ucg_dt_state_t* ucg_dt_start_pack(const void *buffer, const ucg_dt_t *dt,
                                  int32_t count);

uint32_t ucg_dt_packed_size(ucg_dt_state_t *state);

ucg_status_t ucg_dt_pack(ucg_dt_state_t *state, uint64_t offset, void *dst,
                         uint64_t *length);

ucg_dt_state_t* ucg_dt_start_unpack(void *buffer, const ucg_dt_t *dt,
                                    int32_t count);

ucg_status_t ucg_dt_unpack(ucg_dt_state_t *state, uint64_t offset,
                           const void *src, uint64_t *length);

void ucg_dt_finish(ucg_dt_state_t *state);

/***************************************************************
 *                      Operation routines
 ***************************************************************/
static inline uint8_t ucg_op_is_predefined(const ucg_op_t *op)
{
    return !!(op->flags & UCG_OP_FLAG_IS_PREDEFINED);
}

static inline uint8_t ucg_op_is_commutative(const ucg_op_t *op)
{
    return !!(op->flags & UCG_OP_FLAG_IS_COMMUTATIVE);
}

static inline uint8_t ucg_op_is_persistent(const ucg_op_t *op)
{
    return !!(op->flags & UCG_OP_FLAG_IS_PERSISTENT);
}

static inline ucg_op_type_t ucg_op_type(const ucg_op_t *op)
{
    return op->type;
}

static inline ucg_status_t ucg_op_reduce(ucg_op_t *op,
                                         const void *source,
                                         void *target,
                                         int32_t count,
                                         ucg_dt_t *dt)
{
    if (source == NULL || target == NULL || count == 0) {
        return UCG_OK;
    }

    if (ucg_op_is_predefined(op)) {
        return op->func(op, source, target, count, dt);
    }

    ucg_op_generic_t *gop = ucg_derived_of(op, ucg_op_generic_t);
    ucg_assert(!ucg_dt_is_predefined(dt));
    ucg_dt_generic_t *gdt = ucg_derived_of(dt, ucg_dt_generic_t);
    return op->func(gop->user_op, source, target, count, gdt->user_dt);
}

static inline void ucg_op_copy(ucg_op_t *dst, ucg_op_t *src)
{
    if (ucg_op_is_predefined(src)) {
        *dst = *src;
    } else {
        ucg_op_generic_t *gdst = ucg_derived_of(dst, ucg_op_generic_t);
        ucg_op_generic_t *gsrc = ucg_derived_of(src, ucg_op_generic_t);
        *gdst = *gsrc;
    }
    return;
}

#endif