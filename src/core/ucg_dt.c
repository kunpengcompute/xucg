/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ucg_dt.h"
#include <string.h>

#include "util/ucg_malloc.h"
#include "util/ucg_mpool.h"
#include "util/ucg_cpu.h"
#include "util/ucg_log.h"

#define UCG_DT_PREDEFINED_FLAGS UCG_DT_FLAG_IS_PREDEFINED | UCG_DT_FLAG_IS_CONTIGUOUS
#define UCG_OP_PREDEFINED_FLAGS UCG_OP_FLAG_IS_PREDEFINED | UCG_OP_FLAG_IS_COMMUTATIVE | UCG_OP_FLAG_IS_PERSISTENT

#define UCG_OP_FUNC_MAX(_target, _source) (_target) = (_target) > (_source) ? (_target) : (_source)
#define UCG_OP_FUNC_MIN(_target, _source) (_target) = (_target) < (_source) ? (_target) : (_source)
#define UCG_OP_FUNC_SUM(_target, _source) (_target) += (_source)
#define UCG_OP_FUNC_PROD(_target, _source) (_target) *= (_source)
#define UCG_OP_FUNC(_type, _dt, _func) \
    static inline ucg_status_t ucg_op_func_##_type##_##_dt(void *op, \
                                                           const void *source, \
                                                           void *target, \
                                                           int32_t count, \
                                                           void *dt) \
    { \
        UCG_UNUSED(op, dt); \
        _dt *a = (_dt*)source; \
        _dt *b = (_dt*)target; \
        for (int i = 0; i < count; ++i) { \
            _func(*b, *a); \
            ++a; \
            ++b; \
        } \
        return UCG_OK; \
    }

#define UCG_OP_PREDEFINED_FUNCS(_type, _TYPE) \
    UCG_OP_FUNC(_type, int8_t, UCG_OP_FUNC_##_TYPE) \
    UCG_OP_FUNC(_type, int16_t, UCG_OP_FUNC_##_TYPE) \
    UCG_OP_FUNC(_type, int32_t, UCG_OP_FUNC_##_TYPE) \
    UCG_OP_FUNC(_type, int64_t, UCG_OP_FUNC_##_TYPE) \
    UCG_OP_FUNC(_type, uint8_t, UCG_OP_FUNC_##_TYPE) \
    UCG_OP_FUNC(_type, uint16_t, UCG_OP_FUNC_##_TYPE) \
    UCG_OP_FUNC(_type, uint32_t, UCG_OP_FUNC_##_TYPE) \
    UCG_OP_FUNC(_type, uint64_t, UCG_OP_FUNC_##_TYPE) \
    UCG_OP_FUNC(_type, _Float16, UCG_OP_FUNC_##_TYPE) \
    UCG_OP_FUNC(_type, float, UCG_OP_FUNC_##_TYPE) \
    UCG_OP_FUNC(_type, double, UCG_OP_FUNC_##_TYPE)

#define UCG_OP_PREDEFINED_NAME(_type) ucg_op_predefined_##_type
#define UCG_OP_PREDEFINED(_type, _TYPE) \
    UCG_OP_PREDEFINED_FUNCS(_type, _TYPE) \
    static ucg_op_func_t ucg_op_predefined_funcs_##_type[UCG_DT_TYPE_PREDEFINED_LAST] = { \
        [UCG_DT_TYPE_INT8]      = ucg_op_func_##_type##_int8_t, \
        [UCG_DT_TYPE_INT16]     = ucg_op_func_##_type##_int16_t, \
        [UCG_DT_TYPE_INT32]     = ucg_op_func_##_type##_int32_t, \
        [UCG_DT_TYPE_INT64]     = ucg_op_func_##_type##_int64_t, \
        [UCG_DT_TYPE_UINT8]     = ucg_op_func_##_type##_uint8_t, \
        [UCG_DT_TYPE_UINT16]    = ucg_op_func_##_type##_uint16_t, \
        [UCG_DT_TYPE_UINT32]    = ucg_op_func_##_type##_uint32_t, \
        [UCG_DT_TYPE_UINT64]    = ucg_op_func_##_type##_uint64_t, \
        [UCG_DT_TYPE_FP16]      = ucg_op_func_##_type##__Float16, \
        [UCG_DT_TYPE_FP32]      = ucg_op_func_##_type##_float, \
        [UCG_DT_TYPE_FP64]      = ucg_op_func_##_type##_double, \
    }; \
    static ucg_status_t UCG_OP_PREDEFINED_NAME(_type)(void *op, \
                                                       const void *source, \
                                                       void *target, \
                                                       int32_t count, \
                                                       void *dt) \
    { \
        ucg_assert(((ucg_op_t*)op)->type == UCG_OP_TYPE_##_TYPE); \
        ucg_dt_t *ucg_dt = (ucg_dt_t*)dt; \
        ucg_assert(ucg_dt_is_predefined(ucg_dt)); \
        return ucg_op_predefined_funcs_##_type[ucg_dt->type](op, source, target, count, dt); \
    }

#define UCG_DT_STATE_INIT(_action, _state, _buffer, _dt, _count) \
    do { \
        _state = ucg_mpool_get(&ucg_dt_state_mp); \
        if (_state == NULL) { \
            break; \
        } \
        \
        _state->dt = _dt; \
        _state->count = _count; \
        if (ucg_dt_is_contiguous(_dt)) { \
            _state->contig.buffer = (void*)_buffer; \
            break; \
        } \
        \
        ucg_dt_generic_t *gdt = ucg_derived_of(_dt, ucg_dt_generic_t); \
        void *gstate = gdt->conv._action(_buffer, gdt->user_dt, _count); \
        if (gstate != NULL) { \
            _state->generic.state = gstate; \
            break; \
        } \
        ucg_mpool_put(_state); \
        _state = NULL; \
        break; \
    } while(0)

#define UCG_DT_STATE_CLEANUP(_state) \
    do { \
        const ucg_dt_t *dt = _state->dt; \
        if (!ucg_dt_is_contiguous(dt)) { \
            const ucg_dt_generic_t *gdt = ucg_derived_of(dt, ucg_dt_generic_t); \
            gdt->conv.finish(_state->generic.state); \
        } \
        ucg_mpool_put(_state); \
    } while(0)

#define UCG_DT_STATE_ACTION(_action, _state, _offset, _buf, _length, _status) \
    do { \
        _status = UCG_OK; \
        uint64_t want_len = *_length; \
        if (want_len == 0) { \
            break; \
        } \
        \
        const ucg_dt_t *dt = _state->dt; \
        if (ucg_dt_is_contiguous(dt)) { \
            uint64_t total_len = _state->count * ucg_dt_size(dt); \
            if (_offset >= total_len) { \
                *_length = 0; \
            } else { \
                uint64_t remaining = total_len - _offset; \
                uint64_t max_len = remaining < want_len ? remaining : want_len; \
                ucg_dt_##_action##_contiguous(_state, _offset, _buf, max_len); \
                *_length = max_len; \
            } \
            break; \
        } \
        \
        const ucg_dt_generic_t *gdt = ucg_derived_of(dt, ucg_dt_generic_t); \
        _status = gdt->conv._action(_state->generic.state, _offset, _buf, _length); \
    } while(0)

static ucg_mpool_t ucg_dt_state_mp;
static ucg_dt_t ucg_dt_predefined[UCG_DT_TYPE_PREDEFINED_LAST] = {
    {UCG_DT_TYPE_INT8,   UCG_DT_PREDEFINED_FLAGS, 1, 1, 0, 1},
    {UCG_DT_TYPE_INT16,  UCG_DT_PREDEFINED_FLAGS, 2, 2, 0, 2},
    {UCG_DT_TYPE_INT32,  UCG_DT_PREDEFINED_FLAGS, 4, 4, 0, 4},
    {UCG_DT_TYPE_INT64,  UCG_DT_PREDEFINED_FLAGS, 8, 8, 0, 8},
    {UCG_DT_TYPE_UINT8,  UCG_DT_PREDEFINED_FLAGS, 1, 1, 0, 1},
    {UCG_DT_TYPE_UINT16, UCG_DT_PREDEFINED_FLAGS, 2, 2, 0, 2},
    {UCG_DT_TYPE_UINT32, UCG_DT_PREDEFINED_FLAGS, 4, 4, 0, 4},
    {UCG_DT_TYPE_UINT64, UCG_DT_PREDEFINED_FLAGS, 8, 8, 0, 8},
    {UCG_DT_TYPE_FP16,   UCG_DT_PREDEFINED_FLAGS, 2, 2, 0, 2},
    {UCG_DT_TYPE_FP32,   UCG_DT_PREDEFINED_FLAGS, 4, 4, 0, 4},
    {UCG_DT_TYPE_FP64,   UCG_DT_PREDEFINED_FLAGS, 8, 8, 0, 8},
};

UCG_OP_PREDEFINED(max, MAX);
UCG_OP_PREDEFINED(min, MIN);
UCG_OP_PREDEFINED(sum, SUM);
UCG_OP_PREDEFINED(prod, PROD);
static ucg_op_t ucg_op_predefined[UCG_OP_TYPE_PREDEFINED_LAST] = {
    {UCG_OP_TYPE_MAX,  UCG_OP_PREDEFINED_FLAGS, UCG_OP_PREDEFINED_NAME(max)},
    {UCG_OP_TYPE_MIN,  UCG_OP_PREDEFINED_FLAGS, UCG_OP_PREDEFINED_NAME(min)},
    {UCG_OP_TYPE_SUM,  UCG_OP_PREDEFINED_FLAGS, UCG_OP_PREDEFINED_NAME(sum)},
    {UCG_OP_TYPE_PROD, UCG_OP_PREDEFINED_FLAGS, UCG_OP_PREDEFINED_NAME(prod)},
};

static int ucg_dt_is_predefined_type(ucg_dt_type_t type)
{
    return type >= UCG_DT_TYPE_INT8 && type < UCG_DT_TYPE_PREDEFINED_LAST;
}

static int ucg_op_is_predefined_type(ucg_op_type_t type)
{
    return type >= UCG_OP_TYPE_MAX && type < UCG_OP_TYPE_PREDEFINED_LAST;
}

static void ucg_dt_pack_contiguous(ucg_dt_state_t *state, uint64_t offset,
                                   void *dst, uint64_t len)
{
    ucg_assert(ucg_dt_is_contiguous(state->dt));
    memcpy(dst, state->contig.buffer + offset, len);
    return;
}

static void ucg_dt_unpack_contiguous(ucg_dt_state_t *state, uint64_t offset,
                                     const void *src, uint64_t len)
{
    ucg_assert(ucg_dt_is_contiguous(state->dt));
    memcpy(state->contig.buffer + offset, src, len);
    return;
}

ucg_status_t ucg_dt_global_init()
{
    return UCG_MPOOL_INIT(&ucg_dt_state_mp, 0, sizeof(ucg_dt_state_t), 0,
                          UCG_CACHE_LINE_SIZE, 16, -1, NULL, "dt state mpool");
}

void ucg_dt_global_cleanup()
{
    ucg_mpool_cleanup(&ucg_dt_state_mp, 1);
    return;
}

ucg_status_t ucg_dt_create(const ucg_dt_params_t *params, ucg_dt_h *dt)
{
    UCG_CHECK_NULL_INVALID(params, dt);

    uint64_t field_mask = params->field_mask;
    if (!(field_mask & UCG_DT_PARAMS_FIELD_TYPE)) {
        return UCG_ERR_INVALID_PARAM;
    }

    ucg_dt_type_t type = params->type;
    if (ucg_dt_is_predefined_type(type)) {
        *dt = &ucg_dt_predefined[type];
        return UCG_OK;
    }

    if (type != UCG_DT_TYPE_USER ||
        !(field_mask & UCG_DT_PARAMS_FIELD_USER_DT) ||
        !(field_mask & UCG_DT_PARAMS_FIELD_SIZE) ||
        !(field_mask & UCG_DT_PARAMS_FIELD_EXTENT) ||
        !(field_mask & UCG_DT_PARAMS_FIELD_TRUE_LB) ||
        !(field_mask & UCG_DT_PARAMS_FIELD_TRUE_EXTENT)) {
        return UCG_ERR_INVALID_PARAM;
    }

    if (params->size != params->extent &&
        !(field_mask & UCG_DT_PARAMS_FIELD_CONV)) {
        return UCG_ERR_INVALID_PARAM;
    }

    ucg_dt_generic_t *gdt = ucg_calloc(1, sizeof(ucg_dt_generic_t), "generic dt");
    if (gdt == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    gdt->super.type = UCG_DT_TYPE_USER;
    if (params->size == params->extent) {
        gdt->super.flags |= UCG_DT_FLAG_IS_CONTIGUOUS;
    } else {
        gdt->conv = params->conv;
    }
    gdt->super.size = params->size;
    gdt->super.extent = params->extent;
    gdt->user_dt = params->user_dt;
    gdt->super.true_lb = params->true_lb;
    gdt->super.true_extent = params->true_extent;

    *dt = &gdt->super;
    return UCG_OK;
}

void ucg_dt_destroy(ucg_dt_h dt)
{
    UCG_CHECK_NULL_VOID(dt);

    if (dt->opaque.destroy != NULL) {
        dt->opaque.destroy(dt->opaque.obj);
    }

    if (!ucg_dt_is_predefined(dt)) {
        ucg_free(dt);
    }
    return;
}

static ucg_status_t ucg_dt_memcpy_contiguous(void *dst, int32_t dcount, ucg_dt_t *dst_dt,
                                             const void *src, int32_t scount, ucg_dt_t *src_dt)
{
    ucg_assert(ucg_dt_is_contiguous(dst_dt));
    ucg_assert(ucg_dt_is_contiguous(src_dt));

    uint64_t src_len = (uint64_t)scount * ucg_dt_size(src_dt);
    uint64_t dst_len = (uint64_t)dcount * ucg_dt_size(dst_dt);
    if (src_len <= dst_len) {
        memcpy(dst, src, src_len);
        return UCG_OK;
    }
    memcpy(dst, src, dst_len);
    return UCG_ERR_TRUNCATE;
}

static ucg_status_t ucg_dt_memcpy_pack(void *dst, int32_t dcount, ucg_dt_t *dst_dt,
                                       const void *src, int32_t scount, ucg_dt_t *src_dt)
{
    ucg_assert(ucg_dt_is_contiguous(dst_dt));
    ucg_assert(!ucg_dt_is_contiguous(src_dt));

    ucg_status_t status = UCG_OK;
    ucg_dt_state_t *state = ucg_dt_start_pack(src, src_dt, scount);
    if (state == NULL) {
        return UCG_ERR_NO_RESOURCE;
    }

    uint64_t src_len = (uint64_t)scount * ucg_dt_size(src_dt);
    uint64_t dst_len = (uint64_t)dcount * ucg_dt_size(dst_dt);
    uint64_t offset = 0;
    uint64_t len = 0;
    while (1) {
        len = dst_len - offset;
        status = ucg_dt_pack(state, offset, dst + offset, &len);
        if (status != UCG_OK) {
            goto out_finish_pack;
        }
        if (len == 0) {
            break;
        }
        offset += len;
    }
    status = src_len <= dst_len ? UCG_OK : UCG_ERR_TRUNCATE;

out_finish_pack:
    ucg_dt_finish(state);
    return status;
}

static ucg_status_t ucg_dt_memcpy_unpack(void *dst, int32_t dcount, ucg_dt_t *dst_dt,
                                         const void *src, int32_t scount, ucg_dt_t *src_dt)
{
    ucg_assert(!ucg_dt_is_contiguous(dst_dt));
    ucg_assert(ucg_dt_is_contiguous(src_dt));

    ucg_status_t status;
    ucg_dt_state_t *state = ucg_dt_start_unpack(dst, dst_dt, dcount);
    if (state == NULL) {
        return UCG_ERR_NO_RESOURCE;
    }

    uint64_t src_len = (uint64_t)scount * ucg_dt_size(src_dt);
    uint64_t dst_len = (uint64_t)dcount * ucg_dt_size(dst_dt);
    uint64_t offset = 0;
    uint64_t len = 0;
    while (1) {
        len = src_len - offset;
        status = ucg_dt_unpack(state, offset, src + offset, &len);
        if (status != UCG_OK) {
            goto out_finish_unpack;
        }
        if (len == 0) {
            break;
        }
        offset += len;
    }
    status = src_len <= dst_len ? UCG_OK : UCG_ERR_TRUNCATE;

out_finish_unpack:
    ucg_dt_finish(state);
    return status;
}

static ucg_status_t ucg_dt_memcpy_generic(void *dst, int32_t dcount, ucg_dt_t *dst_dt,
                                          const void *src, int32_t scount, ucg_dt_t *src_dt)
{
    uint64_t buf_len = 16 << 10;
    void *buf = ucg_malloc(buf_len, "copy generic buffer");
    if (buf == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    ucg_status_t status = UCG_ERR_NO_RESOURCE;
    ucg_dt_state_t *pack_state = ucg_dt_start_pack(src, src_dt, scount);
    if (pack_state == NULL) {
        goto out;
    }

    ucg_dt_state_t *unpack_state = ucg_dt_start_unpack(dst, dst_dt, dcount);
    if (unpack_state == NULL) {
        goto out_finish_pack;
    }

    uint64_t max_len;
    uint64_t pack_offset = 0;
    uint64_t unpack_offset = 0;
    while (1) {
        max_len = buf_len;
        status = ucg_dt_pack(pack_state, pack_offset, buf, &max_len);
        if (status != UCG_OK) {
            goto out_finish_unpack;
        }
        if (max_len == 0) {
            break;
        }
        pack_offset += max_len;

        status = ucg_dt_unpack(unpack_state, unpack_offset, buf, &max_len);
        if (status != UCG_OK) {
            goto out_finish_unpack;
        }
        if (max_len == 0) {
            break;
        }
        unpack_offset += max_len;
    }
    uint64_t src_len = (uint64_t)scount * ucg_dt_size(src_dt);
    uint64_t dst_len = (uint64_t)dcount * ucg_dt_size(dst_dt);
    status = src_len <= dst_len ? UCG_OK : UCG_ERR_TRUNCATE;

out_finish_unpack:
    ucg_dt_finish(unpack_state);
out_finish_pack:
    ucg_dt_finish(pack_state);
out:
    return status;
}

ucg_dt_t* ucg_dt_get_predefined(ucg_dt_type_t type)
{
    return &ucg_dt_predefined[type];
}

ucg_status_t ucg_dt_memcpy(void *dst, int32_t dcount, ucg_dt_t *dst_dt,
                           const void *src, int32_t scount, ucg_dt_t *src_dt)
{
    if ((0 == dcount) || (0 == ucg_dt_size(dst_dt))) {
        return ((0 == scount) || (0 == ucg_dt_size(src_dt))) ? UCG_OK : UCG_ERR_TRUNCATE;
    }

    int is_src_contig = ucg_dt_is_contiguous(src_dt);
    int is_dst_contig = ucg_dt_is_contiguous(dst_dt);
    if (is_src_contig && is_dst_contig) {
        return ucg_dt_memcpy_contiguous(dst, dcount, dst_dt, src, scount, src_dt);
    }

    if (!is_src_contig && is_dst_contig) {
        return ucg_dt_memcpy_pack(dst, dcount, dst_dt, src, scount, src_dt);
    }

    if (is_src_contig && !is_dst_contig) {
        return ucg_dt_memcpy_unpack(dst, dcount, dst_dt, src, scount, src_dt);
    }
    /* both non-contiguous datatype */
    return ucg_dt_memcpy_generic(dst, dcount, dst_dt, src, scount, src_dt);
}

ucg_dt_state_t* ucg_dt_start_pack(const void *buffer, const ucg_dt_t *dt, int32_t count)
{
    UCG_CHECK_NULL(NULL, buffer, dt);

    ucg_dt_state_t *state = NULL;
    UCG_DT_STATE_INIT(start_pack, state, buffer, dt, count);
    return state;
}

uint32_t ucg_dt_packed_size(ucg_dt_state_t *state)
{
    return state->count * ucg_dt_size(state->dt);
}

ucg_status_t ucg_dt_pack(ucg_dt_state_t *state, uint64_t offset, void *dst,
                         uint64_t *length)
{
    UCG_CHECK_NULL_INVALID(state, dst, length);

    ucg_status_t status;
    UCG_DT_STATE_ACTION(pack, state, offset, dst, length, status);
    return status;
}

ucg_dt_state_t* ucg_dt_start_unpack(void *buffer, const ucg_dt_t *dt, int32_t count)
{
    UCG_CHECK_NULL(NULL, buffer, dt);

    ucg_dt_state_t *state = NULL;
    UCG_DT_STATE_INIT(start_unpack, state, buffer, dt, count);
    return state;
}

ucg_status_t ucg_dt_unpack(ucg_dt_state_t *state, uint64_t offset,
                           const void *src, uint64_t *length)
{
    UCG_CHECK_NULL_INVALID(state, src, length);

    ucg_status_t status;
    UCG_DT_STATE_ACTION(unpack, state, offset, src, length, status);
    return status;
}

void ucg_dt_finish(ucg_dt_state_t *state)
{
    UCG_DT_STATE_CLEANUP(state);
    return;
}

ucg_status_t ucg_op_create(const ucg_op_params_t *params, ucg_op_h *op)
{
    UCG_CHECK_NULL_INVALID(params, op);

    uint64_t field_mask = params->field_mask;
    if (!(field_mask & UCG_OP_PARAMS_FIELD_TYPE)) {
        return UCG_ERR_INVALID_PARAM;
    }

    ucg_op_type_t type = params->type;
    if (ucg_op_is_predefined_type(type)) {
        *op = &ucg_op_predefined[type];
        return UCG_OK;
    }

    if (type != UCG_OP_TYPE_USER ||
        !(field_mask & UCG_OP_PARAMS_FIELD_USER_OP) ||
        !(field_mask & UCG_OP_PARAMS_FIELD_USER_FUNC) ||
        !(field_mask & UCG_OP_PARAMS_FIELD_COMMUTATIVE)) {
        return UCG_ERR_INVALID_PARAM;
    }

    ucg_op_generic_t *gop = ucg_calloc(1, sizeof(ucg_op_generic_t), "generic op");
    if (gop == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    gop->super.type = UCG_OP_TYPE_USER;
    gop->super.flags = UCG_OP_FLAG_IS_PERSISTENT;
    gop->super.flags |= params->commutative ? UCG_OP_FLAG_IS_COMMUTATIVE : 0;
    gop->super.func = params->user_func;
    gop->user_op = params->user_op;
    *op = &gop->super;
    return UCG_OK;
}

void ucg_op_destroy(ucg_op_h op)
{
    UCG_CHECK_NULL_VOID(op);

    if (!ucg_op_is_predefined(op)) {
        ucg_free(op);
    }
    return;
}

ucg_status_t ucg_op_init(const ucg_op_params_t *params, ucg_op_h op, uint32_t op_size)
{
    UCG_CHECK_NULL_INVALID(params, op);
    if (sizeof(ucg_op_generic_t) > op_size) {
        ucg_error("the op size %u is less than %lu", op_size, sizeof(ucg_op_generic_t));
        return UCG_ERR_INVALID_PARAM;
    }

    uint64_t field_mask = params->field_mask;
    if (!(field_mask & UCG_OP_PARAMS_FIELD_TYPE)) {
        return UCG_ERR_INVALID_PARAM;
    }

    ucg_op_type_t type = params->type;
    if (ucg_op_is_predefined_type(type)) {
        *op = ucg_op_predefined[type];
        op->flags &= ~UCG_OP_FLAG_IS_PERSISTENT;
        return UCG_OK;
    }

    if (type != UCG_OP_TYPE_USER ||
        !(field_mask & UCG_OP_PARAMS_FIELD_USER_OP) ||
        !(field_mask & UCG_OP_PARAMS_FIELD_USER_FUNC) ||
        !(field_mask & UCG_OP_PARAMS_FIELD_COMMUTATIVE)) {
        return UCG_ERR_INVALID_PARAM;
    }

    ucg_op_generic_t *gop = ucg_derived_of(op, ucg_op_generic_t);
    gop->super.type = UCG_OP_TYPE_USER;
    gop->super.flags = params->commutative ? UCG_OP_FLAG_IS_COMMUTATIVE : 0;
    gop->super.func = params->user_func;
    gop->user_op = params->user_op;
    return UCG_OK;
}
