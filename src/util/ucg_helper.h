/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_HELPER_H_
#define UCG_HELPER_H_

#include <stddef.h>
#include "ucg/api/ucg.h"
#include <ucs/type/status.h>
#include <ucs/sys/compiler_def.h>

#ifdef UCG_ENABLE_DEBUG
#include <assert.h>
#define ucg_assert(_cond)       assert(_cond)
#else
#define ucg_assert(_cond)
#endif

#define ucg_offsetof(_type, _member)    ucs_offsetof(_type, _member)
#define ucg_derived_of(_ptr, _type)     ucs_derived_of(_ptr, _type)
#define ucg_container_of(_ptr, _type, _member) ucs_container_of(_ptr, _type, _member)

#define ucg_likely(_cond)       ucs_likely(_cond)
#define ucg_unlikely(_cond)     ucs_unlikely(_cond)

/* Convert token to string */
#define UCG_QUOTE(_a) # _a

/* Paste two tokens */
#define _UCG_TOKENPASTE(_a, _b) _a ## _b
#define UCG_TOKENPASTE(_a, _b)  _UCG_TOKENPASTE(_a, _b)

/* Count number of macro arguments */
#define _UCG_NUM_ARGS(_0,_1,_2,_3,_4,_5,_6,_7,_8,_9,_10,_11,_12,_13,_14,N,...) N
#define UCG_NUM_ARGS(...) _UCG_NUM_ARGS(, ## __VA_ARGS__,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0)

/* Expand macro for each argument in the list */
#define UCG_FOREACH_1(_macro, _op, _a, ...) _macro(_a)
#define UCG_FOREACH_2(_macro, _op, _a, ...) _macro(_a) _op() UCG_FOREACH_1(_macro, _op, __VA_ARGS__)
#define UCG_FOREACH_3(_macro, _op, _a, ...) _macro(_a) _op() UCG_FOREACH_2(_macro, _op, __VA_ARGS__)
#define UCG_FOREACH_4(_macro, _op, _a, ...) _macro(_a) _op() UCG_FOREACH_3(_macro, _op, __VA_ARGS__)
#define UCG_FOREACH_5(_macro, _op, _a, ...) _macro(_a) _op() UCG_FOREACH_4(_macro, _op, __VA_ARGS__)
#define UCG_FOREACH_6(_macro, _op, _a, ...) _macro(_a) _op() UCG_FOREACH_5(_macro, _op, __VA_ARGS__)
#define UCG_FOREACH_7(_macro, _op, _a, ...) _macro(_a) _op() UCG_FOREACH_6(_macro, _op, __VA_ARGS__)
#define UCG_FOREACH_8(_macro, _op, _a, ...) _macro(_a) _op() UCG_FOREACH_7(_macro, _op, __VA_ARGS__)
#define UCG_FOREACH_9(_macro, _op, _a, ...) _macro(_a) _op() UCG_FOREACH_8(_macro, _op, __VA_ARGS__)
#define UCG_FOREACH_10(_macro, _op, _a, ...) _macro(_a) _op() UCG_FOREACH_9(_macro, _op, __VA_ARGS__)
#define UCG_FOREACH_11(_macro, _op, _a, ...) _macro(_a) _op() UCG_FOREACH_10(_macro, _op, __VA_ARGS__)
#define UCG_FOREACH_12(_macro, _op, _a, ...) _macro(_a) _op() UCG_FOREACH_11(_macro, _op, __VA_ARGS__)
#define UCG_FOREACH_13(_macro, _op, _a, ...) _macro(_a) _op() UCG_FOREACH_12(_macro, _op, __VA_ARGS__)
#define UCG_FOREACH_14(_macro, _op, _a, ...) _macro(_a) _op() UCG_FOREACH_13(_macro, _op, __VA_ARGS__)
#define UCG_FOREACH(_macro, _op, ...) \
    UCG_TOKENPASTE(UCG_FOREACH_, UCG_NUM_ARGS(__VA_ARGS__)(_macro, _op, __VA_ARGS__))

#define UCG_OR() ||
#define UCG_COMMA() ,
#define UCG_SEMICOLON() ;
#define UCG_IS_NULL(_a) ((_a) == NULL)

#ifdef UCG_ENABLE_CHECK_PARAMS
/* Any parameter is NULL, it will return _rc. */
#define UCG_CHECK_NULL(_rc, ...) \
    do { \
        if (UCG_FOREACH(UCG_IS_NULL, UCG_OR, __VA_ARGS__)) { \
            return _rc; \
        } \
    } while(0)
/* Any parameter is NULL, it will return UCG_ERR_INVALID_PARAM. */
#define UCG_CHECK_NULL_INVALID(...) \
    do { \
        if (UCG_FOREACH(UCG_IS_NULL, UCG_OR, __VA_ARGS__)) { \
            return UCG_ERR_INVALID_PARAM; \
        } \
    } while(0)
/* Any parameter is NULL, it will return void. */
#define UCG_CHECK_NULL_VOID(...) \
    do { \
        if (UCG_FOREACH(UCG_IS_NULL, UCG_OR, __VA_ARGS__)) { \
            return; \
        } \
    } while(0)
#define UCG_CHECK_OUT_RANGE(_rc, _num, _lb, _ub) \
    do { \
        if (_num < _lb || _num >= _ub) { \
            return _rc; \
        } \
    } while(0)
#else
#define UCG_CHECK_NULL(_rc, ...)
#define UCG_CHECK_NULL_INVALID(...)
#define UCG_CHECK_NULL_VOID(...)
#define UCG_CHECK_OUT_RANGE(_rc, _num, _lb, _ub)
#endif //UCG_ENABLE_CHECK_PARAMS

#define UCG_COPY_VALUE(_dst, _src) ({_dst = _src; UCG_OK;})
// When no require field or copy fails, it will goto _err_label
#define UCG_COPY_REQUIRED_FIELD(_field, _copy, _dst, _src, _err_label) \
    if (field_mask & _field) { \
        ucg_status_t status = _copy(_dst, _src); \
        if (status != UCG_OK) { \
            goto _err_label;\
        } \
    } else { \
        goto _err_label; \
    }
// When copy fails, it will goto _err_label
#define UCG_COPY_OPTIONAL_FIELD(_field, _copy, _dst, _src, _default, _err_label) \
    { \
        ucg_status_t status; \
        if (field_mask & _field) { \
            status = _copy(_dst, _src); \
        } else { \
            status = _copy(_dst, _default); \
        } \
        if (status != UCG_OK) { \
            goto _err_label;\
        } \
    }

#define _UCG_UNUSED(_x) (void)(_x)
#define UCG_UNUSED(...) UCG_FOREACH(_UCG_UNUSED, UCG_SEMICOLON, __VA_ARGS__)

#define CASE_S2G(_value) case UCS_ ## _value : { return UCG_ ## _value; }
#define CASE_G2S(_value) case UCG_ ## _value : { return UCS_ ## _value; }

#define UCG_STATIC_INIT     UCS_STATIC_INIT
#define UCG_STATIC_CLEANUP  UCS_STATIC_CLEANUP

#define UCG_CHECK_GOTO(_stmt, _label) \
    do { \
        if (_stmt != UCG_OK) { \
            goto _label; \
        } \
    } while(0)

#define UCG_STATIC_ASSERT(_cond) do{switch(0) case 0: case(_cond):;}while(0)

/**
 * @brief change UCS status to UCG status
 */
static inline ucg_status_t ucg_status_s2g(ucs_status_t ucs_status)
{
    switch (ucs_status) {
        CASE_S2G(OK)
        CASE_S2G(INPROGRESS)
        CASE_S2G(ERR_UNSUPPORTED)
        CASE_S2G(ERR_INVALID_PARAM)
        CASE_S2G(ERR_NO_RESOURCE)
        CASE_S2G(ERR_NO_MEMORY)
        default:
            return UCG_ERR_NOT_FOUND;
    }
}

/**
 * @brief change UCG status to UCS status
 */
static inline ucs_status_t ucg_status_g2s(ucg_status_t ucg_status)
{
    switch (ucg_status) {
        CASE_G2S(OK)
        CASE_G2S(INPROGRESS)
        CASE_G2S(ERR_UNSUPPORTED)
        CASE_G2S(ERR_INVALID_PARAM)
        CASE_G2S(ERR_NO_RESOURCE)
        CASE_G2S(ERR_NO_MEMORY)
        default:
            return UCS_ERR_NO_MESSAGE;
    }
}

static inline void ucg_clear_flags(uint64_t *flags, uint64_t mask)
{
    *flags &= ~mask;
    return;
}

static inline int ucg_test_flags(uint64_t flags, uint64_t mask)
{
    return (flags & mask) == mask;
}

static inline int ucg_test_and_clear_flags(uint64_t *flags, uint64_t mask)
{
    int rc = ucg_test_flags(*flags, mask);
    if (rc) {
        ucg_clear_flags(flags, mask);
    }
    return rc;
}

static inline void* ucg_empty_function_return_null()
{
    return NULL;
}

#endif