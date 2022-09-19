/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PROFILE_H_
#define UCG_PROFILE_H_

#include "ucg/api/ucg.h"

#ifdef UCG_ENABLE_PROFILE
#include <ucs/profile/profile_on.h>
#else
#include <ucs/profile/profile_off.h>
#endif

/** @brief record a scope-begin profiling event */
#define UCG_PROFILE_SCOPE_BEGIN()       UCS_PROFILE_SCOPE_BEGIN()
/** @brief record a scope-end profiling event */
#define UCG_PROFILE_SCOPE_END(_name)    UCS_PROFILE_SCOPE_END(_name)

/**
 * @brief declare a profiled scope of code.
 *
 * Usage:
 * UCG_PROFILE_CODE(<name>) {
 *  <code>
 * }
 */
#define UCG_PROFILE_CODE(_name)         UCS_PROFILE_CODE(_name)

/**
 * @brief create a profiled function named @a _name
 *
 * Usage:
 * UCG_PROFILE_FUNC(<ret>, <func_name>, (a, b), int a, int b)
 * {
 *      <function-body>
 * }
 *
 * @param _rettype  the return type of function
 * @param _name     the name of function
 * @param _arglist  the argument list of function (without types)
 * @param ...       the argument declarations (with types and args's name should
 *                  keep same with _arglist)
 */
#define UCG_PROFILE_FUNC(_rettype, _name, _arglist, ...)    UCS_PROFILE_FUNC(_rettype, _name, _arglist, ## __VA_ARGS__)

/**
 * @brief create a profiled void return function named @a _name
 *
 * Usage:
 * UCG_PROFILE_FUNC_VOID(<func_name>, (a, b), int a, int b)
 * {
 *      <function-body>
 * }
 *
 * @param _name     the name of function
 * @param _arglist  the argument list of function (without types)
 * @param ...       the argument declarations (with types and args's name should
 *                  keep same with _arglist)
 */
#define UCG_PROFILE_FUNC_VOID(_name, _arglist, ...)    UCS_PROFILE_FUNC_VOID(_name, _arglist, ## __VA_ARGS__)

/**
 * @brief Profile a function call
 *
 * Usage:
 * UCG_PROFILE_CALL(func_name, a, b);
 *
 * @param _func     the name of function will be profiled
 * @param ...       the function arguments
 */
#define UCG_PROFILE_CALL(_func, ...)        UCS_PROFILE_CALL(_func, ## __VA_ARGS__)
#define UCG_PROFILE_CALL_VOID(_func, ...)   UCS_PROFILE_CALL_VOID(_func, ## __VA_ARGS__)

/**
 * @brief Profile a function call with name string
 *
 * Usage:
 * UCG_PROFILE_NAMED_CALL("name str", func_name, a, b);
 *
 * @param _name     the string of function name
 * @param _func     the name of function will be profiled
 * @param ...       the function arguments
 */
#define UCG_PROFILE_NAMED_CALL(_name, _func, ...)        UCS_PROFILE_NAMED_CALL(_name, _func, ## __VA_ARGS__)
#define UCG_PROFILE_NAMED_CALL_VOID(_name, _func, ...)   UCS_PROFILE_NAMED_CALL_VOID(_name, _func, ## __VA_ARGS__)

#endif