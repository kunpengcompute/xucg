/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_COMPONENT_H_
#define UCG_COMPONENT_H_

#include "ucg/api/ucg.h"

#define UCG_COMPONENT_OBJNAME_MAX_LEN 31

/**
 * @brief Dynamically loadable component
 */
typedef struct {
    const char *name; /**< Set by component implementer. */
    void *handle; /**< Set by component loader. */
} ucg_component_t;

/**
 * @brief Array of components.
 */
typedef struct {
    int32_t num;
    ucg_component_t **components;
} ucg_components_t;

/**
 * @brief Load components.
 *
 * It loads the library matches @b pattern in the @b path, and get the component
 * object pointer from the library. The symbol name of component object should be
 * same as its library name, i.e. the object symbol name of libfoo.so is foo.
 * The object name should not exceed @ref UCG_COMPONENT_OBJNAME_MAX_LEN.
 *
 * @note Pattern should start with "lib" and end with ".so". Common wildcard
 * characters such as '*' and '?' can be used in the middle.
 *
 * @param [in]  path            Library path.
 * @param [in]  pattern         Library name pattern.
 * @param [out] components      Loaded components.
 * @return ucg_status_t
 */
ucg_status_t ucg_components_load(const char *path, const char *pattern,
                                 ucg_components_t *components);

/**
 * @brief Unload components.
 *
 * @param [in] components       Loaded components.
 */
void ucg_components_unload(ucg_components_t *components);

#endif