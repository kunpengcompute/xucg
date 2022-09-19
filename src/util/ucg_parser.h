/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PARSER_H_
#define UCG_PARSER_H_

#include <stdio.h>
#include "ucg_helper.h"
#include "ucg_list.h"

#include <ucs/sys/preprocessor.h>
#include <ucs/sys/compiler_def.h>
#include <ucs/config/types.h>
#include <ucs/config/parser.h>

#define ucg_config_field_t              ucs_config_field_t
#define ucg_config_names_array_t        ucs_config_names_array_t
#define ucg_config_global_list_entry_t  ucs_config_global_list_entry_t

#define UCG_CONFIG_TYPE_LOG_COMP        UCS_CONFIG_TYPE_LOG_COMP
#define UCG_CONFIG_REGISTER_TABLE       UCS_CONFIG_REGISTER_TABLE
#define UCG_CONFIG_REGISTER_TABLE_ENTRY UCS_CONFIG_REGISTER_TABLE_ENTRY
#define UCG_CONFIG_TYPE_STRING          UCS_CONFIG_TYPE_STRING
#define UCG_CONFIG_TYPE_INT             UCS_CONFIG_TYPE_INT
#define UCG_CONFIG_TYPE_UINT            UCS_CONFIG_TYPE_UINT
#define UCG_CONFIG_TYPE_STRING_ARRAY    UCS_CONFIG_TYPE_STRING_ARRAY
#define UCG_CONFIG_TYPE_ARRAY           UCS_CONFIG_TYPE_ARRAY
#define UCG_CONFIG_TYPE_TABLE           UCS_CONFIG_TYPE_TABLE
#define UCG_CONFIG_TYPE_ULUNITS         UCS_CONFIG_TYPE_ULUNITS
#define UCG_CONFIG_TYPE_ENUM            UCS_CONFIG_TYPE_ENUM
#define UCG_CONFIG_TYPE_MEMUNITS        UCS_CONFIG_TYPE_MEMUNITS
#define UCG_ULUNITS_AUTO                UCS_ULUNITS_AUTO
#define UCG_CONFIG_TYPE_BITMAP          UCS_CONFIG_TYPE_BITMAP
#define UCG_CONFIG_TYPE_MEMUNITS        UCS_CONFIG_TYPE_MEMUNITS
#define UCG_CONFIG_TYPE_BOOL            UCS_CONFIG_TYPE_BOOL
#define UCG_CONFIG_TYPE_TERNARY         UCS_CONFIG_TYPE_TERNARY

static inline ucg_status_t
ucg_config_parser_fill_opts(void *opts, ucg_config_field_t *fields,
                            const char *env_prefix, const char *table_prefix,
                            int ignore_errors)
{
    ucs_status_t status = ucs_config_parser_fill_opts(opts, fields, env_prefix,
                                                      table_prefix, ignore_errors);
    return ucg_status_s2g(status);
}

static inline ucg_status_t
ucg_config_parser_clone_opts(const void *src, void *dst, ucg_config_field_t *fields)
{
    ucs_status_t status = ucs_config_parser_clone_opts(src, dst, fields);
    return ucg_status_s2g(status);
}

static inline void
ucg_config_parser_release_opts(void *opts, ucg_config_field_t *fields)
{
    ucs_config_parser_release_opts(opts, fields);
}

static inline ucg_status_t
ucg_config_parser_set_value(void *opts, ucg_config_field_t *fields,
                            const char *name, const char *value)
{
    ucs_status_t status = ucs_config_parser_set_value(opts, fields, name, value);
    return ucg_status_s2g(status);
}

typedef enum {
    UCG_CONFIG_PRINT_CONFIG = UCG_BIT(0),
    UCG_CONFIG_PRINT_HEADER = UCG_BIT(1),
    UCG_CONFIG_PRINT_DOC    = UCG_BIT(2),
    UCG_CONFIG_PRINT_HIDDEN = UCG_BIT(3)
} ucg_config_print_flags_t;

typedef enum ucg_ternary_auto_value {
    UCG_NO   = UCS_NO,
    UCG_YES  = UCS_YES,
    UCG_TRY  = UCS_TRY,
    UCG_AUTO = UCS_AUTO,
    UCG_TERNARY_LAST
} ucg_ternary_auto_value_t;

/** @brief Transfer the ucg's flags to ucs's flags */
static inline ucs_config_print_flags_t ucg_print_flags_g2s(ucg_config_print_flags_t flags)
{
    int ucs_flags = 0;

    if (flags & UCG_CONFIG_PRINT_CONFIG) {
        ucs_flags |= UCS_CONFIG_PRINT_CONFIG;
    }

    if (flags & UCG_CONFIG_PRINT_HEADER) {
        ucs_flags |= UCS_CONFIG_PRINT_HEADER;
    }

    if (flags & UCG_CONFIG_PRINT_DOC) {
        ucs_flags |= UCS_CONFIG_PRINT_DOC;
    }

    if (flags & UCG_CONFIG_PRINT_HIDDEN) {
        ucs_flags |= UCS_CONFIG_PRINT_HIDDEN;
    }

    return (ucs_config_print_flags_t)ucs_flags;
}

static inline void
ucg_config_parser_print_opts(FILE *stream, const char *title, const void *opts,
                             ucg_config_field_t *fields, const char *table_prefix,
                             const char *prefix, ucg_config_print_flags_t flags)
{
    ucs_config_print_flags_t ucs_flags;

    ucs_flags = ucg_print_flags_g2s(flags);
    ucs_config_parser_print_opts(stream, title, opts, fields, table_prefix,
                                 prefix, ucs_flags);
}

static inline void
ucg_config_parser_print_all_opts(FILE *stream, const char *prefix,
                                 ucg_config_print_flags_t flags,
                                 ucg_list_link_t *config_list)
{
    ucs_config_print_flags_t ucs_flags;

    ucs_flags = ucg_print_flags_g2s(flags);
    ucs_config_parser_print_all_opts(stream, prefix, ucs_flags, config_list);
}

#endif