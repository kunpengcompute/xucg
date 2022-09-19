/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */
#include "ucg_info.h"

#include "core/ucg_global.h"
#include "util/ucg_parser.h"

#include <stdio.h>
#include <unistd.h>


static void usage()
{
    printf("Usage: ucg_info [options]\n");
    printf("At least one of the following options has to be set:\n");
    printf("  -v              Show version information\n");
    printf("  -t              Show type and structures information\n");
    printf("  -p              Show all plans\n");
    printf("  -c              Show UCG configuration\n");
    return;
}

int main(int argc, char **argv)
{
    uint64_t print_flags = 0;

    int opt;
    while ((opt = getopt(argc, argv, "vtpc")) != -1) {
        switch (opt) {
            case 'v':
                print_flags |= PRINT_VERSION;
                break;
            case 't':
                print_flags |= PRINT_TYPES;
                break;
            case 'p':
                print_flags |= PRINT_PLANS;
                break;
            case 'c':
                print_flags |= PRINT_CONFIG;
                break;
            default:
                usage();
                return -1;
        }
    }

    if (print_flags == 0) {
        usage();
        return -1;
    }

    ACL_INIT();
    ucg_global_params_t params;
    if (ucg_global_init(&params) != UCG_OK) {
        printf("Failed to initialize UCG\n");
        return -1;
    }

    if (print_flags & PRINT_VERSION) {
        printf("# UCG version %s\n", UCG_API_VERSION_STR);
    }

    if (print_flags & PRINT_TYPES) {
        print_types();
    }

    if (print_flags & PRINT_PLANS) {
        print_plans();
    }

    if (print_flags & PRINT_CONFIG) {
        ucg_config_parser_print_all_opts(stdout, UCG_DEFAULT_ENV_PREFIX,
                                         UCG_CONFIG_PRINT_CONFIG | UCG_CONFIG_PRINT_DOC,
                                         &ucg_config_global_list);
    }
    ucg_global_cleanup();
    ACL_FINALIZE();
    return 0;
}