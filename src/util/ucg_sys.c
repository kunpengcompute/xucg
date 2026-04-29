/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
#include <unistd.h>
#include <errno.h>
#include <ucs/sys/math.h>
#include "ucg_math.h"
#include "ucg_sys.h"
#include "ucg_log.h"
#include "ucg_helper.h"

int ucg_rand()
{
    ucs_rand_seed_init();
    return ucs_rand();
}

static size_t ucg_get_meminfo_entry(const char* pattern)
{
    char buff[256];
    char final_pattern[80];
    int val = 0;
    size_t val_b = -1;
    FILE *f;

    f = fopen("/proc/meminfo", "r");
    if (f != NULL) {
        snprintf(final_pattern, sizeof(final_pattern), "%s, %s", pattern,
                 "%d kB");
        while (fgets(buff, sizeof(buff), f)) {
            if (sscanf(buff, final_pattern, &val) == 1) {
                val_b = val * 1024ULL;
                break;
            }
        }
        fclose(f);
    }
    
    return val_b;
}

size_t ucg_get_huge_page_size()
{
    static size_t huge_page_size = 0;

    /* Cache the huge page size value */
    if (huge_page_size == 0) {
        huge_page_size = ucg_get_meminfo_entry("Hugepagesize");
        if (huge_page_size == -1) {
            ucg_debug("huge pages are not supported on the system");
        } else {
            ucg_trace("detected huge page size: %ld", huge_page_size);
        }
    }
    
    return huge_page_size;
}

static long ucg_sysconf(int name)
{
    long rc;
    errno = 0;

    rc = sysconf(name);
    ucg_assert(errno == 0);

    return rc;
}

size_t ucg_get_page_size()
{
    static long page_size = 0;
    
    if (page_size == 0) {
        page_size = ucg_sysconf(_SC_PAGESIZE);
        if (page_size < 0) {
            page_size = 4096;
            ucg_info("_SC_PAGE_SIZE is undefined, setting default value to %ld", page_size);
        }   
    }
    return page_size;
}