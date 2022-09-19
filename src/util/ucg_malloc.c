/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */
#ifdef UCG_ENABLE_DEBUG

#include "ucg_malloc.h"

ucg_malloc_hook_t           ucg_malloc_hook;
ucg_calloc_hook_t           ucg_calloc_hook;
ucg_realloc_hook_t          ucg_realloc_hook;
ucg_posix_memalign_hook_t   ucg_posix_memalign_hook;
ucg_free_hook_t             ucg_free_hook;
ucg_strdup_hook_t           ucg_strdup_hook;

#define UCG_MEM_ALLOC_WRAPPER(_name, _func, ...) \
    do { \
        if (ucg_ ## _func ## _hook != NULL) { \
            return ucg_ ## _func ## _hook(__VA_ARGS__, _name); \
        } \
        return _func(__VA_ARGS__); \
    } while(0)

void *ucg_malloc(size_t size, const char *name)
{
    UCG_MEM_ALLOC_WRAPPER(name, malloc, size);
}

void *ucg_calloc(size_t nmemb, size_t size, const char *name)
{
    UCG_MEM_ALLOC_WRAPPER(name, calloc, nmemb, size);
}

void *ucg_realloc(void *ptr, size_t size, const char *name)
{
    UCG_MEM_ALLOC_WRAPPER(name, realloc, ptr, size);
}

int ucg_posix_memalign(void **memptr, size_t alignment, size_t size, const char *name)
{
    UCG_MEM_ALLOC_WRAPPER(name, posix_memalign, memptr, alignment, size);
}

void ucg_free(void *ptr)
{
    if (ucg_free_hook != NULL) {
        return ucg_free_hook(ptr);
    }
    return free(ptr);
}

char *ucg_strdup(const char *s, const char *name)
{
    UCG_MEM_ALLOC_WRAPPER(name, strdup, s);
}

#endif