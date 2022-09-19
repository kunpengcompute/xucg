/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_MALLOC_H_
#define UCG_MALLOC_H_

#include <stdlib.h>
#include <string.h>

#ifdef UCG_ENABLE_DEBUG
typedef void *(*ucg_malloc_hook_t)(size_t s, const char *name);
typedef void *(*ucg_calloc_hook_t)(size_t nmemb, size_t size, const char *name);
typedef void *(*ucg_realloc_hook_t)(void *ptr, size_t size, const char *name);
typedef int   (*ucg_posix_memalign_hook_t)(void **memptr, size_t alignment, size_t size, const char *name);
typedef void  (*ucg_free_hook_t)(void *ptr);
typedef char *(*ucg_strdup_hook_t)(const char *s, const char *name);
/* ucg malloc hook */
extern ucg_malloc_hook_t           ucg_malloc_hook;
extern ucg_calloc_hook_t           ucg_calloc_hook;
extern ucg_realloc_hook_t          ucg_realloc_hook;
extern ucg_posix_memalign_hook_t   ucg_posix_memalign_hook;
extern ucg_free_hook_t             ucg_free_hook;
extern ucg_strdup_hook_t           ucg_strdup_hook;
/* ucg malloc wrapper */
void *ucg_malloc(size_t size, const char *name);
void *ucg_calloc(size_t nmemb, size_t size, const char *name);
void *ucg_realloc(void *ptr, size_t size, const char *name);
int   ucg_posix_memalign(void **memptr, size_t alignment, size_t size, const char *name);
void  ucg_free(void *ptr);
char *ucg_strdup(const char *s, const char *name);
#else
#define ucg_malloc(_s, ...)         malloc(_s)
#define ucg_calloc(_n, _s, ...)     calloc(_n, _s)
#define ucg_realloc(_p, _s, ...)    realloc(_p, _s)
#define ucg_posix_memalign(_ptr, _align, _size, ...) posix_memalign(_ptr, _align, _size)
#define ucg_free(_p, ...)           free(_p)
#define ucg_strdup(_s, ...)         strdup(_s)
#endif

#endif