/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_LOCK_H_
#define UCG_LOCK_H_

#include "ucg/api/ucg.h"
#include "ucg_helper.h"

#include <ucs/type/spinlock.h>
#include <pthread.h>

#define ucg_spinlock_t                  ucs_spinlock_t
#define ucg_spinlock_init(_lock, _flag) ucg_status_s2g(ucs_spinlock_init(_lock, _flag))
#define ucg_spinlock_destroy(_lock)     ucs_spinlock_destroy(_lock)
#define ucg_spin_lock(_lock)            ucs_spin_lock(_lock)
#define ucg_spin_try_lock(_lock)        ucs_spin_try_lock(_lock) /* 1 for lock success, 0 for failed */
#define ucg_spin_unlock(_lock)          ucs_spin_unlock(_lock)

#define ucg_recursive_spinlock_t                    ucs_recursive_spinlock_t
#define ucg_recursive_spinlock_init(_lock, _flag)   ucg_status_s2g(ucs_recursive_spinlock_init(_lock, _flag))
#define ucg_recursive_spinlock_destroy(_lock)       ucs_recursive_spinlock_destroy(_lock)
#define ucg_recursive_spin_is_owner(_lock, _thread) ucs_recursive_spin_is_owner(_lock, _thread)
#define ucg_recursive_spin_lock(_lock)              ucs_recursive_spin_lock(_lock)
#define ucg_recursive_spin_trylock(_lock)           ucs_recursive_spin_trylock(_lock)
#define ucg_recursive_spin_unlock(_lock)            ucs_recursive_spin_unlock(_lock)

typedef enum {
    UCG_LOCK_TYPE_NONE,
    UCG_LOCK_TYPE_SPINLOCK,
    UCG_LOCK_TYPE_MUTEX,
} ucg_lock_type_t;

typedef struct ucg_lock {
    ucg_lock_type_t type;
    union {
        ucg_recursive_spinlock_t spinlock;
        pthread_mutex_t mutex;
    };
} ucg_lock_t;

#ifdef UCG_ENABLE_MT
static inline ucg_status_t ucg_lock_init(ucg_lock_t *lock, ucg_lock_type_t type)
{
    lock->type = type;
    if (type == UCG_LOCK_TYPE_NONE) {
        return UCG_OK;
    }

    if (type == UCG_LOCK_TYPE_SPINLOCK) {
        return ucg_recursive_spinlock_init(&lock->spinlock, 0);
    }

    ucg_assert(type == UCG_LOCK_TYPE_MUTEX);
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    int rc = pthread_mutex_init(&lock->mutex, &attr);
    pthread_mutexattr_destroy(&attr);

    return rc == 0 ? UCG_OK : UCG_ERR_NO_RESOURCE;
}

static inline void ucg_lock_destroy(ucg_lock_t *lock)
{
    if (lock->type == UCG_LOCK_TYPE_NONE) {
        return;
    }

    if (lock->type == UCG_LOCK_TYPE_SPINLOCK) {
        ucg_recursive_spinlock_destroy(&lock->spinlock);
        return;
    }

    ucg_assert(lock->type == UCG_LOCK_TYPE_MUTEX);
    pthread_mutex_destroy(&lock->mutex);
    return;
}

static inline void ucg_lock_enter(ucg_lock_t *lock)
{
    if (lock->type == UCG_LOCK_TYPE_NONE) {
        return;
    }

    if (lock->type == UCG_LOCK_TYPE_SPINLOCK) {
        ucg_recursive_spin_lock(&lock->spinlock);
        return;
    }

    ucg_assert(lock->type == UCG_LOCK_TYPE_MUTEX);
    pthread_mutex_lock(&lock->mutex);
    return;
}

static inline void ucg_lock_leave(ucg_lock_t *lock)
{
    if (lock->type == UCG_LOCK_TYPE_NONE) {
        return;
    }

    if (lock->type == UCG_LOCK_TYPE_SPINLOCK) {
        ucg_recursive_spin_unlock(&lock->spinlock);
        return;
    }

    ucg_assert(lock->type == UCG_LOCK_TYPE_MUTEX);
    pthread_mutex_unlock(&lock->mutex);
    return;
}
#else
#define ucg_lock_init(_lock, _type) ({UCG_UNUSED(_lock, _type); UCG_OK;})
#define ucg_lock_destroy(_lock)     UCG_UNUSED(_lock)
#define ucg_lock_enter(_lock)       UCG_UNUSED(_lock)
#define ucg_lock_leave(_lock)       UCG_UNUSED(_lock)
#endif //UCG_ENABLE_MT

#endif