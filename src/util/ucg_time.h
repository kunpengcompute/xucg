/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_TIME_H_
#define UCG_TIME_H_

#include <sys/time.h>

/**
 * @brief return the micro-seconds(us) of now
 */
static inline uint64_t ucg_get_time_us()
{
    static uint64_t factor = 1000000;
    struct timeval tv;
    gettimeofday(&tv, NULL);

    return tv.tv_sec * factor + tv.tv_usec;
}

#endif