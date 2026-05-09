/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

#ifndef UCG_SHM_H_
#define UCG_SHM_H_

#include <inttypes.h>
#include <pthread.h>
#include <stddef.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "ucg/api/ucg.h"
#include "ucg_list.h"
#include "ucg_helper.h"

#define UCG_KBYTE       (1ull << 10)
#define UCG_MBYTE       (1ull << 20)
#define UCG_GBYTE       (1ull << 30)
#define UCG_TBYTE       (1ull << 40)
#define UCG_PBYTE       (1ull << 50)

#define SHM_PATH_MAX 256
#define UCG_SHMEM_DS_ID_INVALID -1
#define UCG_SHM_CREATE_FLAGS (O_CREAT | O_EXCL | O_RDWR)
#define UCG_SHM_ATTACH_FLAGS (O_RDWR)
#define UCG_SHM_OPEN_MODE 0600
#define UCG_SHM_MMAP_PROT (PROT_READ | PROT_WRITE)
#define UCG_SHM_MMAP_FLAGS (MAP_SHARED)
#define UCG_POSIX_FILE_FMT "/ucg_shm_posix_%" PRIx64
#define UCG_SEG_FLAG_SHM_OPEN UCS_BIT(62)

typedef struct ucg_shmem_remote_fd {
    size_t seg_size;
    size_t start_addr_disp;
    size_t actual_addr_disp;
    char seg_name[SHM_PATH_MAX];
} ucg_shmem_remote_fd_t;

typedef struct ucg_shmem_segment { 
    uint64_t seg_id;
    size_t seg_size;
    void *seg_base_addr;
    size_t start_addr_disp;
    size_t actual_addr_disp;
    char seg_name[SHM_PATH_MAX];
    ucg_list_link_t list;
} ucg_shmem_segment_t;

ucg_status_t ucg_shmem_reset_ds(ucg_shmem_segment_t *ds_p);

size_t ucg_shmem_sizeof_ds(const ucg_shmem_segment_t *ds_p);

ucg_status_t ucg_shmem_segment_create(ucg_shmem_segment_t *ds_p, size_t size);

ucg_status_t ucg_shmem_segment_attach(ucg_shmem_segment_t *ds_p);

ucg_status_t ucg_shmem_segment_detach(ucg_shmem_segment_t *ds_p);

ucg_status_t ucg_shmem_segment_unlink(ucg_shmem_segment_t *ds_p);

ucg_shmem_segment_t *ucg_shmem_segment_get(ucg_list_link_t *head, ucg_shmem_remote_fd_t *remote_fd);

ucg_status_t ucg_shmem_segment_put(ucg_list_link_t *head, ucg_shmem_segment_t *elem);

ucg_status_t ucg_shmem_segment_del(ucg_list_link_t *head, ucg_shmem_segment_t *elem);

ucg_status_t ucg_shmem_segment_cleanup(ucg_list_link_t *head);

#endif