/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

#include <unistd.h>
#include <errno.h>
#include <ucs/sys/string.h>
#include "ucg_shmem_segment.h"
#include "ucg_sys.h"
#include "ucg_log.h"
#include "ucg_math.h"
#include "ucg_malloc.h"
#include <sys/time.h>

static ucg_status_t ucg_shmem_segment_test_mem(int fd, size_t length)
{
    const size_t chunk_size = 64 * UCG_KBYTE;
    size_t remaining;
    size_t single_write;
    size_t size_to_write;
    ucg_status_t status;
    int *buff;
    
    buff = ucg_malloc(chunk_size, "write buffer");
    if (buff == NULL) {
        ucg_error("Failed to allocate memory for testing space for backing file.");
        status = UCG_ERR_NO_MEMORY;
        goto out;
    }

    memset(buff, 0, chunk_size);
    if (lseek(fd, 0, SEEK_SET) < 0) {
        ucg_error("lseek failed.");
        status = UCG_ERR_IO_ERROR;
        goto out_free_buf;
    }

    remaining = length;
    while (remaining > 0) {
        size_to_write = ucs_min(remaining, chunk_size);
        single_write = write(fd, buff, size_to_write);

        if (single_write < 0) {
            switch (errno) {
                case ENOSPC:
                    ucg_error("Not enough memory to write total of %zu bytes."
                              "Please check that /dev/shm or the directory you specified has "
                              "more available memory.", length);
                    status = UCG_ERR_NO_MEMORY;
                    break;
                default:
                    ucg_error("Failed to write %zu bytes. %m", size_to_write);
                    status = UCG_ERR_IO_ERROR;
            }
            goto out_free_buf;
        }

        remaining -= single_write;
    }
    status = UCG_OK;

out_free_buf:
    ucg_free(buff);
out:
    return status;
}

ucg_status_t ucg_shmem_reset_ds(ucg_shmem_segment_t *shm_seg_p)
{
    if (shm_seg_p == NULL) {
        return UCG_ERR_INVALID_PARAM;
    }

    shm_seg_p->seg_id = UCG_SHMEM_DS_ID_INVALID;
    shm_seg_p->seg_size = 0;
    shm_seg_p->seg_base_addr = MAP_FAILED;
    memset(shm_seg_p->seg_name, '\0', SHM_PATH_MAX);

    return UCG_OK;
}

size_t ucg_shmem_sizeof_ds(const ucg_shmem_segment_t *shm_seg_p)
{
    size_t ds_size = sizeof(*shm_seg_p);
    return ds_size;
}

ucg_status_t ucg_shmem_segment_create(ucg_shmem_segment_t *shm_seg_p, size_t size)
{
    char real_filename[SHM_PATH_MAX];
    uint64_t shm_id;
    uint64_t seg_id;
    size_t page_size;
    size_t aligned_length;
    void *result;
    ucg_status_t status;

    memset(real_filename, '\0', SHM_PATH_MAX);

    shm_id = ucg_rand();
    ucs_snprintf_safe(real_filename, sizeof(real_filename), UCG_POSIX_FILE_FMT, shm_id);

    seg_id = shm_open(real_filename, UCG_SHM_CREATE_FLAGS, UCG_SHM_OPEN_MODE);
    if (seg_id < 0) {
        ucg_error("(real_filename=%s flags=0x%x) failed:%m", real_filename,
                  UCG_SHM_CREATE_FLAGS);
        status = UCG_ERR_IO_ERROR;
        goto err;
    }

    /* Check if the location of the backing file has enough memory for the
     * needed size by trying to write there before calling mmap */
    status = ucg_shmem_segment_test_mem(seg_id, size);
    if (status != UCG_OK) {
        goto err_close;
    }

    page_size = ucg_get_page_size();
    aligned_length = ucg_align_up_pow2(size, page_size);

    if (ftruncate(seg_id, aligned_length) == -1) {
        status = UCG_ERR_IO_ERROR;
        goto err_close;
    }

    result = mmap(NULL, aligned_length, UCG_SHM_MMAP_PROT, MAP_SHARED, seg_id, 0);
    if (result == MAP_FAILED) {
        ucg_error("mmap failed filename %s seg_id %ld", real_filename, seg_id);
        status = UCG_ERR_NO_MEMORY;
        goto err_close;
    }

    shm_seg_p->seg_id = seg_id;
    shm_seg_p->seg_base_addr = result;
    shm_seg_p->seg_size = aligned_length;
    memcpy(shm_seg_p->seg_name, real_filename, SHM_PATH_MAX);
    return UCG_OK;

err_close:
    close(seg_id);
err:
    return status;
}

ucg_status_t ucg_shmem_segment_attach(ucg_shmem_segment_t *shm_seg_p)
{
    size_t seg_size = shm_seg_p->seg_size;
    char *seg_name = shm_seg_p->seg_name;
    uint64_t seg_id = shm_open(seg_name, UCG_SHM_ATTACH_FLAGS, UCG_SHM_OPEN_MODE);
    if (seg_id < 0) {
        ucg_error("shm_open seg_name %s failed", seg_name);
        return UCG_ERR_IO_ERROR;
    }

    void *seg_base_addr = mmap(NULL, seg_size, UCG_SHM_MMAP_PROT, MAP_SHARED, seg_id, 0);
    if (seg_base_addr == MAP_FAILED) {
        ucg_error("mmap failed seg_id %ld seg_size %ld", seg_id, seg_size);
        close(seg_id);
        return UCG_ERR_IO_ERROR;
    }

    close(seg_id);
    shm_seg_p->seg_id = seg_id;
    shm_seg_p->seg_base_addr = seg_base_addr;

    return UCG_OK;
}

ucg_status_t ucg_shmem_segment_detach(ucg_shmem_segment_t *shm_seg_p)
{
    int ret;
    void *seg_base_addr = shm_seg_p->seg_base_addr;
    size_t seg_size = shm_seg_p->seg_size;
    ucg_status_t status = UCG_OK;

    ret = munmap(seg_base_addr, seg_size);
    if (ret) {
        ucg_error("munmap failed seg_base_addr %p seg_size %ld", seg_base_addr, seg_size);
        status = UCG_ERR_INVALID_PARAM;
    }

    ucg_shmem_reset_ds(shm_seg_p);
    return status;
}

ucg_status_t ucg_shmem_segment_unlink(ucg_shmem_segment_t *shm_seg_p)
{
    int ret;

    ret = munmap(shm_seg_p->seg_base_addr, shm_seg_p->seg_size);
    if (ret) {
        ucg_error("munmap failed seg_base_addr %p seg_size %ld", shm_seg_p->seg_base_addr, shm_seg_p->seg_size);
        return UCG_ERR_INVALID_PARAM;
    }
    close(shm_seg_p->seg_id);

    ret = shm_unlink(shm_seg_p->seg_name);
    if (ret) {
        ucg_error("shm_unlink failed seg_name %s", shm_seg_p->seg_name);
        return UCG_ERR_INVALID_PARAM;
    }

    shm_seg_p->seg_id = UCG_SHMEM_DS_ID_INVALID;
    return UCG_OK;
}

ucg_shmem_segment_t *ucg_shmem_segment_get(ucg_list_link_t *head, ucg_shmem_remote_fd_t *remote_fd)
{
    ucg_shmem_segment_t *iter, *temp;

    ucg_list_for_each_safe(iter, temp, head, list) {
        if (!strcmp(iter->seg_name, remote_fd->seg_name)) {
            iter->actual_addr_disp = remote_fd->actual_addr_disp;
            ucg_list_del(&iter->list);
            return iter;
        }
    }

    ucg_shmem_segment_t *elem = (ucg_shmem_segment_t *)ucg_malloc(sizeof(ucg_shmem_segment_t), "shmem segment list");
    UCG_CHECK_NULL(NULL, elem);
    elem->seg_size = remote_fd->seg_size;
    elem->start_addr_disp = remote_fd->start_addr_disp;
    elem->actual_addr_disp = remote_fd->actual_addr_disp;
    memcpy(elem->seg_name, remote_fd->seg_name, SHM_PATH_MAX);

    ucg_status_t status = ucg_shmem_segment_attach(elem);
    if (status != UCG_OK) {
        free(elem);
        return NULL;
    }
    return elem;
}

ucg_status_t ucg_shmem_segment_put(ucg_list_link_t *head, ucg_shmem_segment_t *elem)
{
    ucg_list_add_tail(head, &elem->list);
    return UCG_OK;
}

ucg_status_t ucg_shmem_segment_del(ucg_list_link_t *head, ucg_shmem_segment_t *elem)
{
    ucg_status_t status;
    ucg_list_del(&elem->list);
    status = ucg_shmem_segment_detach(elem);
    return status;
}

ucg_status_t ucg_shmem_segment_cleanup(ucg_list_link_t *head)
{
    ucg_status_t status;
    ucg_shmem_segment_t *iter, *temp;

    ucg_list_for_each_safe(iter, temp, head, list) {
        status = ucg_shmem_segment_del(head, iter);
        if (status != UCG_OK) {
            return status;
        }
        free(iter);
    }
    return UCG_OK;
}