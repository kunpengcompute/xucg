/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

#include "stars.h"

#include "sys/sys.h"
#include "offload/numa.h"
#include "sct_md.h"


static scs_stars_info_t sct_stars_info;
static void *sct_stars_handle = NULL;
static ucs_spinlock_t stars_load_lock;
static int sct_stars_ref = 0;
static scs_machine_offload_t g_machine_offload;
static scs_numa_info_t g_numa_info;
static ucg_mpool_t g_stars_trans_pool;

#define LOAD_FUNCTION(ptr, type, name) \
    do { \
        ptr = (type)dlsym(sct_stars_handle, name); \
        if (ptr == NULL) \
        { \
            ucg_fatal("Failed to load function %s with error %s ", name, dlerror()); \
            status = ucg_status_s2g(UCS_ERR_INVALID_ADDR); \
            goto out; \
        } \
    } while (0)

STARS_DEV_INIT           api_stars_dev_init;
STARS_DEV_DEINIT         api_stars_dev_deinit;
STARS_GET_INFO           api_stars_get_info;
STARS_GET_HANDLE         api_stars_get_handle;
STARS_GET_HANDLE_EX      api_stars_get_handle_ex;
STARS_RELEASE_HANDLE     api_stars_release_handle;
STARS_SEND_TASK          api_stars_send_task;
STARS_WAIT_CQE           api_stars_wait_cqe;
STARS_GET_EVENT_ID       api_stars_get_event_id;
STARS_RELEASE_EVENT_ID   api_stars_release_event_id;
STARS_GET_PASID          api_stars_get_pasid;
STARS_PIN_UMEM           api_stars_pin_umem;
STARS_UNPIN_UMEM         api_stars_unpin_umem;
STARS_SDMA_AUTHORIZE     api_stars_sdma_authorize;
STARS_SEND_TASK_WITH_ID  api_stars_send_task_with_id;
STARS_WAIT_CQE_WITH_ID   api_stars_wait_cqe_with_id;

static void sct_stars_reset(void)
{
    api_stars_dev_init = NULL;
    api_stars_dev_deinit = NULL;
    api_stars_get_info = NULL;
    api_stars_get_handle = NULL;
    api_stars_get_handle_ex = NULL;
    api_stars_release_handle = NULL;
    api_stars_send_task = NULL;
    api_stars_wait_cqe = NULL;
    api_stars_get_event_id = NULL;
    api_stars_release_event_id = NULL;
    api_stars_get_pasid = NULL;
    api_stars_pin_umem = NULL;
    api_stars_unpin_umem = NULL;
    api_stars_sdma_authorize = NULL;
    api_stars_send_task_with_id = NULL;
    api_stars_wait_cqe_with_id = NULL;
}

ucg_status_t sct_stars_init()
{
    ucg_status_t status;

    int ret = api_stars_get_info(&sct_stars_info.driver);
    if (ret != 0) {
        ucg_error("Failed to get stars info, ret %d", ret);
        return ucg_status_s2g(UCS_ERR_NO_DEVICE);
    }

    sct_stars_print_info(&sct_stars_info.driver);

    int dev_idx = 0;
    for (; dev_idx < sct_stars_info.driver.dev_num; ++dev_idx) {
        ret = api_stars_dev_init(dev_idx);
        if (ret != 0) {
            ucg_error("Failed to init stars device %m, ret %d", ret);
            status = UCG_ERR_NO_RESOURCE;
            goto de_init;
        }
    }

    status = scs_machine_offload_init(&g_machine_offload);
    UCG_CHECK_GOTO(status, de_init);

    status = scs_numa_get_information(g_machine_offload.affinity.core_id,
                                      &g_numa_info);
    UCG_CHECK_GOTO(status, de_init);

#ifdef UCG_STARS_DEBUG
    ucg_debug("thread run at core_id %d node_id %d numa_node_num %d", (int)g_machine_offload.affinity.core_id,
              (int)g_numa_info.node_id, (int)g_numa_info.numa_num);

    for (uint8_t idx = 0; idx < g_numa_info.numa_num; ++idx) {
        ucg_debug("distance %d, cpu_list start %d, end %d", (int)g_numa_info.distance[idx],
                  (int)g_numa_info.cpus_list[idx].first, (int)g_numa_info.cpus_list[idx].last);
    }
#endif

    return UCG_OK;

de_init:
    while (dev_idx > 0) {
        api_stars_dev_deinit(dev_idx - 1);
        dev_idx -= 1;
    }

    return status;
}

static void sct_stars_deinit(void)
{
    ucg_mpool_cleanup(&g_stars_trans_pool, 1);
    for (int dev_idx = 0; dev_idx < sct_stars_info.driver.dev_num;
         ++dev_idx) {
        api_stars_dev_deinit(dev_idx);
    }
}

ucg_status_t sct_stars_load()
{
    ucg_status_t status = UCG_OK;

    ucs_spin_lock(&stars_load_lock);
    if (sct_stars_handle != NULL) {
        ++sct_stars_ref;
        goto do_unlock;
    }

    sct_stars_handle = dlopen(STARS_DRIVER_SO, RTLD_NOW);
    if (sct_stars_handle == NULL) {
        ucg_debug("Failed to open library %s with error %s ", STARS_DRIVER_SO, dlerror());
        status = ucg_status_s2g(UCS_ERR_IO_ERROR);
        goto do_unlock;
    }

    sct_stars_reset();

    LOAD_FUNCTION(api_stars_dev_init, STARS_DEV_INIT, "stars_dev_init");
    LOAD_FUNCTION(api_stars_dev_deinit, STARS_DEV_DEINIT, "stars_dev_deinit");
    LOAD_FUNCTION(api_stars_get_info, STARS_GET_INFO, "stars_get_info");
    LOAD_FUNCTION(api_stars_get_handle, STARS_GET_HANDLE, "stars_get_handle");
    LOAD_FUNCTION(api_stars_get_handle_ex, STARS_GET_HANDLE_EX, "stars_get_handle_ex");
    LOAD_FUNCTION(api_stars_release_handle, STARS_RELEASE_HANDLE, "stars_release_handle");
    LOAD_FUNCTION(api_stars_send_task, STARS_SEND_TASK, "stars_send_task");
    LOAD_FUNCTION(api_stars_wait_cqe, STARS_WAIT_CQE, "stars_wait_cqe");
    LOAD_FUNCTION(api_stars_get_event_id, STARS_GET_EVENT_ID, "stars_get_event_id");
    LOAD_FUNCTION(api_stars_release_event_id, STARS_RELEASE_EVENT_ID, "stars_release_event_id");
    LOAD_FUNCTION(api_stars_get_pasid, STARS_GET_PASID, "stars_get_pasid");
    LOAD_FUNCTION(api_stars_pin_umem, STARS_PIN_UMEM, "stars_pin_umem");
    LOAD_FUNCTION(api_stars_unpin_umem, STARS_UNPIN_UMEM, "stars_unpin_umem");
    LOAD_FUNCTION(api_stars_sdma_authorize, STARS_SDMA_AUTHORIZE, "stars_sdma_authorize");
    LOAD_FUNCTION(api_stars_send_task_with_id, STARS_SEND_TASK_WITH_ID, "stars_send_task_with_id");
    LOAD_FUNCTION(api_stars_wait_cqe_with_id, STARS_WAIT_CQE_WITH_ID, "stars_wait_cqe_with_id");

    status = sct_stars_init();
    UCG_ASSERT_CODE_GOTO(status, out);

    status = ucg_mpool_init(&g_stars_trans_pool, 0, sizeof(stars_trans_parm_t),
                            0, UCG_CACHE_LINE_SIZE, UCG_ELEMS_PER_CHUNK,
                            UINT_MAX, NULL, "stars_trans_parm_t_pool");
    UCG_ASSERT_CODE_GOTO(status, out);

    ++sct_stars_ref;
    ucs_spin_unlock(&stars_load_lock);
    ucg_debug("Success to load stars library.");
    return ucg_status_s2g(UCS_OK);

out:
    dlclose(sct_stars_handle);
    sct_stars_handle = NULL;
do_unlock:
    ucs_spin_unlock(&stars_load_lock);
    return status;
}

void sct_stars_unload(void)
{
    ucs_spin_lock(&stars_load_lock);
    if (sct_stars_ref > 0) {
        --sct_stars_ref;
        goto out;
    }

    if (sct_stars_ref == 0 && sct_stars_handle != NULL) {
        ucg_debug("will unload stars library");
        sct_stars_deinit();
        dlclose(sct_stars_handle);
        sct_stars_handle = NULL;
    }
out:
    ucs_spin_unlock(&stars_load_lock);
}

const scs_stars_info_t *sct_stars_get_info(void)
{
    return &sct_stars_info;
}

const scs_machine_offload_t *scs_stars_get_machine_info()
{
    return &g_machine_offload;
}

ucs_status_t scs_stars_alloc_events(uint8_t dev_id, uint16_t count, void *event, uint8_t flag)
{
    event_id_type_t *eventid = (event_id_type_t *)event;
    int ret = api_stars_get_event_id(dev_id, count, eventid, 0);
    if (ucg_unlikely(ret != 0)) {
        ucg_error("Failed to alloc stars event, count %d status: %d", count, ret);
        return UCS_ERR_NO_RESOURCE;
    }

    ucg_debug("will alloc eventid for dev_id %d, count %d, event {id %d, offset %lu}", (int)dev_id, (int)count,
              (int)eventid->event_id, (uint64_t)eventid->offset);

    return UCS_OK;
}

ucs_status_t scs_stars_free_events(uint8_t dev_id, uint16_t count, void *event, uint8_t flag)
{
    event_id_type_t *eventid = (event_id_type_t *)event;
    ucg_debug("will free eventid for dev_id %d, count %d, event {id %d, offset %lu}", (int)dev_id, (int)count,
              (int)eventid->event_id, (uint64_t)eventid->offset);
    int ret = api_stars_release_event_id(dev_id, count, eventid);
    if (ucg_unlikely(ret != 0)) {
        ucg_error("Failed to release stars events, status: %d", ret);
        return UCS_ERR_NO_RESOURCE;
    }

    return UCS_OK;
}

stars_trans_parm_t* scs_stars_get_trans_parm()
{
    return ucg_mpool_get(&g_stars_trans_pool);
}

#ifdef UCG_STARS_DEBUG
void sct_stars_print_trans_parm_info(stars_trans_parm_t *stars_tasks, uint32_t task_count)
{
    for (stars_trans_parm_t *ptr = stars_tasks; ptr != NULL; ptr = ptr->next) {
        if (ptr->parms_len == sizeof(event_trans_parm_t)) {
            event_trans_parm_t *event = ptr->trans_parms;
            ucg_debug("stars_trans_parm { opcode %d, wr_cqe_flag %d, event parms_len %d }",
                      ptr->opcode, ptr->wr_cqe_flag, ptr->parms_len);
            ucg_debug("event params { type: %d, event_id %d, event_addr %lu}",
                      event->type, event->event_id, event->event_addr);
        } else if (ptr->parms_len == sizeof(sdma_trans_parm_t)) {
            sdma_trans_parm_t *sdma = ptr->trans_parms;
            ucg_debug("stars_trans_parm { opcode %d, wr_cqe_flag %d, sdma parms_len %d } "
                      "sdma_trans_parm { length: %d, s_substreamid: %d, d_substreamid: %d,"
                      "s_addr %llu, d_addr %llu, s_stride_len %d, d_stride_len %d, stride_num %d, opcode %d}",
                      ptr->opcode, ptr->wr_cqe_flag, ptr->parms_len,
                      sdma->length, sdma->s_substreamid, sdma->d_substreamid, sdma->s_addr, sdma->d_addr,
                      sdma->s_stride_len, sdma->d_stride_len, sdma->stride_num, sdma->opcode);
        } else if (ptr->parms_len == sizeof(rdma_trans_parm_t)) {
            rdma_trans_parm_t *rdma = ptr->trans_parms;
            ucg_debug("stars_trans_parm { opcode %d, wr_cqe_flag %d, rdma parms_len %d }",
                      ptr->opcode, ptr->wr_cqe_flag, ptr->parms_len);
            ucg_debug("rdma params { task_count %d, qp_num %d, db_value %d, cmd %d, streamid %d, substreamid %d, "
                      "hac_functionId %d, sl %d, rdma_addr %ld, real_sqe_type %d }",
                      rdma->task_count, rdma->qp_num, rdma->db_value, (int)rdma->cmd,
                      (int)rdma->streamid, (int)rdma->substreamid, (int)rdma->hac_functionId,
                      (int)rdma->sl, rdma->rdma_addr, (int)rdma->real_sqe_type);
        } else if (ptr->parms_len == sizeof(write_notify_trans_param_t)) {
            write_notify_trans_param_t *event = ptr->trans_parms;
            ucg_debug("stars_trans_parm { opcode %d, wr_cqe_flag %d, event parms_len %d }, "
                      "event params { dev_id: %d, event_id %d}",
                      ptr->opcode, ptr->wr_cqe_flag, ptr->parms_len,
                      event->devid, event->eventid);
        } else {
            ucg_error("stars_trans_parm { opcode %d, wr_cqe_flag %d, unknown parms_len %d }",
                      ptr->opcode, ptr->wr_cqe_flag, ptr->parms_len);
        }
    }
}

void sct_stars_print_wait_output(stars_wait_output_t *output)
{
    if (ucs_unlikely(output == NULL || output->cqe_output == NULL)) {
        ucg_info("Failed to print as invalid wait output.");
        return;
    }

    stars_cqe_output_t *cqe = output->cqe_output;
    for (uint32_t idx = 0; idx < output->out_pos; ++idx) {
        if (cqe->is_rdma_task) {
            rdma_output_status_t *rdma_cqe = &cqe->rdma_cqe;
            ucg_debug("wait_output { out_pos %d, out_num %d, cqe_output { is_rdma_task %d, "
                      "rdma_cqe { wr_id %lu, byte_len %d, imm_data %d, status %d,"
                      "slid %d, qp_num %d, cmpl_cnt %d } } }",
                      output->out_pos, output->out_num, (int)cqe->is_rdma_task,
                      rdma_cqe->wr_id, rdma_cqe->byte_len, rdma_cqe->imm_data, (int)rdma_cqe->status,
                      (int)rdma_cqe->slid, rdma_cqe->qp_num, rdma_cqe->cmpl_cnt);
        } else {
            ucg_debug("wait_output { out_pos %d, out_num %d, cqe_output { is_rdma_task %d } }",
                      output->out_pos, output->out_num, (int)cqe->is_rdma_task);
        }
        cqe++;
    }
}
#endif
