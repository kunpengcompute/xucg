/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2025. All rights reserved.
 * Description: sdma_offload_iface.c
 * Author:
 * Create: 2021
 * Notes:
 */

#include "sdma_offload_iface.h"
#include "sdma_offload_ep.h"
#include <execinfo.h>

#include "sct_md.h"

#if defined(__GNUC__) || defined(__clang__)
#ifdef __clang__
#define OPTIMIZE_ATTRIBUTE __attribute__((optnone))
#else
#define OPTIMIZE_ATTRIBUTE __attribute__((optimize("O0")))
#endif
#endif

#define UCS_SM_IFACE_ADDR_FLAG_EXT UCS_BIT(63)

static ucs_config_field_t uct_sdma_ofd_iface_config_table[] = {
    {"SDMA_OFD_", "", NULL, ucs_offsetof(sct_sdma_ofd_iface_config_t, super),
     UCS_CONFIG_TYPE_TABLE(sct_iface_config_table)},

    {"BW", "16911MBs", "BW of SDMA",
     ucs_offsetof(sct_sdma_ofd_iface_config_t, bw),
     UCS_CONFIG_TYPE_BW},

    {NULL}
};

typedef struct {
    uint64_t                        id;
} ucs_sm_iface_base_device_addr_t;

typedef struct {
    ucs_sm_iface_base_device_addr_t super;
    ucs_sys_ns_t                    ipc_ns;
} ucs_sm_iface_ext_device_addr_t;

size_t sct_sm_iface_get_device_addr_len()
{
    return ucs_sys_ns_is_default(UCS_SYS_NS_TYPE_IPC) ?
           sizeof(ucs_sm_iface_base_device_addr_t) :
           sizeof(ucs_sm_iface_ext_device_addr_t);
}

/* read boot_id GUID or use machine_guid */
static uint64_t sct_sm_iface_get_system_id()
{
    uint64_t high;
    uint64_t low;
    ucs_status_t status;

    status = ucs_sys_get_boot_id(&high, &low);
    if (status == UCS_OK) {
        return high ^ low;
    }

    return ucs_machine_guid();
}

/* GCC failed to compile it in release mode */
ucs_status_t OPTIMIZE_ATTRIBUTE sct_sm_iface_get_device_address(sct_iface_t *tl_iface,
                                                                uct_device_addr_t *addr)
{
    ucs_sm_iface_ext_device_addr_t *ext_addr = (void*)addr;

    ext_addr->super.id  = sct_sm_iface_get_system_id() & ~UCS_SM_IFACE_ADDR_FLAG_EXT;

    if (!ucs_sys_ns_is_default(UCS_SYS_NS_TYPE_IPC)) {
        ext_addr->super.id |= UCS_SM_IFACE_ADDR_FLAG_EXT;
        ext_addr->ipc_ns    = ucs_sys_get_ns(UCS_SYS_NS_TYPE_IPC);
    }

    return UCS_OK;
}

static ucs_status_t sct_sdma_ofd_iface_query(sct_iface_h tl_iface, sct_iface_attr_t *attr)
{
    sct_sdma_ofd_iface_t *iface = ucs_derived_of(tl_iface, sct_sdma_ofd_iface_t);
    sct_base_iface_query(&iface->super, attr);

    attr->iface_addr_len          = sizeof(sct_sdma_ofd_iface_addr_t);
    attr->device_addr_len         = sct_sm_iface_get_device_addr_len();
    attr->ep_addr_len             = sizeof(int);
    attr->cap.flags               = UCT_IFACE_FLAG_CONNECT_TO_IFACE;
    attr->latency                 = ucs_linear_func_make(80e-9, 0);
    attr->bandwidth.dedicated     = 0;
    attr->bandwidth.shared        = iface->config.bw;
    attr->overhead                = 1e-9;
    attr->priority                = 0;
    return UCS_OK;
}

uint8_t sct_sdma_ofd_iface_get_stars_dev_id(sct_iface_h tl_iface)
{
    sct_sdma_ofd_iface_t *iface = ucs_derived_of(tl_iface, sct_sdma_ofd_iface_t);
    return iface->super.md->stars_dev_id;
}

static ucs_status_t sct_sdma_ofd_iface_get_address(sct_iface_t *tl_iface, uct_iface_addr_t *addr)
{
    const sct_sdma_ofd_iface_t *iface = ucs_derived_of(tl_iface, sct_sdma_ofd_iface_t);
    sct_sdma_ofd_iface_addr_t *iface_addr = (sct_sdma_ofd_iface_addr_t *)addr;

    iface_addr->pasid = iface->sdma_pasid;
    iface_addr->dev_id = iface->sdma_md->super.stars_dev_id;
    ucg_debug("get sdma offload iface address as pasid %u dev_id %d",
              iface_addr->pasid, iface_addr->dev_id);
    return UCS_OK;
}

int sct_sm_iface_is_reachable(const sct_iface_h tl_iface,
                              const uct_device_addr_t *dev_addr,
                              const uct_iface_addr_t *iface_addr)
{
    ucs_sm_iface_ext_device_addr_t *ext_addr = (void*)dev_addr;
    ucs_sm_iface_ext_device_addr_t  my_addr  = {};
    ucs_status_t status;

    status = sct_sm_iface_get_device_address(tl_iface,
                                             (uct_device_addr_t*)&my_addr);
    if (status != UCS_OK) {
        ucg_error("failed to get device address");
        return 0;
    }

    /* do not merge these evaluations into single 'if' due
     * to clags compilation warning */
    /* check if both processes are on same host and
     * both of them are in root (or non-root) pid namespace */
    if (ext_addr->super.id != my_addr.super.id) {
        return 0;
    }

    if (!(ext_addr->super.id & UCS_SM_IFACE_ADDR_FLAG_EXT)) {
        return 1; /* both processes are in root namespace */
    }

    /* ok, we are in non-root PID namespace - return 1 if ID of
     * namespaces are same */
    return ext_addr->ipc_ns == my_addr.ipc_ns;
}

ucs_status_t sct_sdma_ofd_iface_progress_enter(sct_sdma_ofd_iface_t *iface)
{
    ucs_status_t status = UCS_OK;
    ucs_spin_lock(&iface->lock);
    if (iface->wait_flag == 1) {
        status = UCS_ERR_BUSY;
    } else {
        iface->wait_flag = 1;
    }
    ucs_spin_unlock(&iface->lock);
    return status;
}

void sct_sdma_ofd_iface_progress_exit(sct_sdma_ofd_iface_t *iface)
{
    ucs_spin_lock(&iface->lock);
    iface->wait_flag = 0;
    ucs_spin_unlock(&iface->lock);
}

ucs_status_t sct_sdma_ofd_iface_notify_progress(sct_iface_h tl_iface)
{
    sct_sdma_ofd_iface_t *iface = ucs_derived_of(tl_iface, sct_sdma_ofd_iface_t);

    uint32_t waited_cnt = 0;
    if (sct_sdma_ofd_iface_progress_enter(iface) != UCS_OK) {
        return UCS_OK;
    }

    int ret;
    sct_ofd_req_h req = NULL;
    ucs_queue_iter_t iter;
    uint32_t timeout = 0;
    ucs_queue_for_each_safe(req, iter, &iface->req_queue, progress) {
        ucg_debug("will progreess stars { handle %p, task_id %d, cqe_cnt %d }",
                  req->stars.handle, req->stars.task_id, req->stars.cqe_cnt);
        ret = api_stars_wait_cqe_with_id(req->stars.handle, req->stars.task_id,
                                         req->stars.cqe_cnt, &waited_cnt, NULL, 0);
        if (ucg_unlikely(ret != 0 || waited_cnt > req->stars.cqe_cnt)) {
            ucg_error("failed to wait stars cqe, ret %d, waited %d, excepted %d",
                      ret, waited_cnt, req->stars.cqe_cnt);
            return UCS_ERR_IO_ERROR;
        }

        ucg_debug("progreess stars done { task_id %d,  waited_cnt %d, cqe_cnt  %d }",
                  req->stars.task_id, waited_cnt, req->stars.cqe_cnt);
        req->stars.cqe_cnt -= waited_cnt;

        if (req->stars.cqe_cnt == 0) {
            if (req->cb != NULL) {
                req->cb(req->context, UCG_OK);
            }
            sct_ofd_req_clean(req);
            ucs_queue_del_iter(&iface->req_queue, iter);
        }
    }

    sct_sdma_ofd_iface_progress_exit(iface);
    return UCS_OK;
}

ucs_status_t sct_sdma_ofd_iface_submit_request(sct_iface_h tl_iface, sct_ofd_req_h req)
{
    sct_sdma_ofd_iface_t *iface = ucs_derived_of(tl_iface, sct_sdma_ofd_iface_t);

    sct_stars_print_trans_parm_info(req->stars.trans_task.head.next,
                                    req->stars.trans_task.count);

    int ret = api_stars_send_task_with_id(req->stars.handle,
                                          req->stars.trans_task.head.next,
                                          req->stars.trans_task.count,
                                          &req->stars.task_id);
    if (ucg_unlikely(ret != 0)) {
        ucg_error("failded to send stars task, ret %d (%m)", ret);
        return ucg_status_g2s(UCG_ERR_IO_ERROR);
    }

    ucg_debug("submit stars task, handle %p, taskid %d, task_cnt %d", req->stars.handle,
              req->stars.task_id, req->stars.trans_task.count);

    ucs_queue_push(&iface->req_queue, &req->progress);

    return UCS_OK;
}

static ucs_status_t sct_sdma_ofd_iface_create_stars_stream(sct_iface_h tl_iface, void **handle_p, uint16_t stream_depth)
{
    sct_sdma_ofd_iface_t *iface = ucs_derived_of(tl_iface, sct_sdma_ofd_iface_t);
    stars_handle_attrs_t handle_config;
    handle_config.streamDepth = stream_depth;

    void *handle = api_stars_get_handle_ex(iface->sdma_md->super.stars_dev_id, 0, &handle_config);
    if (ucg_unlikely(handle == NULL)) {
        ucg_error("Failed to create stars handle %m");
        return UCS_ERR_NO_RESOURCE;
    }

    *handle_p = handle;
    return UCS_OK;
}

static UCS_CLASS_DECLARE_DELETE_FUNC(sct_sdma_ofd_iface_t, sct_iface_t);

static sct_iface_ops_t uct_sdma_ofd_iface_ops = {
    .ep_put_with_notify       = sct_sdma_ofd_ep_put_with_notify,
    .ep_wait_notify           = sct_sdma_ofd_ep_wait_notify,
    .ep_barrier               = sct_sdma_ofd_ep_barrier,
    .ep_alloc_event           = sct_sdma_ofd_ep_alloc_event,
    .ep_create                = UCS_CLASS_NEW_FUNC_NAME(sct_sdma_ofd_ep_t),
    .ep_destroy               = UCS_CLASS_DELETE_FUNC_NAME(sct_sdma_ofd_ep_t),
    .iface_create_stars_stream = sct_sdma_ofd_iface_create_stars_stream,
    .iface_delete_stars_stream = sct_iface_free_stars_stream,
    .iface_submit_req         = sct_sdma_ofd_iface_submit_request,
    .iface_notify_progress    = sct_sdma_ofd_iface_notify_progress,
    .iface_close              = UCS_CLASS_DELETE_FUNC_NAME(sct_sdma_ofd_iface_t),
    .iface_query              = sct_sdma_ofd_iface_query,
    .iface_get_device_address = sct_sm_iface_get_device_address,
    .iface_get_address        = sct_sdma_ofd_iface_get_address,
    .iface_is_reachable       = sct_sm_iface_is_reachable,
    .iface_get_stars_dev_id   = sct_sdma_ofd_iface_get_stars_dev_id
};

static ucs_status_t sct_sdma_ofd_query_tl_devices(sct_md_h md,
                                                  sct_tl_device_resource_t **tl_devices_p,
                                                  unsigned *num_tl_devices_p)
{
    return sct_single_device_resource(md, "memory", UCT_DEVICE_TYPE_SELF,
                                      UCS_SYS_DEVICE_ID_UNKNOWN,
                                      tl_devices_p, num_tl_devices_p);
}

static
UCS_CLASS_INIT_FUNC(sct_sdma_ofd_iface_t, sct_md_h md, sct_worker_h worker,
                    const sct_iface_params_t *params,
                    const sct_iface_config_t *tl_config)
{
    UCS_CLASS_CALL_SUPER_INIT(sct_base_iface_t, &uct_sdma_ofd_iface_ops, md, worker, params,
        tl_config);
    sct_sdma_ofd_iface_config_t *config = ucs_derived_of(tl_config, sct_sdma_ofd_iface_config_t);
    sct_sdma_ofd_md_t *sdma_md = (sct_sdma_ofd_md_t *)md;
    int ret = api_stars_get_pasid(sdma_md->super.stars_dev_id, &self->sdma_pasid);
    if (ucg_unlikely(ret != 0)) {
        ucg_error("iface %p failed to get stars pasid, ret %d", self, ret);
        return UCS_ERR_NO_RESOURCE;
    }

    /* set the subnet_id of sdma to maximum so it can always be the last one 
       when select ifaces in scp_worker_select_ifaces. */ 
    md->subnet_id = UINT64_MAX;
    self->sdma_md = sdma_md;
    self->config.bw = config->bw;
    ucg_debug("iface create pasid %d in dev %d, sdma bandwidth = %f MB/s",
        self->sdma_pasid, sdma_md->super.stars_dev_id, config->bw / (1024 * 1024));

    ucs_spinlock_init(&self->lock, 0);
    ucs_queue_head_init(&self->req_queue);
    return UCS_OK;
}

static UCS_CLASS_CLEANUP_FUNC(sct_sdma_ofd_iface_t)
{
    ucs_spinlock_destroy(&self->lock);
}

UCS_CLASS_DEFINE(sct_sdma_ofd_iface_t, sct_base_iface_t);

static UCS_CLASS_DEFINE_NEW_FUNC(sct_sdma_ofd_iface_t, sct_iface_t, sct_md_h,
                                 sct_worker_h, const sct_iface_params_t*,
                                 const sct_iface_config_t *);

static UCS_CLASS_DEFINE_DELETE_FUNC(sct_sdma_ofd_iface_t, sct_iface_t);

SCT_TL_DEFINE(&sct_sdma_ofd_cmpt, sdma_acc, sct_sdma_ofd_query_tl_devices,
              sct_sdma_ofd_iface_t, "SDMA_", uct_sdma_ofd_iface_config_table,
              sct_sdma_ofd_iface_config_t);
