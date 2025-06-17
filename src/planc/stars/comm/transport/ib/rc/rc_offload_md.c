/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "rc_offload_md.h"
#include "offload/machine.h"

static sct_ib_md_ops_t sct_rc_ofd_md_ops;

static inline void sct_rc_set_md_from_uct(sct_rc_ofd_md_t *md, uct_md_h uct_md)
{
    SET_PTR_BY_ADDRESS(md->super.pd, struct ibv_pd, uct_md, SCT_UCT_MD_OFFSET_IBV_PD);
    SET_PTR_BY_ADDRESS(md->super.dev.ibv_context, struct ibv_context, uct_md,
                       SCT_UCT_MD_OFFSET_IBV_DEV + SCT_UCT_IB_DEVICE_OFFSET_IBV_CONTEXT);
    md->super.use_uct_md = 1;
}

static ucs_status_t sct_rc_ofd_md_open_device(struct ibv_device *ibv_device,
                                              sct_rc_ofd_md_t *md,
                                              sct_ib_device_t *dev,
                                              uct_md_h uct_md)
{
    struct hnsdv_context_attr attr = {0};

    if (uct_md != NULL) {
        sct_rc_set_md_from_uct(md, uct_md);
    } else {
        md->super.use_uct_md = 0;
        dev->ibv_context = hnsdv_open_device(ibv_device, &attr);
    }

    if (ucg_unlikely(dev->ibv_context == NULL)) {
        ucg_error("hnsdv_open_device(%s) failed: %m", ibv_get_device_name(ibv_device));
        return UCS_ERR_NO_DEVICE;
    }

    return UCS_OK;
}

ucs_status_t sct_rc_ofd_md_open(uct_md_h uct_md, struct ibv_device *ibv_device,
                                const sct_ib_md_config_t *md_config, sct_ib_md_t **p_md)
{
    ucs_status_t status = sct_stars_load();
    if (ucs_unlikely(status != UCS_OK)) {
        ucg_error("Failed to load stars module.");
        return status;
    }

    const scs_stars_info_t *stars_info = sct_stars_get_info();
    if (ucg_unlikely(!stars_info)) {
        return UCS_ERR_INVALID_PARAM;
    }

    sct_rc_ofd_md_t *md = ucg_calloc(1, sizeof(*md), "sct_rc_ofd_md_t");
    if (ucs_unlikely(md == NULL)) {
        return UCS_ERR_NO_MEMORY;
    }

    sct_ib_device_t *dev = &md->super.dev;
    status = sct_rc_ofd_md_open_device(ibv_device, md, dev, uct_md);
    if (ucs_unlikely(status != UCS_OK)) {
        goto err;
    }

    status = sct_ib_device_query(dev, ibv_device);
    if (ucs_unlikely(status != UCS_OK)) {
        goto err_free_context;
    }

    struct hnsdv_context attrs_out = {0};
    attrs_out.comp_mask |= HNSDV_CONTEXT_MASK_HW_ID;
    int ret = hnsdv_query_device(dev->ibv_context, &attrs_out);
    if (ucs_unlikely(ret != 0)) {
        ucg_error("hnsdv_query_device(%s) returned %d",
                  ibv_get_device_name(dev->ibv_context->device), ret);
        status = UCS_ERR_NO_DEVICE;
        goto err_free_context;
    }

    const scs_machine_offload_t *machine = scs_stars_get_machine_info();

    md->super.config             = md_config->ext;
    md->super.ops                = &sct_rc_ofd_md_ops;
    md->super.memh_struct_size   = sizeof(sct_ib_verbs_mem_t);

    md->super.super.stars_dev_id = attrs_out.hw_id.chip_id * CPU_DIE_NUM_PER_SOCKET +
                                   attrs_out.hw_id.die_id / IO_DIE_NUM_PER_CPU_DIE;
    /* In offload, the subnet arrangement of ifaces[0-8] is {0, 1, 1, 0, 0, 1, 1, 0}*/
    md->dev_attr.pool_id         = attrs_out.hw_id.die_id % 2; /* 2 means location for hns3 and cpu die */

    struct ibv_device *device = dev->ibv_context->device;
    ucg_debug("device info node_type %d, transport_type %d, name %s, dev_name %s,"
              " dev_path %s ibdev_path %s", (int)device->node_type, (int)device->transport_type,
              device->name, device->dev_name, device->dev_path, device->ibdev_path);

    ucg_debug("core id %d core die id %d die id in chip: %d iface chip_id %d, die_id %d pool_id %d"
              " stars_dev_id %d", machine->affinity.core_id, machine->affinity.die_id,
              machine->affinity.inner_die_id, attrs_out.hw_id.chip_id, attrs_out.hw_id.die_id,
              md->dev_attr.pool_id, md->super.super.stars_dev_id);

    if (UCT_IB_HAVE_ODP_IMPLICIT(&dev->dev_attr)) {
        /* Hi1636 hns don't support */
        md->super.dev.flags |= UCT_IB_DEVICE_FLAG_ODP_IMPLICIT;
    }

    status = sct_ib_md_open_common(&md->super, ibv_device, md_config);
    if (ucs_unlikely(status != UCS_OK)) {
        goto err_free_context;
    }

    ucg_status_t ucg_status = ucg_mpool_init(&md->rdma_params_pool, 0, sizeof(rdma_trans_parm_t),
                                             0, UCG_CACHE_LINE_SIZE, UCG_ELEMS_PER_CHUNK,
                                             UINT_MAX, NULL, "rdma_trans_parm_t_pool");
    if (ucg_unlikely(ucg_status != UCG_OK)) {
        status = UCS_ERR_NO_RESOURCE;
        goto err_free_context;
    }

    ucg_status = ucg_mpool_init(&md->event_params_pool, 0, sizeof(event_trans_parm_t),
                                0, UCG_CACHE_LINE_SIZE, UCG_ELEMS_PER_CHUNK,
                                UINT_MAX, NULL, "event_trans_parm_t_pool");
    if (ucg_unlikely(ucg_status != UCG_OK)) {
        status = UCS_ERR_NO_RESOURCE;
        goto err_free_rdma_params;
    }

    ucg_status = ucg_mpool_init(&md->notify_params_pool, 0, sizeof(write_notify_trans_param_t),
                                0, UCG_CACHE_LINE_SIZE, UCG_ELEMS_PER_CHUNK,
                                UINT_MAX, NULL, "event_trans_parm_t_pool");
    if (ucg_unlikely(ucg_status != UCG_OK)) {
        status = UCS_ERR_NO_RESOURCE;
        goto err_free_notify_params;
    }

    md->super.dev.flags = sct_ib_device_spec(&md->super.dev)->flags;
    *p_md = &md->super;
    return UCS_OK;

err_free_notify_params:
    ucg_mpool_cleanup(&md->notify_params_pool, 1);

err_free_rdma_params:
    ucg_mpool_cleanup(&md->rdma_params_pool, 1);

err_free_context:
    if (!md->super.use_uct_md) {
        ibv_close_device(dev->ibv_context);
    }
err:
    ucg_free(md);
    return status;
}

void sct_rc_ofd_md_cleanup(sct_ib_md_t *ibmd)
{
    sct_rc_ofd_md_t *md = ucs_derived_of(ibmd, sct_rc_ofd_md_t);
    ucg_mpool_cleanup(&md->rdma_params_pool, 1);
    ucg_mpool_cleanup(&md->event_params_pool, 1);
    ucg_mpool_cleanup(&md->notify_params_pool, 1);
    sct_stars_unload();
}

static sct_ib_md_ops_t sct_rc_ofd_md_ops = {
    .open                = sct_rc_ofd_md_open,
    .cleanup             = sct_rc_ofd_md_cleanup,
    .reg_key             = sct_ib_verbs_reg_key,
    .dereg_key           = sct_ib_verbs_dereg_key,
};

SCT_IB_MD_OPS(sct_rc_ofd_md_ops, 3);
