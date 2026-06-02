/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "ib_iface.h"

#include "sct_md.h"


static UCS_CONFIG_DEFINE_ARRAY(path_bits_spec,
                               sizeof(ucs_range_spec_t),
                               UCS_CONFIG_TYPE_RANGE_SPEC);

const char *sct_ib_mtu_values[] = {
    [UCT_IB_MTU_DEFAULT]    = "default",
    [UCT_IB_MTU_512]        = "512",
    [UCT_IB_MTU_1024]       = "1024",
    [UCT_IB_MTU_2048]       = "2048",
    [UCT_IB_MTU_4096]       = "4096",
    [UCT_IB_MTU_LAST]       = NULL
};

enum {
    UCT_IB_ADDRESS_TYPE_LINK_LOCAL,
    UCT_IB_ADDRESS_TYPE_SITE_LOCAL,
    UCT_IB_ADDRESS_TYPE_GLOBAL,
    UCT_IB_ADDRESS_TYPE_ETH,
    UCT_IB_ADDRESS_TYPE_LAST,
    UCT_IB_IFACE_ADDRESS_TYPE_AUTO  = UCT_IB_ADDRESS_TYPE_LAST,
    UCT_IB_IFACE_ADDRESS_TYPE_LAST
};

static const char *uct_ib_iface_addr_types[] = {
    [UCT_IB_ADDRESS_TYPE_LINK_LOCAL] = "ib_local",
    [UCT_IB_ADDRESS_TYPE_SITE_LOCAL] = "ib_site_local",
    [UCT_IB_ADDRESS_TYPE_GLOBAL]     = "ib_global",
    [UCT_IB_ADDRESS_TYPE_ETH]        = "eth",
    [UCT_IB_IFACE_ADDRESS_TYPE_AUTO] = "auto",
    [UCT_IB_IFACE_ADDRESS_TYPE_LAST] = NULL
};

ucs_config_field_t sct_ib_iface_config_table[] = {
    {"", "", NULL,
     ucs_offsetof(sct_ib_iface_config_t, super), UCS_CONFIG_TYPE_TABLE(sct_iface_config_table)},

    {"SEG_SIZE", "8192",
     "Size of bounce buffers used for post_send and post_recv.",
     ucs_offsetof(sct_ib_iface_config_t, seg_size), UCS_CONFIG_TYPE_MEMUNITS},

    {"TX_QUEUE_LEN", "1024",
     "Length of send queue in the QP.",
     ucs_offsetof(sct_ib_iface_config_t, tx.queue_len), UCS_CONFIG_TYPE_UINT},

    {"TX_MAX_BATCH", "16",
     "Number of send WQEs to batch in one post-send list. Larger values reduce\n"
     "the CPU usage, but increase the latency and pipelining between sender and\n"
     "receiver.",
     ucs_offsetof(sct_ib_iface_config_t, tx.max_batch), UCS_CONFIG_TYPE_UINT},

    {"TX_MAX_POLL", "16",
     "Max number of receive completions to pick during TX poll",
     ucs_offsetof(sct_ib_iface_config_t, tx.max_poll), UCS_CONFIG_TYPE_UINT},

    {"TX_MIN_INLINE", "64",
     "Bytes to reserve in send WQE for inline data. Messages which are small\n"
     "enough will be sent inline.",
     ucs_offsetof(sct_ib_iface_config_t, tx.min_inline), UCS_CONFIG_TYPE_MEMUNITS},

    {"TX_INLINE_RESP", "0",
     "Bytes to reserve in send WQE for inline response. Responses which are small\n"
     "enough, such as of atomic operations and small reads, will be received inline.",
     ucs_offsetof(sct_ib_iface_config_t, inl[UCT_IB_DIR_TX]), UCS_CONFIG_TYPE_MEMUNITS},

    SCT_IFACE_MPOOL_CONFIG_FIELDS("TX_", -1, 1024, "send",
                                    ucs_offsetof(sct_ib_iface_config_t, tx.mp),
        "\nAttention: Setting this param with value != -1 is a dangerous thing\n"
        "in RC/DC and could cause deadlock or performance degradation."),

    {"ADDR_TYPE", "auto",
     "Set the interface address type. \"auto\" mode detects the type according to\n"
     "link layer type and IB subnet prefix.\n"
     "Deprecated. To force use of global routing use IS_GLOBAL.",
     ucs_offsetof(sct_ib_iface_config_t, addr_type),
     UCS_CONFIG_TYPE_ENUM(uct_ib_iface_addr_types)},

    {"IS_GLOBAL", "n",
     "Force interface to use global routing.",
     ucs_offsetof(sct_ib_iface_config_t, is_global), UCS_CONFIG_TYPE_BOOL},

    {"HOP_LIMIT", "255",
     "IB Hop limit / RoCEv2 Time to Live. Should be between 0 and 255.\n",
     ucs_offsetof(sct_ib_iface_config_t, hop_limit), UCS_CONFIG_TYPE_UINT},

    {"NUM_PATHS", "auto",
     "Number of connections that should be created between a pair of communicating\n"
     "endpoints for optimal performance. The default value 'auto' behaves according\n"
     "to the port link layer:\n"
     " RoCE       - "UCS_PP_MAKE_STRING(SCT_IB_DEV_MAX_PORTS) " for LAG port, otherwise - 1.\n"
     " InfiniBand - As the number of path bits enabled by fabric's LMC value and selected\n"
     "              by "UCS_DEFAULT_ENV_PREFIX SCT_IB_CONFIG_PREFIX"LID_PATH_BITS configuration.",
     ucs_offsetof(sct_ib_iface_config_t, num_paths), UCS_CONFIG_TYPE_ULUNITS},

    {"ROCE_LOCAL_SUBNET", "n",
     "Use the local IP address and subnet mask of each network device to route RoCEv2 packets.\n"
     "If set to 'y', only addresses within the interface's subnet will be assumed as reachable.\n"
     "If set to 'n', every remote RoCEv2 IP address is assumed to be reachable from any port.",
     ucs_offsetof(sct_ib_iface_config_t, rocev2_local_subnet), UCS_CONFIG_TYPE_BOOL},

    {"ROCE_SUBNET_PREFIX_LEN", "auto",
     "Length, in bits, of the subnet prefix to be used for reachability check\n"
     "when UCX_IB_ROCE_LOCAL_SUBNET is enabled.\n"
     " - auto  - Detect the subnet prefix length automatically from device address\n"
     " - inf   - Allow connections only within the same machine and same device\n"
     " - <num> - Specify a numeric bit-length value for the subnet prefix",
     ucs_offsetof(sct_ib_iface_config_t, rocev2_subnet_pfx_len), UCS_CONFIG_TYPE_ULUNITS},

    {"ROCE_PATH_FACTOR", "1",
     "Multiplier for RoCE LAG UDP source port calculation. The UDP source port\n"
     "is typically used by switches and network adapters to select a different\n"
     "path for the same pair of endpoints.",
     ucs_offsetof(sct_ib_iface_config_t, roce_path_factor), UCS_CONFIG_TYPE_UINT},

    {"LID_PATH_BITS", "0",
     "List of IB Path bits separated by comma (a,b,c) "
     "which will be the low portion of the LID, according to the LMC in the fabric.",
     ucs_offsetof(sct_ib_iface_config_t, lid_path_bits), UCS_CONFIG_TYPE_ARRAY(path_bits_spec)},

    {"PKEY", "auto",
     "Which pkey value to use. Should be between 0 and 0x7fff.\n"
     "\"auto\" option selects a first valid pkey value with full membership.",
     ucs_offsetof(sct_ib_iface_config_t, pkey), UCS_CONFIG_TYPE_HEX},

    {"PATH_MTU", "default",
     "Path MTU. \"default\" will select the best MTU for the device.",
     ucs_offsetof(sct_ib_iface_config_t, path_mtu),
                    UCS_CONFIG_TYPE_ENUM(sct_ib_mtu_values)},

    {NULL}
};

int sct_ib_iface_is_roce(sct_ib_iface_t *iface)
{
    return sct_ib_device_is_port_roce(sct_ib_iface_device(iface),
                                      iface->config.port_num);
}

static inline sct_ib_roce_version_t sct_ib_address_flags_get_roce_version(uint8_t flags)
{
    ucg_assert(flags & UCT_IB_ADDRESS_FLAG_LINK_LAYER_ETH);
    return (sct_ib_roce_version_t)(flags >> ucs_ilog2(UCT_IB_ADDRESS_FLAG_ETH_LAST));
}

static inline sa_family_t sct_ib_address_flags_get_roce_af(uint8_t flags)
{
    ucg_assert(flags & UCT_IB_ADDRESS_FLAG_LINK_LAYER_ETH);
    return (flags & UCT_IB_ADDRESS_FLAG_ROCE_IPV6) ?
           AF_INET6 : AF_INET;
}

size_t sct_ib_address_size(const sct_ib_address_pack_params_t *params)
{
    size_t size = sizeof(sct_ib_address_t);

    if (params->flags & UCT_IB_ADDRESS_PACK_FLAG_ETH) {
        /* Ethernet: address contains only raw GID */
        size += sizeof(union ibv_gid);
    } else {
        /* InfiniBand: address always contains LID */
        size += sizeof(uint16_t); /* lid */

        if (params->flags & UCT_IB_ADDRESS_PACK_FLAG_INTERFACE_ID) {
            /* Add GUID */
            UCS_STATIC_ASSERT(sizeof(params->gid.global.interface_id) == sizeof(uint64_t));
            size += sizeof(uint64_t);
        }

        if (params->flags & UCT_IB_ADDRESS_PACK_FLAG_SUBNET_PREFIX) {
            if ((params->gid.global.subnet_prefix & SCT_IB_SITE_LOCAL_MASK) ==
                                                    SCT_IB_SITE_LOCAL_PREFIX) {
                /* 16-bit subnet prefix */
                size += sizeof(uint16_t);
            } else if (params->gid.global.subnet_prefix != SCT_IB_LINK_LOCAL_PREFIX) {
                /* 64-bit subnet prefix */
                size += sizeof(uint64_t);
            }
            /* Note: if subnet prefix is LINK_LOCAL, no need to pack it because
             * it's a well-known value defined by IB specification.
             */
        }
    }

    if (params->flags & UCT_IB_ADDRESS_PACK_FLAG_PATH_MTU) {
        size += sizeof(uint8_t);
    }

    if (params->flags & UCT_IB_ADDRESS_PACK_FLAG_GID_INDEX) {
        size += sizeof(uint8_t);
    }

    if (params->flags & UCT_IB_ADDRESS_PACK_FLAG_PKEY) {
        size += sizeof(uint16_t);
    }

    return size;
}

void sct_ib_address_pack(const sct_ib_address_pack_params_t *params,
                         sct_ib_address_t *ib_addr)
{
    void *ptr = ib_addr + 1;

    if (params->flags & UCT_IB_ADDRESS_PACK_FLAG_ETH) {
        /* RoCE, in this case we don't use the lid, we pack the gid, the RoCE
         * version, address family and set the ETH flag */
        ib_addr->flags = UCT_IB_ADDRESS_FLAG_LINK_LAYER_ETH |
                         (params->roce_info.ver <<
                          ucs_ilog2(UCT_IB_ADDRESS_FLAG_ETH_LAST));

        if (params->roce_info.addr_family == AF_INET6) {
            ib_addr->flags |= UCT_IB_ADDRESS_FLAG_ROCE_IPV6;
        }

        memcpy(ptr, params->gid.raw, sizeof(params->gid.raw));
        ptr = UCS_PTR_TYPE_OFFSET(ptr, params->gid.raw);
    } else {
        /* IB, LID */
        ib_addr->flags   = 0;
        *(uint16_t*)ptr  = params->lid;
        ptr              = UCS_PTR_TYPE_OFFSET(ptr, uint16_t);

        if (params->flags & UCT_IB_ADDRESS_PACK_FLAG_INTERFACE_ID) {
            /* Pack GUID */
            ib_addr->flags  |= UCT_IB_ADDRESS_FLAG_IF_ID;
            *(uint64_t*) ptr = params->gid.global.interface_id;
            ptr              = UCS_PTR_TYPE_OFFSET(ptr, uint64_t);
        }

        if (params->flags & UCT_IB_ADDRESS_PACK_FLAG_SUBNET_PREFIX) {
            if ((params->gid.global.subnet_prefix & SCT_IB_SITE_LOCAL_MASK) ==
                                                    SCT_IB_SITE_LOCAL_PREFIX) {
                /* Site-local */
                ib_addr->flags |= UCT_IB_ADDRESS_FLAG_SUBNET16;
                *(uint16_t*)ptr = params->gid.global.subnet_prefix >> 48;
                ptr             = UCS_PTR_TYPE_OFFSET(ptr, uint16_t);
            } else if (params->gid.global.subnet_prefix != SCT_IB_LINK_LOCAL_PREFIX) {
                /* Global */
                ib_addr->flags |= UCT_IB_ADDRESS_FLAG_SUBNET64;
                *(uint64_t*)ptr = params->gid.global.subnet_prefix;
                ptr             = UCS_PTR_TYPE_OFFSET(ptr, uint64_t);
            }
        }
    }

    if (params->flags & UCT_IB_ADDRESS_PACK_FLAG_PATH_MTU) {
        ucg_assert((int)params->path_mtu < UINT8_MAX);
        ib_addr->flags |= UCT_IB_ADDRESS_FLAG_PATH_MTU;
        *(uint8_t*)ptr  = (uint8_t)params->path_mtu;
        ptr             = UCS_PTR_TYPE_OFFSET(ptr, uint8_t);
    }

    if (params->flags & UCT_IB_ADDRESS_PACK_FLAG_GID_INDEX) {
        ib_addr->flags |= UCT_IB_ADDRESS_FLAG_GID_INDEX;
        *(uint8_t*)ptr  = params->gid_index;
    }

    if (params->flags & UCT_IB_ADDRESS_PACK_FLAG_PKEY) {
        ucg_assert(params->pkey != SCT_IB_ADDRESS_DEFAULT_PKEY);
        ib_addr->flags |= UCT_IB_ADDRESS_FLAG_PKEY;
        *(uint16_t*)ptr = params->pkey;
    }
}

unsigned sct_ib_iface_address_pack_flags(sct_ib_iface_t *iface)
{
    unsigned pack_flags = 0;

    if (iface->pkey != SCT_IB_ADDRESS_DEFAULT_PKEY) {
        pack_flags |= UCT_IB_ADDRESS_PACK_FLAG_PKEY;
    }

    if (sct_ib_iface_is_roce(iface)) {
        /* pack Ethernet address */
        pack_flags |= UCT_IB_ADDRESS_PACK_FLAG_ETH;
    } else if (iface->config.force_global_addr) {
        /* pack full IB address */
        pack_flags |= UCT_IB_ADDRESS_PACK_FLAG_SUBNET_PREFIX |
                      UCT_IB_ADDRESS_PACK_FLAG_INTERFACE_ID;
    } else {
        /* pack only subnet prefix for reachability test */
        pack_flags |= UCT_IB_ADDRESS_PACK_FLAG_SUBNET_PREFIX;
    }

    if (iface->config.path_mtu != IBV_MTU_4096) {
        pack_flags |= UCT_IB_ADDRESS_PACK_FLAG_PATH_MTU;
    }

    return pack_flags;
}

size_t sct_ib_iface_address_size(sct_ib_iface_t *iface)
{
    sct_ib_address_pack_params_t params;

    params.flags     = sct_ib_iface_address_pack_flags(iface);
    params.gid       = iface->gid_info.gid;
    params.roce_info = iface->gid_info.roce_info;
    return sct_ib_address_size(&params);
}

void sct_ib_iface_address_pack(sct_ib_iface_t *iface, sct_ib_address_t *ib_addr)
{
    sct_ib_address_pack_params_t params;

    params.flags     = sct_ib_iface_address_pack_flags(iface);
    params.gid       = iface->gid_info.gid;
    params.lid       = sct_ib_iface_port_attr(iface)->lid;
    params.roce_info = iface->gid_info.roce_info;
    params.path_mtu  = iface->config.path_mtu;
    /* to suppress gcc 4.3.4 warning */
    params.gid_index = SCT_IB_ADDRESS_INVALID_GID_INDEX;
    params.pkey      = iface->pkey;
    sct_ib_address_pack(&params, ib_addr);
}

void sct_ib_address_unpack(const sct_ib_address_t *ib_addr,
                           sct_ib_address_pack_params_t *params_p)
{
    const void *ptr                     = ib_addr + 1;
    /* silence cppcheck warning */
    sct_ib_address_pack_params_t params = {0};

    params.gid_index = SCT_IB_ADDRESS_INVALID_GID_INDEX;
    params.path_mtu  = SCT_IB_ADDRESS_INVALID_PATH_MTU;
    params.pkey      = SCT_IB_ADDRESS_DEFAULT_PKEY;

    if (ib_addr->flags & UCT_IB_ADDRESS_FLAG_LINK_LAYER_ETH) {
        memcpy(params.gid.raw, ptr, sizeof(params.gid.raw));
        ptr           = UCS_PTR_BYTE_OFFSET(ptr, sizeof(params.gid.raw));
        params.flags |= UCT_IB_ADDRESS_PACK_FLAG_ETH;

        params.roce_info.addr_family =
            sct_ib_address_flags_get_roce_af(ib_addr->flags);
        params.roce_info.ver         =
            sct_ib_address_flags_get_roce_version(ib_addr->flags);
    } else {
        /* Default prefix */
        params.gid.global.subnet_prefix = SCT_IB_LINK_LOCAL_PREFIX;
        params.gid.global.interface_id  = 0;
        params.flags                   |= UCT_IB_ADDRESS_PACK_FLAG_SUBNET_PREFIX |
                                          UCT_IB_ADDRESS_PACK_FLAG_INTERFACE_ID;

        /* If the link layer is not ETHERNET, then it is IB and a lid
         * must be present */
        params.lid                      = *(const uint16_t*)ptr;
        ptr                             = UCS_PTR_TYPE_OFFSET(ptr, uint16_t);

        if (ib_addr->flags & UCT_IB_ADDRESS_FLAG_IF_ID) {
            params.gid.global.interface_id = *(uint64_t*)ptr;
            ptr                            = UCS_PTR_TYPE_OFFSET(ptr, uint64_t);
        }

        if (ib_addr->flags & UCT_IB_ADDRESS_FLAG_SUBNET16) {
            params.gid.global.subnet_prefix = SCT_IB_SITE_LOCAL_PREFIX |
                                              (((uint64_t)*((uint16_t*)ptr)) << 48);
            ptr                             = UCS_PTR_TYPE_OFFSET(ptr, uint16_t);
            ucg_assert(!(ib_addr->flags & UCT_IB_ADDRESS_FLAG_SUBNET64));
        }

        if (ib_addr->flags & UCT_IB_ADDRESS_FLAG_SUBNET64) {
            params.gid.global.subnet_prefix = *(uint64_t*)ptr;
            ptr                             = UCS_PTR_TYPE_OFFSET(ptr, uint64_t);
            params.flags                   |= UCT_IB_ADDRESS_PACK_FLAG_SUBNET_PREFIX;
        }
    }

    if (ib_addr->flags & UCT_IB_ADDRESS_FLAG_PATH_MTU) {
        params.path_mtu = (enum ibv_mtu)*(const uint8_t*)ptr;
        ptr             = UCS_PTR_TYPE_OFFSET(ptr, const uint16_t);
        params.flags   |= UCT_IB_ADDRESS_PACK_FLAG_PATH_MTU;
    }

    if (ib_addr->flags & UCT_IB_ADDRESS_FLAG_GID_INDEX) {
        params.gid_index = *(const uint8_t*)ptr;
        ptr              = UCS_PTR_TYPE_OFFSET(ptr, const uint16_t);
        params.flags    |= UCT_IB_ADDRESS_PACK_FLAG_GID_INDEX;
    }

    if (ib_addr->flags & UCT_IB_ADDRESS_FLAG_PKEY) {
        params.pkey = *(const uint16_t*)ptr;
    }
    /* PKEY is always in params */
    params.flags |= UCT_IB_ADDRESS_PACK_FLAG_PKEY;

    *params_p = params;
}

ucs_status_t sct_ib_iface_get_device_address(sct_iface_h tl_iface,
                                             uct_device_addr_t *dev_addr)
{
    sct_ib_iface_t *iface = ucs_derived_of(tl_iface, sct_ib_iface_t);

    sct_ib_iface_address_pack(iface, (void*)dev_addr);

    return UCS_OK;
}

static uint64_t sct_ib_iface_roce_get_subnet_id(const uint8_t *addr, unsigned prefix_bits)
{
    size_t length = prefix_bits / 8;
    unsigned remainder_bits = prefix_bits % 8;

    uint64_t result = 0;
    if (length > 0) {
        memcpy(&result, addr, length);
    }

    result = (result << 8) + (*((uint8_t*)addr + length) & ~UCS_MASK(8 - remainder_bits));
    return result;
}

static ucs_status_t sct_ib_iface_init_roce_subnet_id(sct_ib_iface_t *iface)
{
    sct_ib_device_gid_info_t *gid_info = &iface->gid_info;
    ucs_status_t status = UCS_OK;
    uint8_t *local_addr;
    size_t addr_offset;
    size_t addr_size;
    unsigned prefix_bits = iface->addr_prefix_bits;
    sa_family_t local_ib_addr_af = gid_info->roce_info.addr_family;

    status = ucs_sockaddr_inet_addr_size(local_ib_addr_af, &addr_size);
    if (status != UCS_OK) {
        ucg_error("failed to detect RoCE address size");
        return UCS_ERR_INVALID_PARAM;
    }

    addr_offset = sizeof(union ibv_gid) - addr_size;
    local_addr  = UCS_PTR_BYTE_OFFSET(&gid_info->gid, addr_offset);

    iface->roce_subnet_id = sct_ib_iface_roce_get_subnet_id(local_addr, prefix_bits);
    return UCS_OK;
}

static int sct_ib_iface_roce_is_reachable(const sct_ib_device_gid_info_t *local_gid_info,
                                          const sct_ib_address_t *remote_ib_addr,
                                          unsigned prefix_bits)
{
    sa_family_t local_ib_addr_af         = local_gid_info->roce_info.addr_family;
    sct_ib_roce_version_t local_roce_ver = local_gid_info->roce_info.ver;
    uint8_t remote_ib_addr_flags         = remote_ib_addr->flags;
    sct_ib_roce_version_t remote_roce_ver;
    sa_family_t remote_ib_addr_af;
    char local_str[128], remote_str[128];
    uint8_t *local_addr, *remote_addr;
    ucs_status_t status;
    size_t addr_offset;
    size_t addr_size;
    int ret;

    /* check for wildcards in the RoCE version (RDMACM or non-RoCE cases) */
    if ((sct_ib_address_flags_get_roce_version(remote_ib_addr_flags)) ==
         UCT_IB_DEVICE_ROCE_ANY) {
        return 1;
    }

    /* check for zero-sized netmask */
    if (prefix_bits == 0) {
        return 1;
    }

    /* check the address family */
    remote_ib_addr_af = sct_ib_address_flags_get_roce_af(remote_ib_addr_flags);

    if (local_ib_addr_af != remote_ib_addr_af) {
        ucg_assert(local_ib_addr_af != 0);
        ucg_debug("different addr_family detected. local %s remote %s",
                  ucs_sockaddr_address_family_str(local_ib_addr_af),
                  ucs_sockaddr_address_family_str(remote_ib_addr_af));
        return 0;
    }

    /* check the RoCE version */
    ucg_assert(local_roce_ver != UCT_IB_DEVICE_ROCE_ANY);

    remote_roce_ver = sct_ib_address_flags_get_roce_version(remote_ib_addr_flags);

    if (local_roce_ver != remote_roce_ver) {
        ucg_debug("different RoCE versions detected. local %s (gid=%s)"
                  "remote %s (gid=%s)",
                  sct_ib_roce_version_str(local_roce_ver),
                  sct_ib_gid_str(&local_gid_info->gid, local_str,
                                 sizeof(local_str)),
                  sct_ib_roce_version_str(remote_roce_ver),
                  sct_ib_gid_str((union ibv_gid *)(remote_ib_addr + 1), remote_str,
                                 sizeof(remote_str)));
        return 0;
    }

    if (local_gid_info->roce_info.ver != UCT_IB_DEVICE_ROCE_V2) {
        return 1; /* We assume it is, but actually there's no good test */
    }

    status = ucs_sockaddr_inet_addr_size(local_ib_addr_af, &addr_size);
    if (status != UCS_OK) {
        ucg_error("failed to detect RoCE address size");
        return 0;
    }

    addr_offset = sizeof(union ibv_gid) - addr_size;
    local_addr  = UCS_PTR_BYTE_OFFSET(&local_gid_info->gid, addr_offset);
    remote_addr = UCS_PTR_BYTE_OFFSET(&remote_ib_addr->flags + 1, addr_offset);

    /* sanity check on the subnet mask size (bits belonging to the prefix) */
    ucg_assert((prefix_bits / 8) <= addr_size);

    /* check if the addresses have matching prefixes */
    ret = ucs_bitwise_is_equal(local_addr, remote_addr, prefix_bits);

    ucg_debug(ret ? "IP addresses match with a %u-bit prefix: local IP is %s,"
                    " remote IP is %s" :
                    "IP addresses do not match with a %u-bit prefix. local IP"
                    " is %s, remote IP is %s",
              prefix_bits,
              inet_ntop(local_ib_addr_af, local_addr, local_str, 128),
              inet_ntop(remote_ib_addr_af, remote_addr, remote_str, 128));

    return ret;
}

int sct_ib_iface_is_reachable(const sct_iface_h tl_iface,
                              const uct_device_addr_t *dev_addr,
                              const uct_iface_addr_t *iface_addr)
{
    sct_ib_iface_t *iface           = ucs_derived_of(tl_iface, sct_ib_iface_t);
    int is_local_eth                = sct_ib_iface_is_roce(iface);
    const sct_ib_address_t *ib_addr = (const void*)dev_addr;
    sct_ib_address_pack_params_t params;

    sct_ib_address_unpack(ib_addr, &params);
    /* at least one PKEY has to be with full membership */
    if (!((params.pkey | iface->pkey) & SCT_IB_PKEY_MEMBERSHIP_MASK) ||
        /* PKEY values have to be equal */
        ((params.pkey ^ iface->pkey) & SCT_IB_PKEY_PARTITION_MASK)) {
        return 0;
    }

    if (!is_local_eth && !(ib_addr->flags & UCT_IB_ADDRESS_FLAG_LINK_LAYER_ETH)) {
        /* same subnet prefix */
        return params.gid.global.subnet_prefix ==
               iface->gid_info.gid.global.subnet_prefix;
    } else if (is_local_eth && (ib_addr->flags & UCT_IB_ADDRESS_FLAG_LINK_LAYER_ETH)) {
        /* there shouldn't be a lid and the UCT_IB_ADDRESS_FLAG_LINK_LAYER_ETH
         * flag should be on. If reachable, the remote and local RoCE versions
         * and address families have to be the same */
        return sct_ib_iface_roce_is_reachable(&iface->gid_info, ib_addr,
                                              iface->addr_prefix_bits);
    } else {
        /* local and remote have different link layers and therefore are unreachable */
        return 0;
    }
}

void sct_ib_iface_fill_ah_attr_from_gid_lid(sct_ib_iface_t *iface, uint16_t lid,
                                            const union ibv_gid *gid,
                                            uint8_t gid_index,
                                            unsigned path_index,
                                            struct ibv_ah_attr *ah_attr)
{
    uint8_t path_bits;
    char buf[128];

    memset(ah_attr, 0, sizeof(*ah_attr));

    ucg_assert(iface->config.sl < SCT_IB_SL_NUM);

    ah_attr->sl                = iface->config.sl;
    ah_attr->port_num          = iface->config.port_num;
    ah_attr->grh.traffic_class = iface->config.traffic_class;

    if (sct_ib_iface_is_roce(iface)) {
        ah_attr->dlid          = SCT_IB_ROCE_UDP_SRC_PORT_BASE |
                                 (iface->config.roce_path_factor * path_index);
        /* Workaround rdma-core issue of calling rand() which affects global
         * random state in glibc */
        ah_attr->grh.flow_label = 1;
    } else {
        path_bits              = iface->path_bits[path_index %
                                                  iface->path_bits_count];
        ah_attr->dlid          = lid | path_bits;
        ah_attr->src_path_bits = path_bits;
    }

    if (iface->config.force_global_addr ||
        (iface->gid_info.gid.global.subnet_prefix != gid->global.subnet_prefix)) {
        ah_attr->is_global      = 1;
        ah_attr->grh.dgid       = *gid;
        ah_attr->grh.sgid_index = gid_index;
        ah_attr->grh.hop_limit  = iface->config.hop_limit;
    } else {
        ah_attr->is_global      = 0;
    }

    ucg_debug("iface %p: ah_attr %s", iface,
              sct_ib_ah_attr_str(buf, sizeof(buf), ah_attr));
}

void sct_ib_iface_fill_ah_attr_from_addr(sct_ib_iface_t *iface,
                                         const sct_ib_address_t *ib_addr,
                                         unsigned path_index,
                                         struct ibv_ah_attr *ah_attr,
                                         enum ibv_mtu *path_mtu)
{
    sct_ib_address_pack_params_t params;

    ucg_assert(!sct_ib_iface_is_roce(iface) ==
               !(ib_addr->flags & UCT_IB_ADDRESS_FLAG_LINK_LAYER_ETH));

    sct_ib_address_unpack(ib_addr, &params);

    if (params.flags & UCT_IB_ADDRESS_PACK_FLAG_PATH_MTU) {
        ucg_assert(params.path_mtu != SCT_IB_ADDRESS_INVALID_PATH_MTU);
        *path_mtu = params.path_mtu;
    } else {
        *path_mtu = iface->config.path_mtu;
    }

    if (params.flags & UCT_IB_ADDRESS_PACK_FLAG_GID_INDEX) {
        ucg_assert(params.gid_index != SCT_IB_ADDRESS_INVALID_GID_INDEX);
    } else {
        params.gid_index = iface->gid_info.gid_index;
    }

    sct_ib_iface_fill_ah_attr_from_gid_lid(iface, params.lid, &params.gid,
                                           params.gid_index, path_index,
                                           ah_attr);
}

static ucs_status_t sct_ib_iface_init_pkey(sct_ib_iface_t *iface,
                                           const sct_ib_iface_config_t *config)
{
    sct_ib_device_t *dev    = sct_ib_iface_device(iface);
    uint16_t pkey_tbl_len   = sct_ib_iface_port_attr(iface)->pkey_tbl_len;
    int pkey_found          = 0;
    uint16_t lim_pkey       = SCT_IB_ADDRESS_INVALID_PKEY;
    uint16_t lim_pkey_index = UINT16_MAX;
    uint16_t pkey_index, port_pkey, pkey;

    if ((config->pkey != UCS_HEXUNITS_AUTO) &&
        (config->pkey > SCT_IB_PKEY_PARTITION_MASK)) {
        ucg_error("requested pkey 0x%x is invalid, should be in the range 0..0x%x",
                  config->pkey, SCT_IB_PKEY_PARTITION_MASK);
        return UCS_ERR_INVALID_PARAM;
    }

    /* get the user's pkey value and find its index in the port's pkey table */
    for (pkey_index = 0; pkey_index < pkey_tbl_len; ++pkey_index) {
        /* get the pkey values from the port's pkeys table */
        if (ibv_query_pkey(dev->ibv_context, iface->config.port_num, pkey_index,
                           &port_pkey)) {
            ucg_debug("ibv_query_pkey("SCT_IB_IFACE_FMT", index=%d) failed: %m",
                      SCT_IB_IFACE_ARG(iface), pkey_index);
            continue;
        }

        pkey = ntohs(port_pkey);
        /* if pkey = 0x0, just skip it w/o debug trace, because 0x0
         * means that there is no real pkey configured at this index */
        if (pkey == SCT_IB_ADDRESS_INVALID_PKEY) {
            continue;
        }

        if ((config->pkey == UCS_HEXUNITS_AUTO) ||
            /* take only the lower 15 bits for the comparison */
            ((pkey & SCT_IB_PKEY_PARTITION_MASK) == config->pkey)) {
            if (!(pkey & SCT_IB_PKEY_MEMBERSHIP_MASK) &&
                /* limited PKEY has not yet been found */
                (lim_pkey == SCT_IB_ADDRESS_INVALID_PKEY)) {
                lim_pkey_index = pkey_index;
                lim_pkey       = pkey;
                continue;
            }

            iface->pkey_index = pkey_index;
            iface->pkey       = pkey;
            pkey_found        = 1;
            break;
        }
    }

    if (!pkey_found) {
        if (lim_pkey == SCT_IB_ADDRESS_INVALID_PKEY) {
            /* PKEY neither with full nor with limited membership was found */
            if (config->pkey == UCS_HEXUNITS_AUTO) {
                ucg_error("there is no valid pkey to use on "
                          SCT_IB_IFACE_FMT, SCT_IB_IFACE_ARG(iface));
            } else {
                ucg_error("unable to find specified pkey 0x%x on "SCT_IB_IFACE_FMT,
                          config->pkey, SCT_IB_IFACE_ARG(iface));
            }

            return UCS_ERR_NO_ELEM;
        } else {
            ucg_assert(lim_pkey_index != UINT16_MAX);
            iface->pkey_index = lim_pkey_index;
            iface->pkey       = lim_pkey;
        }
    }

    ucg_debug("using pkey[%d] 0x%x on "SCT_IB_IFACE_FMT, iface->pkey_index,
              iface->pkey, SCT_IB_IFACE_ARG(iface));

    return UCS_OK;
}

static ucs_status_t sct_ib_iface_init_lmc(sct_ib_iface_t *iface,
                                          const sct_ib_iface_config_t *config)
{
    unsigned i, j, num_path_bits;
    unsigned first, last;
    uint8_t lmc;
    int step;

    if (config->lid_path_bits.count == 0) {
        ucg_error("List of path bits must not be empty");
        return UCS_ERR_INVALID_PARAM;
    }

    /* count the number of lid_path_bits */
    num_path_bits = 0;
    for (i = 0; i < config->lid_path_bits.count; i++) {
        num_path_bits += 1 + abs((int)(config->lid_path_bits.ranges[i].first -
                                       config->lid_path_bits.ranges[i].last));
    }

    iface->path_bits = ucg_calloc(1, num_path_bits * sizeof(*iface->path_bits),
                                  "ib_path_bits");
    if (iface->path_bits == NULL) {
        return UCS_ERR_NO_MEMORY;
    }

    lmc = sct_ib_iface_port_attr(iface)->lmc;

    /* go over the list of values (ranges) for the lid_path_bits and set them */
    iface->path_bits_count = 0;
    for (i = 0; i < config->lid_path_bits.count; ++i) {
        first = config->lid_path_bits.ranges[i].first;
        last  = config->lid_path_bits.ranges[i].last;

        /* range of values or one value */
        if (first < last) {
            step = 1;
        } else {
            step = -1;
        }

        /* fill the value/s */
        for (j = first; j != (last + step); j += step) {
            if (j >= UCS_BIT(lmc)) {
                ucg_debug("Not using value %d for path_bits - must be < 2^lmc (lmc=%d)",
                          j, lmc);
                if (step == 1) {
                    break;
                } else {
                    continue;
                }
            }

            ucg_assert(iface->path_bits_count < num_path_bits);
            iface->path_bits[iface->path_bits_count] = j;
            iface->path_bits_count++;
        }
    }

    return UCS_OK;
}

static ucs_status_t sct_ib_iface_create_cq(sct_ib_iface_t *iface, sct_ib_dir_t dir,
                                           const sct_ib_iface_init_attr_t *init_attr,
                                           const sct_ib_iface_config_t *config,
                                           int preferred_cpu)
{
    ucs_status_t status;
    size_t inl                          = config->inl[dir];

    status = iface->ops->create_cq(iface, dir, init_attr, preferred_cpu, inl);
    if (status != UCS_OK) {
        goto out_unsetenv;
    }

    status = UCS_OK;

out_unsetenv:
    return status;
}

static void sct_ib_iface_set_num_paths(sct_ib_iface_t *iface,
                                       const sct_ib_iface_config_t *config)
{
    sct_ib_device_t *dev = sct_ib_iface_device(iface);

    if (config->num_paths == UCS_ULUNITS_AUTO) {
        if (sct_ib_iface_is_roce(iface)) {
            /* RoCE - number of paths is RoCE LAG level */
            iface->num_paths =
                    sct_ib_device_get_roce_lag_level(dev, iface->config.port_num,
                                                     iface->gid_info.gid_index);
        } else {
            /* IB - number of paths is LMC level */
            ucg_assert(iface->path_bits_count > 0);
            iface->num_paths = iface->path_bits_count;
        }
    } else {
        iface->num_paths = config->num_paths;
    }
}

int sct_ib_iface_is_roce_v2(sct_ib_iface_t *iface, sct_ib_device_t *dev)
{
    return sct_ib_iface_is_roce(iface) &&
           (iface->gid_info.roce_info.ver == UCT_IB_DEVICE_ROCE_V2);
}

ucs_status_t sct_ib_iface_init_roce_gid_info(sct_ib_iface_t *iface,
                                             size_t md_config_index)
{
    sct_ib_device_t *dev = sct_ib_iface_device(iface);
    uint8_t port_num     = iface->config.port_num;

    ucg_assert(sct_ib_iface_is_roce(iface));

    if (md_config_index == UCS_ULUNITS_AUTO) {
        return sct_ib_device_select_gid(dev, port_num, &iface->gid_info);
    }

    return sct_ib_device_query_gid_info(dev->ibv_context, sct_ib_device_name(dev),
                                        port_num, md_config_index,
                                        &iface->gid_info);
}

static ucs_status_t sct_tcp_netif_inaddr(const char *if_name, struct sockaddr_in *ifaddr,
                                         struct sockaddr_in *netmask)
{
    ucs_status_t status;
    struct ifreq ifra, ifrnm;

    status = ucs_netif_ioctl(if_name, SIOCGIFADDR, &ifra);
    if (status != UCS_OK) {
        return status;
    }

    if (netmask != NULL) {
        status = ucs_netif_ioctl(if_name, SIOCGIFNETMASK, &ifrnm);
        if (status != UCS_OK) {
            return status;
        }
    }

    if ((ifra.ifr_addr.sa_family != AF_INET) ) {
        ucs_error("%s address is not INET", if_name);
        return UCS_ERR_INVALID_ADDR;
    }

    memcpy(ifaddr,  (struct sockaddr_in*)&ifra.ifr_addr,  sizeof(*ifaddr));
    if (netmask != NULL) {
        memcpy(netmask, (struct sockaddr_in*)&ifrnm.ifr_addr, sizeof(*netmask));
    }

    return UCS_OK;
}

static ucs_status_t
sct_ib_iface_init_roce_addr_prefix(sct_ib_iface_t *iface,
                                   const sct_ib_iface_config_t *config)
{
    sct_ib_device_t *dev               = sct_ib_iface_device(iface);
    uint8_t port_num                   = iface->config.port_num;
    sct_ib_device_gid_info_t *gid_info = &iface->gid_info;
    size_t addr_size, max_prefix_bits;
    struct sockaddr_storage mask;
    struct sockaddr_storage ifaddr;
    char ndev_name[IFNAMSIZ];
    const void *mask_addr;
    ucs_status_t status;

    ucg_assert(sct_ib_iface_is_roce(iface));

    ucg_debug("The rocev2_local_subnet mode is %s, rocev2_subnet_pfx_len is %lu",
              config->rocev2_local_subnet ? "on" : "off", config->rocev2_subnet_pfx_len);
    if ((gid_info->roce_info.ver != UCT_IB_DEVICE_ROCE_V2) ||
        !config->rocev2_local_subnet) {
        iface->addr_prefix_bits = 0;
        return UCS_OK;
    }

    status = ucs_sockaddr_inet_addr_size(gid_info->roce_info.addr_family,
                                         &addr_size);
    if (status != UCS_OK) {
        return status;
    }

    max_prefix_bits = 8 * addr_size;
    ucg_assert(max_prefix_bits <= UINT8_MAX);

    if (config->rocev2_subnet_pfx_len == UCS_ULUNITS_INF) {
        /* Maximal prefix length value */
        iface->addr_prefix_bits = max_prefix_bits;
        return UCS_OK;
    } else if (config->rocev2_subnet_pfx_len != UCS_ULUNITS_AUTO) {
        /* Configured prefix length value */
        if (config->rocev2_subnet_pfx_len > max_prefix_bits) {
            ucg_error("invalid parameter for ROCE_SUBNET_PREFIX_LEN: "
                      "actual %zu, expected <= %zu",
                      config->rocev2_subnet_pfx_len, max_prefix_bits);
            return UCS_ERR_INVALID_PARAM;
        }

        iface->addr_prefix_bits = config->rocev2_subnet_pfx_len;
        return UCS_OK;
    }

    status = sct_ib_device_get_roce_ndev_name(dev, port_num,
                                              iface->gid_info.gid_index,
                                              ndev_name, sizeof(ndev_name));
    if (status != UCS_OK) {
        goto out_mask_info_failed;
    }

    status = sct_tcp_netif_inaddr(ndev_name, (struct sockaddr_in*)&ifaddr,
                                  (struct sockaddr_in*)&mask);
    if (status != UCS_OK) {
        goto out_mask_info_failed;
    }

    mask_addr               = ucs_sockaddr_get_inet_addr((struct sockaddr*)&mask);
    iface->addr_prefix_bits = max_prefix_bits -
                              ucs_count_ptr_trailing_zero_bits(mask_addr,
                                                               max_prefix_bits);
    return UCS_OK;

out_mask_info_failed:
    ucg_debug("failed to detect RoCE subnet mask prefix on "SCT_IB_IFACE_FMT
              " - ignoring mask", SCT_IB_IFACE_ARG(iface));
    iface->addr_prefix_bits = 0;
    return UCS_OK;
}

static ucs_status_t sct_ib_iface_init_gid_info(sct_ib_iface_t *iface,
                                               const sct_ib_iface_config_t *config)
{
    sct_ib_md_t *md                    = sct_ib_iface_md(iface);
    unsigned long md_config_index      = md->config.gid_index;
    sct_ib_device_gid_info_t *gid_info = &iface->gid_info;
    ucs_status_t status;

    /* Fill the gid index and the RoCE version */
    if (sct_ib_iface_is_roce(iface)) {
        status = sct_ib_iface_init_roce_gid_info(iface, md_config_index);
        if (status != UCS_OK) {
            goto out;
        }

        status = sct_ib_iface_init_roce_addr_prefix(iface, config);
        if (status != UCS_OK) {
            goto out;
        }
    } else {
        gid_info->gid_index             = (md_config_index ==
                                           UCS_ULUNITS_AUTO) ?
                                          SCT_IB_MD_DEFAULT_GID_INDEX :
                                          md_config_index;
        gid_info->roce_info.ver         = UCT_IB_DEVICE_ROCE_ANY;
        gid_info->roce_info.addr_family = 0;
    }

    /* Fill the gid */
    status = sct_ib_device_query_gid(sct_ib_iface_device(iface),
                                     iface->config.port_num,
                                     gid_info->gid_index, &gid_info->gid);
    if (status != UCS_OK) {
        goto out;
    }

out:
    return status;
}

static void sct_ib_iface_set_path_mtu(sct_ib_iface_t *iface,
                                      const sct_ib_iface_config_t *config)
{
    enum ibv_mtu port_mtu = sct_ib_iface_port_attr(iface)->active_mtu;
    sct_ib_device_t *dev  = sct_ib_iface_device(iface);

    /* MTU is set by user configuration */
    if (config->path_mtu != UCT_IB_MTU_DEFAULT) {
        /* cast from sct_ib_mtu_t to ibv_mtu */
        iface->config.path_mtu = (enum ibv_mtu)(config->path_mtu +
                                                (IBV_MTU_512 - UCT_IB_MTU_512));
    } else if ((port_mtu > IBV_MTU_2048) &&
               (IBV_DEV_ATTR(dev, vendor_id) == 0x02c9) &&
               ((IBV_DEV_ATTR(dev, vendor_part_id) == 4099) ||
                (IBV_DEV_ATTR(dev, vendor_part_id) == 4100) ||
                (IBV_DEV_ATTR(dev, vendor_part_id) == 4103) ||
                (IBV_DEV_ATTR(dev, vendor_part_id) == 4104))) {
        /* On some devices optimal path_mtu is 2048 */
        iface->config.path_mtu = IBV_MTU_2048;
    } else {
        iface->config.path_mtu = port_mtu;
    }
}

UCS_CLASS_INIT_FUNC(sct_ib_iface_t, sct_ib_iface_ops_t *ops, sct_md_h md,
                    sct_worker_h worker, const sct_iface_params_t *params,
                    const sct_ib_iface_config_t *config,
                    const sct_ib_iface_init_attr_t *init_attr)
{
    sct_ib_md_t *ib_md   = ucs_derived_of(md, sct_ib_md_t);
    sct_ib_device_t *dev = &ib_md->dev;
    ucs_status_t status;
    uint8_t port_num;

    ucs_cpu_set_t cpu_mask = params->cpu_mask;
    int preferred_cpu = ucs_cpu_set_find_lcs(&cpu_mask);

    UCS_CLASS_CALL_SUPER_INIT(sct_base_iface_t, &ops->super, md, worker,
                              params, &config->super);

    status = sct_ib_device_find_port(dev, params->mode.device.dev_name,
                                     &port_num);
    if (status != UCS_OK) {
        goto err;
    }

    self->ops                       = ops;
    self->config.seg_size           = init_attr->seg_size;
    self->config.roce_path_factor   = config->roce_path_factor;
    self->config.tx_max_poll        = config->tx.max_poll;
    self->config.port_num           = port_num;
    /* initialize to invalid value */
    self->config.sl                 = SCT_IB_SL_NUM;
    self->config.hop_limit          = config->hop_limit;
    ucg_debug("The hop_limit of RC is %u", config->hop_limit);
    sct_ib_iface_set_path_mtu(self, config);

    if (ucs_derived_of(worker, sct_priv_worker_t)->thread_mode == UCS_THREAD_MODE_MULTI) {
        ucg_error("IB transports do not support multi-threaded worker");
        return UCS_ERR_INVALID_PARAM;
    }

    status = sct_ib_iface_init_pkey(self, config);
    if (status != UCS_OK) {
        goto err;
    }

    status = sct_ib_iface_init_gid_info(self, config);
    if (status != UCS_OK) {
        goto err;
    }

    status = sct_ib_iface_init_roce_subnet_id(self);
    if (status != UCS_OK) {
        goto err;
    }
    md->subnet_id = self->roce_subnet_id;

    self->config.traffic_class = sct_ib_iface_is_roce_v2(self, dev) ?
                                 SCT_IB_DEFAULT_ROCEV2_DSCP : 0;

    status = sct_ib_iface_init_lmc(self, config);
    if (status != UCS_OK) {
        goto err;
    }

    sct_ib_iface_set_num_paths(self, config);

    self->comp_channel = ibv_create_comp_channel(dev->ibv_context);
    if (self->comp_channel == NULL) {
        ucg_error("ibv_create_comp_channel() failed: %m");
        status = UCS_ERR_IO_ERROR;
        goto err_cleanup;
    }

    status = ucs_sys_fcntl_modfl(self->comp_channel->fd, O_NONBLOCK, 0);
    if (status != UCS_OK) {
        goto err_destroy_comp_channel;
    }

    status = sct_ib_iface_create_cq(self, UCT_IB_DIR_TX, init_attr,
                                    config, preferred_cpu);
    if (status != UCS_OK) {
        goto err_destroy_comp_channel;
    }

    status = sct_ib_iface_create_cq(self, UCT_IB_DIR_RX, init_attr,
                                    config, preferred_cpu);
    if (status != UCS_OK) {
        goto err_destroy_send_cq;
    }

    /* Address scope and size */
    if (sct_ib_iface_is_roce(self) || config->is_global ||
        uct_ib_grh_required(sct_ib_iface_port_attr(self)) ||
        /* check ADDR_TYPE for backward compatibility */
        (config->addr_type == UCT_IB_ADDRESS_TYPE_SITE_LOCAL) ||
        (config->addr_type == UCT_IB_ADDRESS_TYPE_GLOBAL)) {
        self->config.force_global_addr = 1;
    } else {
        self->config.force_global_addr = 0;
    }

    self->addr_size  = sct_ib_iface_address_size(self);

    ucg_debug("created sct_ib_iface_t headroom_ofs %d payload_ofs %d hdr_ofs %d data_sz %d",
              self->config.rx_headroom_offset, self->config.rx_payload_offset,
              self->config.rx_hdr_offset, self->config.seg_size);

    return UCS_OK;

err_destroy_send_cq:
    ibv_destroy_cq(self->cq[UCT_IB_DIR_TX]);
err_destroy_comp_channel:
    ibv_destroy_comp_channel(self->comp_channel);
err_cleanup:
    ucg_free(self->path_bits);
err:
    return status;
}

static UCS_CLASS_CLEANUP_FUNC(sct_ib_iface_t)
{
    int ret;

    ret = ibv_destroy_cq(self->cq[UCT_IB_DIR_RX]);
    if (ret != 0) {
        ucg_warn("ibv_destroy_cq(recv_cq) returned %d: %m", ret);
    }

    ret = ibv_destroy_cq(self->cq[UCT_IB_DIR_TX]);
    if (ret != 0) {
        ucg_warn("ibv_destroy_cq(send_cq) returned %d: %m", ret);
    }

    ret = ibv_destroy_comp_channel(self->comp_channel);
    if (ucg_unlikely(ret != 0)) {
        ucg_warn("ibv_destroy_comp_channel(comp_channel) returned %d: %m", ret);
    }

    ucg_free(self->path_bits);
}

UCS_CLASS_DEFINE(sct_ib_iface_t, sct_base_iface_t);


static ucs_status_t sct_ib_iface_get_numa_latency(sct_ib_iface_t *iface,
                                                  double *latency)
{
    sct_ib_device_t *dev = sct_ib_iface_device(iface);
    sct_ib_md_t *md      = sct_ib_iface_md(iface);
    ucg_sys_cpuset_t temp_cpu_mask, process_affinity;
#if HAVE_NUMA
    int distance, min_cpu_distance;
    int cpu, num_cpus;
#endif
    int ret;

    if (!md->config.prefer_nearest_device) {
        *latency = 0;
        return UCS_OK;
    }

    ret = ucs_sys_getaffinity(&process_affinity);
    if (ret) {
        ucg_error("sched_getaffinity() failed: %m");
        return UCS_ERR_INVALID_PARAM;
    }

#if HAVE_NUMA
    /* Try to estimate the extra device latency according to NUMA distance */
    if (dev->numa_node != -1) {
        min_cpu_distance = INT_MAX;
        num_cpus         = ucs_min(CPU_SETSIZE, numa_num_configured_cpus());
        for (cpu = 0; cpu < num_cpus; ++cpu) {
            if (!CPU_ISSET(cpu, &process_affinity)) {
                continue;
            }
            distance = numa_distance(ucs_numa_node_of_cpu(cpu), dev->numa_node);
            if (distance >= UCS_NUMA_MIN_DISTANCE) {
                min_cpu_distance = ucs_min(min_cpu_distance, distance);
            }
        }

        if (min_cpu_distance != INT_MAX) {
            /* set the extra latency to (numa_distance - 10) * 20nsec */
            *latency = (min_cpu_distance - UCS_NUMA_MIN_DISTANCE) * 20e-9;
            return UCS_OK;
        }
    }
#endif

    /* Estimate the extra device latency according to its local CPUs mask */
    CPU_AND(&temp_cpu_mask, &dev->local_cpus, &process_affinity);
    if (CPU_EQUAL(&process_affinity, &temp_cpu_mask)) {
        *latency = 0;
    } else {
        *latency = 200e-9;
    }
    return UCS_OK;
}

ucs_status_t sct_ib_iface_query(sct_ib_iface_t *iface, size_t xport_hdr_len,
                                sct_iface_attr_t *iface_attr)
{
    static const uint8_t ib_port_widths[] = {
        [1] = 1, [2] = 4, [4] = 8, [8] = 12, [16] = 2
    };

    if (!sct_ib_iface_is_roce(iface)) {
        ucg_fatal("offload transport only support roce");
    }

    sct_base_iface_query(&iface->super, iface_attr);

    uint8_t active_width = sct_ib_iface_port_attr(iface)->active_width;
    uint8_t active_speed = sct_ib_iface_port_attr(iface)->active_speed;
    uint8_t active_mtu   = sct_ib_iface_port_attr(iface)->active_mtu;

    /*
     * Parse active width.
     * See IBTA section 14.2.5.6 "PortInfo", Table 164, field "LinkWidthEnabled"
     */
    uint8_t width;
    double encoding, signal_rate;
    if ((active_width >= ucs_static_array_size(ib_port_widths)) ||
        (ib_port_widths[active_width] == 0)) {
        ucg_warn("invalid active width on " SCT_IB_IFACE_FMT ": %d, "
                 "assuming 1x",
                 SCT_IB_IFACE_ARG(iface), active_width);
        width = 1;
    } else {
        width = ib_port_widths[active_width];
    }

    iface_attr->device_addr_len = iface->addr_size;
    iface_attr->dev_num_paths   = iface->num_paths;

    switch (active_speed) {
        case 1: /* SDR */
            iface_attr->latency.c = 5000e-9;
            signal_rate           = 2.5e9;
            encoding              = 8.0 / 10.0;
            break;
        case 2: /* DDR */
            iface_attr->latency.c = 2500e-9;
            signal_rate           = 5.0e9;
            encoding              = 8.0 / 10.0;
            break;
        case 4:
            iface_attr->latency.c = 1300e-9;
            if (sct_ib_iface_is_roce(iface)) {
                /* 10/40g Eth  */
                signal_rate       = 10.3125e9;
                encoding          = 64.0 / 66.0;
            } else {
                /* QDR */
                signal_rate       = 10.0e9;
                encoding          = 8.0 / 10.0;
            }
            break;
        case 8: /* FDR10 */
            iface_attr->latency.c = 700e-9;
            signal_rate           = 10.3125e9;
            encoding              = 64.0 / 66.0;
            break;
        case 16: /* FDR */
            iface_attr->latency.c = 700e-9;
            signal_rate           = 14.0625e9;
            encoding              = 64.0 / 66.0;
            break;
        case 32: /* EDR / 100g Eth */
            iface_attr->latency.c = 600e-9;
            signal_rate           = 25.78125e9;
            encoding              = 64.0 / 66.0;
            break;
        case 64: /* HDR / HDR100 / 50g Eth */
            iface_attr->latency.c = 600e-9;
            signal_rate           = 25.78125e9 * 2;
            encoding              = 64.0 / 66.0;
            break;
        default:
            ucg_error("Invalid active_speed on %s:%d: %d",
                      SCT_IB_IFACE_ARG(iface), active_speed);
            return UCS_ERR_IO_ERROR;
    }

    double numa_latency;
    ucs_status_t status = sct_ib_iface_get_numa_latency(iface, &numa_latency);
    UCG_ASSERT_RET(status == UCS_OK, ucg_status_s2g(status));

    iface_attr->latency.c += (numa_latency + 200e-9);
    iface_attr->latency.m  = 0;

    /* Wire speed calculation: Width * SignalRate * Encoding */
    double wire_speed = (width * signal_rate * encoding) / 8.0; /* Bit to byte */

    /* Calculate packet overhead  */
    size_t mtu = ucs_min(sct_ib_mtu_value((enum ibv_mtu)active_mtu),
                         iface->config.seg_size);

    size_t extra_pkt_len = SCT_IB_BTH_LEN + xport_hdr_len + SCT_IB_ICRC_LEN +
                           SCT_IB_VCRC_LEN + SCT_IB_DELIM_LEN + SCT_IB_GRH_LEN + SCT_IB_ROCE_LEN;

    sct_ib_md_t *md = sct_ib_iface_md(iface);
    iface_attr->bandwidth.shared = (wire_speed * mtu) / (mtu + extra_pkt_len);
    iface_attr->bandwidth.dedicated = 0;

    sct_ib_device_t *dev = sct_ib_iface_device(iface);
    iface_attr->priority = sct_ib_device_spec(dev)->priority;
    return UCS_OK;
}
