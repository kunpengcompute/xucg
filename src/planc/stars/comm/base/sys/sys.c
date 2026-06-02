/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

#include "sys.h"

#include "time/time.h"


/* Default huge page size is 2 MBytes */
#define UCS_PROCESS_SMAPS_FILE     "/proc/self/smaps"
#define UCS_PROCESS_NS_DIR         "/proc/self/ns"
#define UCS_PROCESS_BOOTID_FILE    "/proc/sys/kernel/random/boot_id"
#define UCS_PROCESS_BOOTID_FMT     "%x-%4hx-%4hx-%4hx-%2hhx%2hhx%2hhx%2hhx%2hhx%2hhx"
#define UCS_PROCESS_NS_FIRST       0xF0000000U
#define UCS_PROCESS_NS_NET_DFLT    0xF0000080U
#define MAC_ADDRESS_LENGH          6
#define UCS_NS_INFO_ITEM(_id, _name, _dflt) \
    [_id] = {.name = (_name), .dflt = (_dflt), .value = (_dflt), \
             .init_once = UCS_INIT_ONCE_INITIALIZER}


struct {
    const char        *name;
    const ucs_sys_ns_t dflt;
    ucs_sys_ns_t       value;
    ucs_init_once_t    init_once; /* use own initialization sequence per NS */
} static ucs_sys_namespace_info[] = {
    UCS_NS_INFO_ITEM(UCS_SYS_NS_TYPE_IPC,  "ipc",  UCS_PROCESS_NS_FIRST - 1),
    UCS_NS_INFO_ITEM(UCS_SYS_NS_TYPE_MNT,  "mnt",  UCS_PROCESS_NS_FIRST - 0),
    UCS_NS_INFO_ITEM(UCS_SYS_NS_TYPE_NET,  "net",  UCS_PROCESS_NS_NET_DFLT),
    UCS_NS_INFO_ITEM(UCS_SYS_NS_TYPE_PID,  "pid",  UCS_PROCESS_NS_FIRST - 4),
    UCS_NS_INFO_ITEM(UCS_SYS_NS_TYPE_USER, "user", UCS_PROCESS_NS_FIRST - 3),
    UCS_NS_INFO_ITEM(UCS_SYS_NS_TYPE_UTS,  "uts",  UCS_PROCESS_NS_FIRST - 2)
};

const char *ucs_get_host_name(void)
{
    static char hostname[HOST_NAME_MAX] = {0};

    if (*hostname == 0) {
        gethostname(hostname, sizeof(hostname));
        strtok(hostname, ".");
    }
    return hostname;
}

static uint64_t ucs_get_mac_address()
{
    static uint64_t mac_address = 0;
    struct ifreq ifr, *it, *end;
    struct ifconf ifc;
    char buf[1024];
    int sock;

    if (mac_address == 0) {
        sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
        if (sock == -1) {
            ucg_error("failed to create socket: %m");
            return 0;
        }

        ifc.ifc_len = sizeof(buf);
        ifc.ifc_buf = buf;
        if (ioctl(sock, SIOCGIFCONF, &ifc) == -1) {
            ucg_error("ioctl(SIOCGIFCONF) failed: %m");
            close(sock);
            return 0;
        }

        it = ifc.ifc_req;
        end = it + (ifc.ifc_len / sizeof *it);
        for (it = ifc.ifc_req; it != end; ++it) {
            strcpy(ifr.ifr_name, it->ifr_name);

            if (ioctl(sock, SIOCGIFFLAGS, &ifr) != 0) {
                ucg_error("ioctl(SIOCGIFFLAGS) failed: %m");
                close(sock);
                return 0;
            }

            if (!(ifr.ifr_flags & IFF_LOOPBACK)) {
                if (ioctl(sock, SIOCGIFHWADDR, &ifr) != 0) {
                    ucg_error("ioctl(SIOCGIFHWADDR) failed: %m");
                    close(sock);
                    return 0;
                }
                memcpy(&mac_address, ifr.ifr_hwaddr.sa_data, MAC_ADDRESS_LENGH);
                break;
            }
        }

        close(sock);
        ucs_trace("MAC address is 0x%012"PRIX64, mac_address);
    }

    return mac_address;
}

static uint64_t __sumup_host_name(unsigned prime_index)
{
    uint64_t sum, n;
    const char *p;
    unsigned i;

    sum = 0;
    i = prime_index;
    p = ucs_get_host_name();
    while (*p != '\0') {
        n = 0;
        memcpy(&n, p, strnlen(p, sizeof(n)));
        sum += ucs_get_prime(i) * n;
        ++i;
        p += ucs_min(sizeof(n), strlen(p));
    }
    return sum;
}

uint64_t ucs_machine_guid()
{
    return ucs_get_prime(0) * ucs_get_mac_address() +
           __sumup_host_name(1);
}

/*
 * If a certain system constant (name) is undefined on the underlying system the
 * sysconf routine returns -1.  ucs_sysconf return the negative value
 * a user and the user is responsible to define default value or abort.
 *
 * If an error occurs sysconf modified errno and ucs_sysconf aborts.
 *
 * Otherwise, a non-negative values is returned.
 */
static long ucs_sysconf(int name)
{
    long rc;
    errno = 0;

    rc = sysconf(name);

    return rc;
}

int ucs_get_first_cpu(void)
{
    int first_cpu, total_cpus, ret;
    ucg_sys_cpuset_t mask;

    ret = ucs_sysconf(_SC_NPROCESSORS_CONF);
    if (ret < 0) {
        ucg_error("failed to get local cpu count: %m");
        return ret;
    }
    total_cpus = ret;

    CPU_ZERO(&mask);
    ret = ucs_sys_getaffinity(&mask);
    if (ret < 0) {
        ucg_error("failed to get process affinity: %m");
        return ret;
    }

    for (first_cpu = 0; first_cpu < total_cpus; ++first_cpu) {
        if (CPU_ISSET(first_cpu, &mask)) {
            return first_cpu;
        }
    }

    return total_cpus;
}

uint64_t ucs_generate_uuid(uint64_t seed)
{
    struct timeval tv;
    uint64_t high;
    uint64_t low;
    uint64_t boot_id = 0;
    ucs_status_t status;

    status = ucs_sys_get_boot_id(&high, &low);
    if (status == UCS_OK) {
        boot_id = high ^ low;
    } else {
        ucg_error("failed to get boot id");
    }

    gettimeofday(&tv, NULL);
    return seed +
           ucs_get_prime(0) * ucs_get_tid() +
           ucs_get_prime(1) * ucs_get_time() +
           ucs_get_prime(2) * boot_id +
           ucs_get_prime(3) * tv.tv_sec +
           ucs_get_prime(4) * tv.tv_usec +
           __sumup_host_name(5);
}

static ssize_t ucg_read_file_vararg(char *buffer, size_t max, int silent,
                                    const char *filename_fmt, va_list ap)
{
    char filename[MAXPATHLEN];
    ssize_t read_bytes;
    int fd;

    vsnprintf(filename, MAXPATHLEN, filename_fmt, ap);

    fd = open(filename, O_RDONLY);
    if (fd < 0) {
        if (!silent) {
            ucg_error("failed to open %s: %m", filename);
        }
        read_bytes = -1;
        goto out;
    }

    read_bytes = read(fd, buffer, max - 1);
    if (read_bytes < 0) {
        if (!silent) {
            ucg_error("failed to read from %s: %m", filename);
        }
        goto out_close;
    }

    if (read_bytes < max) {
        buffer[read_bytes] = '\0';
    }

out_close:
    close(fd);
out:
    return read_bytes;
}

ssize_t ucs_read_file(char *buffer, size_t max, int silent,
                      const char *filename_fmt, ...)
{
    ssize_t read_bytes;
    va_list ap;

    va_start(ap, filename_fmt);
    read_bytes = ucg_read_file_vararg(buffer, max, silent, filename_fmt, ap);
    va_end(ap);

    return read_bytes;
}

ucs_status_t ucg_read_file_number(long *value, int silent,
                                  const char *filename_fmt, ...)
{
    char buffer[64], *tail;
    ssize_t read_bytes;
    va_list ap;
    long n;

    va_start(ap, filename_fmt);
    read_bytes = ucg_read_file_vararg(buffer, sizeof(buffer) - 1, silent,
                                      filename_fmt, ap);
    va_end(ap);

    if (read_bytes < 0) {
        /* read error */
        return UCS_ERR_IO_ERROR;
    }

    n = strtol(buffer, &tail, 0);
    if ((*tail != '\0') && !isspace(*tail)) {
        /* parse error */
        return UCS_ERR_INVALID_PARAM;
    }

    *value = n;
    return UCS_OK;
}

ssize_t ucs_read_file_str(char *buffer, size_t max, int silent,
                          const char *filename_fmt, ...)
{
    size_t max_read = ucs_max(max, 1) - 1;
    ssize_t read_bytes;
    va_list ap;

    va_start(ap, filename_fmt);
    read_bytes = ucg_read_file_vararg(buffer, max_read, silent, filename_fmt, ap);
    va_end(ap);

    if ((read_bytes >= 0) && (max > 0)) {
        buffer[read_bytes] = '\0';
    }

    return read_bytes;
}

size_t ucs_get_page_size()
{
    static long page_size = 0;

    if (page_size == 0) {
        page_size = ucs_sysconf(_SC_PAGESIZE);
        if (page_size < 0) {
            page_size = 4096;
            ucg_debug("_SC_PAGESIZE is undefined, setting default value to %ld",
                      page_size);
        }
    }
    return page_size;
}

void ucs_sys_iterate_vm(void *address, size_t size, ucs_sys_vma_cb_t cb,
                        void *ctx)
{
    ucs_sys_vma_info_t info;
    unsigned long start, end;
    unsigned long page_size_kb;
    char buf[1024];
    char *p, *save;
    FILE *file;
    int n;

    file = fopen(UCS_PROCESS_SMAPS_FILE, "r");
    if (!file) {
        return;
    }

    while (fgets(buf, sizeof(buf), file) != NULL) {
        n = sscanf(buf, "%lx-%lx", &start, &end);
        if (n != 2) {
            continue;
        }

        if (start > (uintptr_t)address + size) {
            /* the scanned range is after memory range of interest - stop */
            break;
        }
        if (end <= (uintptr_t)address) {
            /* the scanned range is still before the memory range of interest */
            continue;
        }

        memset(&info, 0, sizeof(info));
        info.start = start;
        info.end   = end;

        while (fgets(buf, sizeof(buf), file) != NULL) {
            n = sscanf(buf, "KernelPageSize: %lu kB", &page_size_kb);
            if (n == 1) {
                info.page_size = page_size_kb * UCS_KBYTE;
                continue;
            }

            n = 9;
            if (memcmp(buf, "VmFlags: ", n) == 0) {
                p = buf + n;
                while ((p = strtok_r(p, " \n", &save)) != NULL) {
                    if (strcmp(p, "dc") == 0) {
                        info.flags |= UCS_SYS_VMA_FLAG_DONTCOPY;
                    }

                    p = NULL;
                }

                break;
            }
        }

        cb(&info, ctx);
    }

    fclose(file);
}

typedef struct {
    int    found;
    size_t min_page_size;
    size_t max_page_size;
} ucs_mem_page_size_info_t;

static void ucs_get_mem_page_size_cb(ucs_sys_vma_info_t *mem_info, void *ctx)
{
    ucs_mem_page_size_info_t *info = (ucs_mem_page_size_info_t *)ctx;

    if (info->found) {
        info->min_page_size = ucs_min(info->min_page_size, mem_info->page_size);
        info->max_page_size = ucs_max(info->max_page_size, mem_info->page_size);
    } else {
        info->found         = 1;
        info->min_page_size = mem_info->page_size;
        info->max_page_size = mem_info->page_size;
    }
}

void ucs_get_mem_page_size(void *address, size_t size, size_t *min_page_size_p,
                           size_t *max_page_size_p)
{
    ucs_mem_page_size_info_t info = {};

    ucs_sys_iterate_vm(address, size, ucs_get_mem_page_size_cb, &info);

    if (info.found) {
        *min_page_size_p = info.min_page_size;
        *max_page_size_p = info.max_page_size;
    } else {
        *min_page_size_p = *max_page_size_p = ucs_get_page_size();
    }
}

static void ucs_sysv_shmget_error_check_ENOSPC(size_t alloc_size,
                                               const struct shminfo *ipc_info,
                                               char *buf, size_t max)
{
    unsigned long new_used_ids;
    unsigned long new_shm_tot;
    struct shm_info shm_info;
    char *p, *endp;
    int ret;

    p    = buf;
    endp = p + max;

    ret = shmctl(0, SHM_INFO, (struct shmid_ds *)&shm_info);
    if (ret >= 0) {
        return;
    }

    new_used_ids = shm_info.used_ids;
    if (new_used_ids > ipc_info->shmmni) {
        snprintf(p, endp - p,
                 ", total number of segments in the system (%lu) would exceed the"
                 " limit in /proc/sys/kernel/shmmni (=%lu)",
                 new_used_ids, ipc_info->shmmni);
        p += strlen(p);
    }

    new_shm_tot = shm_info.shm_tot +
                  (alloc_size + ucs_get_page_size() - 1) / ucs_get_page_size();
    if (new_shm_tot > ipc_info->shmall) {
        snprintf(p, endp - p,
                 ", total shared memory pages in the system (%lu) would exceed the"
                 " limit in /proc/sys/kernel/shmall (=%lu)",
                 new_shm_tot, ipc_info->shmall);
    }
}

static void ucs_sysv_shmget_error_check_EPERM(int flags, char *buf, size_t max)
{
    snprintf(buf, max,
             ", please check for CAP_IPC_LOCK privilege for using SHM_HUGETLB");
}

static void ucs_sysv_shmget_format_error(size_t alloc_size, int flags,
                                         const char *alloc_name, int sys_errno,
                                         char *buf, size_t max)
{
    struct shminfo ipc_info;
    char *p, *endp, *errp;
    int ret;

    buf[0] = '\0';
    p      = buf;
    endp   = p + max;

    snprintf(p, endp - p, "shmget(size=%zu flags=0x%x) for %s failed: %s",
             alloc_size, flags, alloc_name, strerror(sys_errno));
    p   += strlen(p);
    errp = p; /* save current string pointer to detect if anything was added */

    ret = shmctl(0, IPC_INFO, (struct shmid_ds *)&ipc_info);
    if (ret >= 0) {
        if ((sys_errno == EINVAL) && (alloc_size > ipc_info.shmmax)) {
            snprintf(p, endp - p,
                     ", allocation size exceeds /proc/sys/kernel/shmmax (=%zu)",
                     ipc_info.shmmax);
            p += strlen(p);
        }

        if (sys_errno == ENOSPC) {
            ucs_sysv_shmget_error_check_ENOSPC(alloc_size, &ipc_info, p, endp - p);
            p += strlen(p);
        }
    }

    if (sys_errno == EPERM) {
        ucs_sysv_shmget_error_check_EPERM(flags, p, endp - p);
        p += strlen(p);
    }

    /* default error message if no useful information was added to the string */
    if (p == errp) {
        snprintf(p, endp - p, ", please check shared memory limits by 'ipcs -l'");
    }
}

ucs_status_t ucs_sysv_alloc(size_t *size, size_t max_size, void **address_p,
                            int flags, const char *alloc_name, int *shmid)
{
    char error_string[256];
    size_t alloc_size;
    int sys_errno;
    void *ptr;
    int ret;

    {
        alloc_size = ucs_align_up(*size, ucs_get_page_size());
    }

    if (alloc_size >= max_size) {
        return UCS_ERR_EXCEEDS_LIMIT;
    }

    flags |= IPC_CREAT | SHM_R | SHM_W;
    *shmid = shmget(IPC_PRIVATE, alloc_size, flags);
    if (*shmid < 0) {
        sys_errno = errno;
        ucs_sysv_shmget_format_error(alloc_size, flags, alloc_name, sys_errno,
                                     error_string, sizeof(error_string));
        switch (sys_errno) {
            case ENOMEM:
            case EPERM:
                ucg_error("%s", error_string);
                shmctl(*shmid, IPC_RMID, NULL);
                return UCS_ERR_NO_MEMORY;
            case ENOSPC:
            case EINVAL:
                ucg_error("%s", error_string);
                shmctl(*shmid, IPC_RMID, NULL);
                return UCS_ERR_NO_MEMORY;
            default:
                ucg_error("%s", error_string);
                shmctl(*shmid, IPC_RMID, NULL);
                return UCS_ERR_SHMEM_SEGMENT;
        }
    }

    /* Attach segment */
    if (*address_p) {
        shmctl(*shmid, IPC_RMID, NULL);
        return UCS_ERR_INVALID_PARAM;
    }

    ptr = shmat(*shmid, NULL, 0);

    /* Remove segment, the attachment keeps a reference to the mapping */
    ret = shmctl(*shmid, IPC_RMID, NULL);
    if (ret != 0) {
        ucg_warn("shmctl(IPC_RMID, shmid=%d) returned %d: %m", *shmid, ret);
    }

    /* Check if attachment was successful */
    if (ptr == (void*)-1) {
        if (errno == ENOMEM) {
            return UCS_ERR_NO_MEMORY;
        } else {
            ucg_error("shmat(shmid=%d) returned unexpected error: %m", *shmid);
            return UCS_ERR_SHMEM_SEGMENT;
        }
    }

    *address_p = ptr;
    *size      = alloc_size;
    return UCS_OK;
}

ucs_status_t ucs_sysv_free(void *address)
{
    int ret;

    ret = shmdt(address);
    if (ret) {
        ucg_warn("Unable to detach shared memory segment at %p: %m", address);
        return UCS_ERR_INVALID_PARAM;
    }

    return UCS_OK;
}

ucs_status_t ucs_mmap_alloc(size_t *size, void **address_p,
                            int flags)
{
    size_t alloc_length;
    void *addr;

    alloc_length = ucs_align_up_pow2(*size, ucs_get_page_size());

    addr = mmap(*address_p, alloc_length, PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANON | flags, -1, 0);
    if (addr == MAP_FAILED) {
        return UCS_ERR_NO_MEMORY;
    }

    *size      = alloc_length;
    *address_p = addr;
    return UCS_OK;
}

ucs_status_t ucs_mmap_free(void *address, size_t length)
{
    int ret;
    size_t alloc_length;

    alloc_length = ucs_align_up_pow2(length, ucs_get_page_size());

    ret = munmap(address, alloc_length);
    if (ret != 0) {
        ucg_warn("munmap(address=%p, length=%zu) failed: %m", address, length);
        return UCS_ERR_INVALID_PARAM;
    }
    return UCS_OK;
}

typedef struct {
    unsigned long start;
    unsigned long end;
    int           prot;
    int           found;
} ucs_get_mem_prot_ctx_t;

ucs_status_t ucs_sys_fcntl_modfl(int fd, int add, int rem)
{
    int oldfl, ret;

    oldfl = fcntl(fd, F_GETFL);
    if (oldfl < 0) {
        ucg_error("fcntl(fd=%d, F_GETFL) returned %d: %m", fd, oldfl);
        return UCS_ERR_IO_ERROR;
    }

    ret = fcntl(fd, F_SETFL, (oldfl | add) & ~rem);
    if (ret < 0) {
        ucg_error("fcntl(fd=%d, F_SETFL) returned %d: %m", fd, ret);
        return UCS_ERR_IO_ERROR;
    }

    return UCS_OK;
}

pid_t ucs_get_tid(void)
{
#ifdef SYS_gettid
    return syscall(SYS_gettid);
#else
#error "Port me"
#endif
}

int ucs_sys_getaffinity(ucg_sys_cpuset_t *cpuset)
{
    int ret;

#if defined(HAVE_SCHED_GETAFFINITY)
    ret = sched_getaffinity(0, sizeof(*cpuset), cpuset);
#elif defined(HAVE_CPUSET_GETAFFINITY)
    ret = cpuset_getaffinity(CPU_LEVEL_WHICH, CPU_WHICH_PID, getpid(),
                             sizeof(*cpuset), cpuset);
#else
#error "Port me"
#endif
    return ret;
}

void ucs_sys_cpuset_copy(ucs_cpu_set_t *dst, const ucg_sys_cpuset_t *src)
{
    int c;

    UCS_CPU_ZERO(dst);
    for (c = 0; c < UCS_CPU_SETSIZE; ++c) {
        if (CPU_ISSET(c, src)) {
            UCS_CPU_SET(c, dst);
        }
    }
}

ucs_sys_ns_t ucs_sys_get_ns(ucs_sys_namespace_type_t ns)
{
    char filename[MAXPATHLEN];
    int res;
    struct stat st;

    if (ns >= UCS_SYS_NS_TYPE_LAST) {
        return 0;
    }

    UCS_INIT_ONCE(&ucs_sys_namespace_info[ns].init_once) {
        snprintf(filename, sizeof(filename), "%s/%s", UCS_PROCESS_NS_DIR,
                 ucs_sys_namespace_info[ns].name);

        res = stat(filename, &st);
        if (res == 0) {
            ucs_sys_namespace_info[ns].value = (ucs_sys_ns_t)st.st_ino;
        } else {
            ucg_debug("failed to stat(%s): %m", filename);
        }
    }

    return ucs_sys_namespace_info[ns].value;
}

int ucs_sys_ns_is_default(ucs_sys_namespace_type_t ns)
{
    return ucs_sys_get_ns(ns) == ucs_sys_namespace_info[ns].dflt;
}

ucs_status_t ucs_sys_get_boot_id(uint64_t *high, uint64_t *low)
{
    static struct {
        uint64_t     high;
        uint64_t     low;
    } boot_id                        = {0, 0};

    static ucs_init_once_t init_once = UCS_INIT_ONCE_INITIALIZER;
    static ucs_status_t status       = UCS_ERR_IO_ERROR;
    char bootid_str[256];
    ssize_t size;
    uint32_t v1;
    uint16_t v2;
    uint16_t v3;
    uint16_t v4;
    uint8_t v5[6];
    int res;
    int i;

    UCS_INIT_ONCE(&init_once) {
        size = ucs_read_file_str(bootid_str, sizeof(bootid_str), 1,
                                 "%s", UCS_PROCESS_BOOTID_FILE);
        if (size <= 0) {
            continue; /* jump out of INIT_ONCE section */
        }

        res = sscanf(bootid_str, UCS_PROCESS_BOOTID_FMT,
                     &v1, &v2, &v3, &v4,
                     &v5[0], &v5[1], &v5[2],
                     &v5[3], &v5[4], &v5[5]);
        if (res == 10) { /* 10 values should be scanned */
            status       = UCS_OK;
            boot_id.low  = ((uint64_t)v1) | ((uint64_t)v2 << 32) |
                           ((uint64_t)v3 << 48);
            boot_id.high = v4;
            for (i = 0; i < ucs_static_array_size(v5); i++) {
                boot_id.high |= (uint64_t)v5[i] << (16 + (i * 8));
            }
        }
    }

    if (status == UCS_OK) {
        *high = boot_id.high;
        *low  = boot_id.low;
    }

    return status;
}
