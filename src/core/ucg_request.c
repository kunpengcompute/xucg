/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ucg_request.h"
#include "ucg_group.h"
#include "ucg_plan.h"
#include "ucg_dt.h"

#include "util/ucg_log.h"
#include "util/ucg_helper.h"
#include "util/ucg_profile.h"
#include <string.h>


#define UCG_REQUEST_COPY_REQUIRED_FIELD(_field, _copy, _dst, _src, _err_label) \
    UCG_COPY_REQUIRED_FIELD(UCG_TOKENPASTE(UCG_REQUEST_INFO_FIELD_, _field), \
                            _copy, _dst, _src, _err_label)

#define UCG_REQUEST_COPY_OPTIONAL_FIELD(_field, _copy, _dst, _src, _default, _err_label) \
    UCG_COPY_OPTIONAL_FIELD(UCG_TOKENPASTE(UCG_REQUEST_INFO_FIELD_, _field), \
                            _copy, _dst, _src, _default, _err_label)

#define UCG_REQUEST_CHECK_MEM_TYPE_RETURN(_info, ...) \
    do { \
        ucg_request_info_t *info = _info; \
        if (!(info->field_mask & UCG_REQUEST_INFO_FIELD_MEM_TYPE) || \
            info->mem_type == UCG_MEM_TYPE_UNKNOWN) { \
            ucg_mem_type_t mem_type; \
            const void *buffers[] = {__VA_ARGS__}; \
            ucg_status_t status = ucg_request_check_mem_type(buffers, UCG_NUM_ARGS(__VA_ARGS__), &mem_type); \
            if (status != UCG_OK) { \
                return status; \
            } \
            info->field_mask |= UCG_REQUEST_INFO_FIELD_MEM_TYPE; \
            ucg_assert(mem_type != UCG_MEM_TYPE_UNKNOWN); \
            info->mem_type = mem_type; \
        } \
    } while(0)

#define UCG_REQUEST_APPLY_INFO_RETURN(_dst, _src, ...) \
    ucg_request_apply_info(_dst, _src); \
    UCG_REQUEST_CHECK_MEM_TYPE_RETURN(_dst, ##__VA_ARGS__)

static ucg_status_t ucg_request_check_mem_type(const void *buffers[], uint32_t count, ucg_mem_type_t *type)
{
    if (count == 0) {
        ucg_error("No buffer, unable to determine the memory type");
        return UCG_ERR_NOT_FOUND;
    }
    ucg_status_t status;
    ucg_mem_attr_t attr1;
    attr1.field_mask = UCG_REQUEST_INFO_FIELD_MEM_TYPE;
    status = ucg_mem_query(buffers[0], &attr1);
    if (status != UCG_OK) {
        ucg_error("Failed to query memory type");
        return status;
    }

    for (int i = 1; i < count; ++i) {
        ucg_mem_attr_t attr2;
        attr2.field_mask = UCG_REQUEST_INFO_FIELD_MEM_TYPE;
        status = ucg_mem_query(buffers[i], &attr2);
        if (status != UCG_OK) {
            ucg_error("Failed to query memory type");
            return status;
        }
        if (attr1.mem_type != attr2.mem_type) {
            ucg_error("Heterogeneous memory is not supported");
            return UCG_ERR_UNSUPPORTED;
        }
    }
    *type = attr1.mem_type;
    return UCG_OK;
}

static void ucg_request_apply_info(ucg_request_info_t *dst, const ucg_request_info_t *src)
{
    if (src == NULL) {
        dst->field_mask = 0;
        return;
    }

    uint64_t field_mask = src->field_mask;
    dst->field_mask = field_mask;
    UCG_REQUEST_COPY_OPTIONAL_FIELD(MEM_TYPE, UCG_COPY_VALUE,
                                    dst->mem_type, src->mem_type,
                                    UCG_MEM_TYPE_UNKNOWN, out);

    UCG_REQUEST_COPY_OPTIONAL_FIELD(CB, UCG_COPY_VALUE,
                                    dst->complete_cb, src->complete_cb,
                                    dst->complete_cb, out);

out:
    return;
}

static ucg_status_t ucg_request_ctor(ucg_request_t *self, const ucg_coll_args_t *args)
{
    self->status = UCG_OK;
    self->args = *args;
    self->id = UCG_GROUP_INVALID_REQ_ID;
    /** trade-off, get more information from comments of @ref ucg_op_init */
    if (args->type == UCG_COLL_TYPE_ALLREDUCE) {
        if (!ucg_op_is_persistent(args->allreduce.op)) {
            self->args.allreduce.op = &self->args.allreduce.gop.super;
            ucg_op_copy(self->args.allreduce.op, args->allreduce.op);
        }
    }
    return UCG_OK;
}

static void ucg_request_dtor(ucg_request_t *self)
{
    return;
}

static inline ucg_status_t ucg_request_init(ucg_group_t *group, ucg_coll_args_t *args,
                                            ucg_request_t **request)
{
    ucg_context_lock(group->context);

    ucg_plan_op_t *op;
    ucg_status_t status = ucg_plans_prepare(group->plans, args, group->size, &op);
    if (status != UCG_OK) {
        ucg_debug("Failed to prepare op(%d), %s", args->type, ucg_status_string(status));
        goto out;
    }
    op->super.group = group;
    *request = &op->super;
    ucg_assert((*request)->status == UCG_OK);
out:
    ucg_context_unlock(group->context);
    return status;
}

static inline void ucg_request_complete(ucg_request_t *request, ucg_status_t status)
{
    ucg_group_free_req_id(request->group, request->id);
    request->id = UCG_GROUP_INVALID_REQ_ID;
    ucg_request_info_t *info = &request->args.info;
    if (info->field_mask & UCG_REQUEST_INFO_FIELD_CB) {
        info->complete_cb.cb(info->complete_cb.arg, status);
    }
    return;
}

ucg_status_t ucg_request_bcast_init(void *buffer, int32_t count, ucg_dt_t *dt,
                                    ucg_rank_t root, ucg_group_h group,
                                    const ucg_request_info_t *info,
                                    ucg_request_h *request)
{
    UCG_CHECK_NULL_INVALID(buffer, dt, group, request);

    ucg_coll_args_t args = {
        .type = UCG_COLL_TYPE_BCAST,
        .bcast.buffer = buffer,
        .bcast.count = count,
        .bcast.dt = dt,
        .bcast.root = root,
    };
    UCG_REQUEST_APPLY_INFO_RETURN(&args.info, info, buffer);

    return ucg_request_init(group, &args, request);
}

ucg_status_t ucg_request_allreduce_init(const void *sendbuf, void *recvbuf,
                                        int32_t count, ucg_dt_t *dt,
                                        ucg_op_t *op, ucg_group_h group,
                                        const ucg_request_info_t *info,
                                        ucg_request_h *request)
{
    UCG_CHECK_NULL_INVALID(sendbuf, recvbuf, dt, op, group, request);

    ucg_coll_args_t args = {
        .type = UCG_COLL_TYPE_ALLREDUCE,
        .allreduce.sendbuf = sendbuf,
        .allreduce.recvbuf = recvbuf,
        .allreduce.count = count,
        .allreduce.dt = dt,
        .allreduce.op = op,
    };
    UCG_REQUEST_APPLY_INFO_RETURN(&args.info, info, sendbuf, recvbuf);

    return ucg_request_init(group, &args, request);
}

ucg_status_t ucg_request_barrier_init(ucg_group_h group,
                                      const ucg_request_info_t *info,
                                      ucg_request_h *request)
{
    UCG_CHECK_NULL_INVALID(group, request);

    ucg_coll_args_t args = {
        .type = UCG_COLL_TYPE_BARRIER,
    };
    UCG_REQUEST_APPLY_INFO_RETURN(&args.info, info);

    return ucg_request_init(group, &args, request);
}

ucg_status_t ucg_request_alltoallv_init(const void *sendbuf, const int32_t sendcounts[],
                                        const int32_t sdispls[], ucg_dt_t *sendtype,
                                        void *recvbuf, const int32_t recvcounts[],
                                        const int32_t rdispls[], ucg_dt_t *recvtype,
                                        ucg_group_h group, const ucg_request_info_t *info,
                                        ucg_request_h *request)
{
    UCG_CHECK_NULL_INVALID(sendbuf, sendcounts, sdispls, sendtype, recvbuf,
                           recvcounts, rdispls, recvtype, group, request);

    ucg_coll_args_t args = {
        .type = UCG_COLL_TYPE_ALLTOALLV,
        .alltoallv.sendbuf = sendbuf,
        .alltoallv.sendcounts = sendcounts,
        .alltoallv.sdispls = sdispls,
        .alltoallv.sendtype = sendtype,
        .alltoallv.recvbuf = recvbuf,
        .alltoallv.recvcounts = recvcounts,
        .alltoallv.rdispls = rdispls,
        .alltoallv.recvtype = recvtype,
    };
    UCG_REQUEST_APPLY_INFO_RETURN(&args.info, info, sendbuf, recvbuf);

    return ucg_request_init(group, &args, request);
}

ucg_status_t ucg_request_scatterv_init(const void *sendbuf, const int32_t *sendcounts,
                                       const int32_t *displs, ucg_dt_t *sendtype,
                                       void *recvbuf, int32_t recvcount,
                                       ucg_dt_t *recvtype, ucg_rank_t root,
                                       ucg_group_h group, const ucg_request_info_t *info,
                                       ucg_request_h *request)
{
#ifdef UCG_ENABLE_CHECK_PARAMS
    if (group->myrank == root) {
        UCG_CHECK_NULL_INVALID(sendbuf, sendcounts, displs, sendtype, group, request);
    } else {
        /* sendbuf, sendcounts, displs and sendtype are not significant for non-root process*/
        UCG_CHECK_NULL_INVALID(recvbuf, recvtype, group, request);
    }
#endif

    ucg_coll_args_t args = {
        .type = UCG_COLL_TYPE_SCATTERV,
        .scatterv.sendbuf = sendbuf,
        .scatterv.sendcounts = sendcounts,
        .scatterv.displs = displs,
        .scatterv.sendtype = sendtype,
        .scatterv.recvbuf = recvbuf,
        .scatterv.recvcount = recvcount,
        .scatterv.recvtype = recvtype,
        .scatterv.root = root,
    };

    if (group->myrank == root) {
        if (recvbuf == UCG_IN_PLACE) {
            UCG_REQUEST_APPLY_INFO_RETURN(&args.info, info, sendbuf);
        } else {
            UCG_REQUEST_APPLY_INFO_RETURN(&args.info, info, sendbuf, recvbuf);
        }
    } else {
        UCG_REQUEST_APPLY_INFO_RETURN(&args.info, info, recvbuf);
    }

    return ucg_request_init(group, &args, request);
}

ucg_status_t ucg_request_gatherv_init(const void *sendbuf, const int32_t sendcount,
                                      ucg_dt_t *sendtype, void *recvbuf,
                                      const int32_t* recvcounts, const int32_t *displs,
                                      ucg_dt_t *recvtype, ucg_rank_t root,
                                      ucg_group_h group, const ucg_request_info_t *info,
                                      ucg_request_h *request)
{
#ifdef UCG_ENABLE_CHECK_PARAMS
    if (group->myrank == root) {
        UCG_CHECK_NULL_INVALID(recvbuf, recvcounts, displs, recvbuf, group, request);
    } else {
        /* sendbuf, sendcounts, displs and sendtype are not significant for non-root process*/
        UCG_CHECK_NULL_INVALID(sendbuf, sendtype, group, request);
    }
#endif

    ucg_coll_args_t args = {
        .type = UCG_COLL_TYPE_GATHERV,
        .gatherv.sendbuf = sendbuf,
        .gatherv.sendcount = sendcount,
        .gatherv.sendtype = sendtype,
        .gatherv.recvbuf = recvbuf,
        .gatherv.recvcounts = recvcounts,
        .gatherv.displs = displs,
        .gatherv.recvtype = recvtype,
        .gatherv.root = root,
    };

    if (group->myrank == root) {
        if (sendbuf == UCG_IN_PLACE) {
            UCG_REQUEST_APPLY_INFO_RETURN(&args.info, info, recvbuf);
        } else {
            UCG_REQUEST_APPLY_INFO_RETURN(&args.info, info, sendbuf, recvbuf);
        }
    } else {
        UCG_REQUEST_APPLY_INFO_RETURN(&args.info, info, sendbuf);
    }

    return ucg_request_init(group, &args, request);
}

ucg_status_t ucg_request_allgatherv_init(const void *sendbuf, int sendcount,
                                         ucg_dt_t *sendtype, void *recvbuf,
                                         const int *recvcounts, const int *displs,
                                         ucg_dt_t *recvtype, ucg_group_h group,
                                         const ucg_request_info_t *info,
                                         ucg_request_h *request)
{
    UCG_CHECK_NULL_INVALID(sendbuf, sendtype, recvbuf, recvcounts, displs,
                           recvtype, group, request);

    ucg_coll_args_t args = {
        .type = UCG_COLL_TYPE_ALLGATHERV,
        .allgatherv.sendbuf = sendbuf,
        .allgatherv.sendcount = sendcount,
        .allgatherv.sendtype = sendtype,
        .allgatherv.recvbuf = recvbuf,
        .allgatherv.recvcounts = recvcounts,
        .allgatherv.displs = displs,
        .allgatherv.recvtype = recvtype,
    };

    if (sendbuf == UCG_IN_PLACE) {
        UCG_REQUEST_APPLY_INFO_RETURN(&args.info, info, recvbuf);
    } else {
        UCG_REQUEST_APPLY_INFO_RETURN(&args.info, info, sendbuf, recvbuf);
    }

    return ucg_request_init(group, &args, request);
}

UCG_PROFILE_FUNC(ucg_status_t, ucg_request_start, (request), ucg_request_h request)
{
    UCG_CHECK_NULL_INVALID(request);

    ucg_context_lock(request->group->context);

    if (ucg_unlikely(request->status != UCG_OK)) {
        ucg_error("Attempt to start a request with status %d", request->status);
        ucg_context_unlock(request->group->context);
        return request->status;
    }

    /* Requests with the same ID are combined into a complete collection op. */
    ucg_assert(request->id == UCG_GROUP_INVALID_REQ_ID);
    request->id = ucg_group_alloc_req_id(request->group);

    ucg_plan_op_t *op = ucg_derived_of(request, ucg_plan_op_t);
    ucg_status_t status = op->trigger(op);
    if (status == UCG_OK) {
        if (op->super.status == UCG_INPROGRESS) {
            ucg_list_add_tail(&op->super.group->context->plist, &op->super.list);
        } else {
            ucg_request_complete(&op->super, op->super.status);
        }
    }
    ucg_context_unlock(request->group->context);

    return status;
}

UCG_PROFILE_FUNC(ucg_status_t, ucg_request_test, (request), ucg_request_h request)
{
    UCG_CHECK_NULL_INVALID(request);

    ucg_context_lock(request->group->context);
    if (ucg_unlikely(request->status != UCG_INPROGRESS)) {
        ucg_context_unlock(request->group->context);
        return request->status;
    }

    ucg_plan_op_t *op = ucg_derived_of(request, ucg_plan_op_t);
    ucg_status_t status = op->progress(op);
    ucg_assert(status == op->super.status);
    if (status != UCG_INPROGRESS) {
        ucg_list_del(&op->super.list);
        ucg_request_complete(&op->super, status);
    }
    ucg_context_unlock(request->group->context);

    return status;
}

UCG_PROFILE_FUNC(ucg_status_t, ucg_request_cleanup, (request), ucg_request_h request)
{
    UCG_CHECK_NULL_INVALID(request);

    ucg_context_lock(request->group->context);

    if (ucg_unlikely(request->status == UCG_INPROGRESS)) {
        ucg_error("Attempt to cleanup a in-progress request");
        ucg_context_unlock(request->group->context);
        return UCG_INPROGRESS;
    }

    ucg_plan_op_t *op = ucg_derived_of(request, ucg_plan_op_t);
    ucg_status_t status = op->discard(op);
    ucg_context_unlock(request->group->context);
    return status;
}

ucg_status_t ucg_request_msg_size(const ucg_coll_args_t *args, const uint32_t size, uint32_t *msize)
{
    uint64_t total_size;
    uint64_t dt_size;
    switch (args->type) {
        case UCG_COLL_TYPE_BCAST:
            *msize = ucg_dt_size(args->bcast.dt) * args->bcast.count;
            break;
        case UCG_COLL_TYPE_ALLREDUCE:
            *msize = ucg_dt_size(args->allreduce.dt) * args->allreduce.count;
            break;
        case UCG_COLL_TYPE_BARRIER:
        case UCG_COLL_TYPE_ALLTOALLV:
        case UCG_COLL_TYPE_SCATTERV:
        case UCG_COLL_TYPE_GATHERV:
            *msize = 0;
            break;
        case UCG_COLL_TYPE_ALLGATHERV:
            /* The message size of each process is different, so using 0. */
            dt_size = (args->allgatherv.sendbuf != UCG_IN_PLACE) ?
                      (uint64_t)ucg_dt_size(args->allgatherv.sendtype) :
                      (uint64_t)ucg_dt_size(args->allgatherv.recvtype);
            total_size = 0;
            for (int i = 0; i < size; i++) {
                total_size += dt_size * args->allgatherv.recvcounts[i];
            }
            *msize = (uint32_t)(total_size / size);
            break;
        default:
            return UCG_ERR_INVALID_PARAM;
    }

    return UCG_OK;
}

const char* ucg_coll_type_string(ucg_coll_type_t coll_type)
{
    switch (coll_type) {
        case UCG_COLL_TYPE_BCAST:
            return "bcast";
        case UCG_COLL_TYPE_ALLREDUCE:
            return "allreduce";
        case UCG_COLL_TYPE_BARRIER:
            return "barrier";
        case UCG_COLL_TYPE_ALLTOALLV:
            return "alltoallv";
        case UCG_COLL_TYPE_SCATTERV:
            return "scatterv";
        case UCG_COLL_TYPE_GATHERV:
            return "gatherv";
        case UCG_COLL_TYPE_ALLGATHERV:
            return "allgatherv";
        default:
            return "unknown";
    }
}

UCG_CLASS_DEFINE(ucg_request_t, ucg_request_ctor, ucg_request_dtor);