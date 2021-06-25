/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2020.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_BUILTIN_OPS_H_
#define UCG_BUILTIN_OPS_H_

BEGIN_C_DECLS

#include "../plan/builtin_plan.h"
#include <ucp/core/ucp_request.h>

/*
 * The built-in collective operations are composed of one or more steps.
 * In each step, we apply a method to a subgroup of peer processes.
 * Collectives are planned using "templates", and once the user
 * provides the details a step is "instantiated" from a suitable
 * template and the instance is executed. Often more than one instance
 * is created from the same template, and instances can run side-by-side.
 *
 * Methods are the basic algorithmic building blocks, like fan-in and
 * fan-out for trees, or the "Recursive K-ing" algorithm.
 * For example, Allreduce can either be done in two step,
 * fan-in and fanout, or in a single Recursive K-ing step.
 * Once the user requests an Allreduce operation - the selected
 * step templates are used to generate an instance
 * (or it is fetched from cache) and that instance is executed.
 */
typedef void (*mpi_reduce_f)(void *mpi_op, char *src_buffer,
                             char *dst_buffer, unsigned dcount,
                             void* mpi_datatype);
typedef void(*ucg_builtin_op_complete_cb_f)(void *complete_cb_arg);

extern ucg_plan_component_t ucg_builtin_component;
extern mpi_reduce_f ucg_builtin_mpi_reduce_cb;
extern unsigned builtin_base_am_id;
extern ucg_group_member_index_t g_myidx;
extern unsigned num_procs;

#ifndef MPI_IN_PLACE
#define MPI_IN_PLACE ((void*)0x1)
#endif

typedef union ucg_builtin_header {
    struct {
        ucg_group_id_t group_id;
        union {
            struct {
                ucg_coll_id_t  coll_id;
                ucg_step_idx_t step_idx;
            };
            uint16_t local_id;
        };
        ucg_offset_t remote_offset;
    };
    uint64_t header;
} ucg_builtin_header_t;

/*
 * The builtin operation
 */
enum ucg_builtin_op_step_flags {
    /* General characteristics */
    UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND    = UCS_BIT(0),
    UCG_BUILTIN_OP_STEP_FLAG_RECV_BEFORE_SEND1  = UCS_BIT(1),
    UCG_BUILTIN_OP_STEP_FLAG_RECV1_BEFORE_SEND  = UCS_BIT(2),

    UCG_BUILTIN_OP_STEP_FLAG_FIRST_STEP         = UCS_BIT(3),
    UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP          = UCS_BIT(4),
    UCG_BUILTIN_OP_STEP_FLAG_SINGLE_ENDPOINT    = UCS_BIT(5),
    UCG_BUILTIN_OP_STEP_FLAG_LENGTH_PER_REQUEST = UCS_BIT(6),
    UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED         = UCS_BIT(7),
    UCG_BUILTIN_OP_STEP_FLAG_PIPELINED          = UCS_BIT(8),

    /* Send types */
    UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT      = UCS_BIT(9),
    UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY      = UCS_BIT(10),
    UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY      = UCS_BIT(11),
};

enum ucg_builtin_op_step_displs_rule {
    /* rule of displacement for bruck plan with alltoall  */
    UCG_BUILTIN_OP_STEP_DISPLS_RULE_BRUCK_ALLTOALL
};

enum ucg_builtin_op_step_resend_flag {
    /* Flag whether to resend data */
    UCG_BUILTIN_OP_STEP_FIRST_SEND,
    UCG_BUILTIN_OP_STEP_RESEND,
};

/* Definitions of several callback functions, used during an operation */
typedef struct ucg_builtin_op ucg_builtin_op_t;
typedef struct ucg_builtin_request ucg_builtin_request_t;
typedef void         (*ucg_builtin_op_init_cb_t)  (ucg_builtin_op_t *op);
typedef ucs_status_t (*ucg_builtin_op_optm_cb_t)  (ucg_builtin_op_t *op);
typedef void         (*ucg_builtin_op_final_cb_t) (ucg_builtin_request_t *req);
typedef void         (*ucg_builtin_comp_send_cb_t)(ucg_builtin_request_t *req);
typedef int          (*ucg_builtin_comp_recv_cb_t)(ucg_builtin_request_t *req,
                                                   uint64_t offset,
                                                   void *data,
                                                   size_t length);

typedef struct ucg_builtin_zcomp {
    uct_completion_t           comp;
    ucg_builtin_request_t     *req;
} ucg_builtin_zcomp_t;

typedef struct ucg_builtin_op_step {
    uint16_t                   flags;            /* @ref enum ucg_builtin_op_step_flags */
    uint8_t                    iter_ep;          /* iterator, somewhat volatile */
    ucg_offset_t               iter_offset;      /* iterator, somewhat volatile */
    ucg_offset_t               remote_offset;    /*  for algorithm like ring    */
#define UCG_BUILTIN_OFFSET_PIPELINE_READY   ((ucg_offset_t)-1)
#define UCG_BUILTIN_OFFSET_PIPELINE_PENDING ((ucg_offset_t)-2)

    uct_iface_h                uct_iface;
    uct_md_h                   uct_md;
    ucg_builtin_plan_phase_t  *phase;

    int8_t                    *send_buffer;
    int8_t                    *recv_buffer;
    size_t                     buffer_length;
    size_t                     buffer_length_recv;
    ucg_builtin_header_t       am_header;
    uint32_t                   am_id;
    size_t                     buf_len_unit;   /* only for discrete buffer sending */

    uint32_t                   fragments;        /* != 1 for fragmented operations */
    size_t                     fragment_length;  /* only for fragmented operations */
    /* To enable pipelining of fragmented messages, each fragment has a counter,
     * similar to the request's overall "pending" counter. Once it reaches zero,
     * the fragment can be "forwarded" regardless of the other fragments.
     * This optimization is only valid for "*_WAYPOINT" methods. */
#define UCG_BUILTIN_FRAG_PENDING ((uint8_t)-1)
    volatile uint8_t          *fragment_pending;

    /* fragments for receiver */
    uint32_t                   fragments_recv;  /* != 1 for fragmented operations */

    unsigned                   displs_rule; /* @ref enum ucg_builtin_op_step_displs_rule */

    unsigned                   resend_flag; /* @ref enum ucg_builtin_op_step_resend_flag */

    ucg_builtin_comp_send_cb_t send_cb;
    ucg_builtin_comp_recv_cb_t recv_cb;

    /* Fields intended for non-contig datatypes */
    struct {
        int8_t                *contig_buffer;
        void                  *pack_state;
        void                  *unpack_state;
        void                  *pack_state_recv;
    } non_contig;

    /* Fields intended for zero-copy */
    struct {
        uct_mem_h              memh;
        ucg_builtin_zcomp_t   *zcomp;
        uint32_t               num_store; /* < number of step's store zcopy messages */
    } zcopy;
} ucg_builtin_op_step_t;

typedef struct ucg_builtin_comp_slot ucg_builtin_comp_slot_t;
struct ucg_builtin_op {
    ucg_op_t                  super;
    unsigned                  opt_cnt;  /**< optimization count-down */
    ucg_builtin_op_optm_cb_t  optm_cb;  /**< optimization function for the operation */
    ucg_builtin_op_init_cb_t  init_cb;  /**< Initialization function for the operation */
    ucg_builtin_op_final_cb_t final_cb; /**< Finalization function for the operation */
    ucp_dt_generic_t         *send_dt;  /**< Generic send datatype (if non-contig) */
    ucp_dt_generic_t         *recv_dt;  /**< Generic receive datatype (if non-contig) */
    dt_span_f                 dtspan_f;
    ucg_builtin_comp_slot_t  *slots;    /**< slots pointer, for faster initialization */
    ucs_list_link_t          *resend;   /**< resend pointer, for faster resend */
    ucg_builtin_op_step_t     steps[];  /**< steps required to complete the operation */
};

/*
 * For every instance of the builtin collective operation (op), we create allocate
 * a request to handle completion and interaction with the user (via API).
 */
struct ucg_builtin_request {
    ucg_request_t          super;
    volatile uint32_t      pending;   /**< number of step's pending messages */
    ucg_builtin_op_step_t *step;      /**< indicator of current step within the op */
    ucg_builtin_op_t      *op;        /**< operation currently running */
    ucg_request_t         *comp_req;  /**< completion status is written here */
    ucs_list_link_t        send_list; /**< membership in progress list */
    unsigned               recv_comp; /**< if recv is complete, only use in r1s */
};

ucs_status_t ucg_builtin_step_create (ucg_builtin_plan_phase_t *phase,
                                      ucp_datatype_t send_dtype,
                                      ucp_datatype_t recv_dtype,
                                      unsigned extra_flags,
                                      unsigned base_am_id,
                                      ucg_group_id_t group_id,
                                      const ucg_collective_params_t *params,
                                      int8_t **current_data_buffer,
                                      ucg_builtin_op_step_t *step);
ucs_status_t ucg_builtin_step_execute(ucg_builtin_request_t *req,
                                      ucg_request_t **user_req);

ucs_status_t ucg_builtin_op_create (ucg_plan_t *plan,
                                    const ucg_collective_params_t *params,
                                    ucg_op_t **op);
void         ucg_builtin_op_discard(ucg_op_t *op);
ucs_status_t ucg_builtin_op_trigger(ucg_op_t *op,
                                    ucg_coll_id_t coll_id,
                                    ucg_request_t **request);
ucs_status_t ucg_builtin_msg_process(ucg_builtin_comp_slot_t *slot, ucg_builtin_request_t *req);

void ucg_builtin_swap_net_recv(char *netdata, size_t length, size_t offset,
                               ucg_builtin_request_t *req);

size_t ucg_builtin_get_dt_len(ucp_dt_generic_t *dt_gen);

/*
 * Incoming messages are processed for one of the collective operations
 * currently outstanding - arranged in as a window (think: TCP) of slots.
 * The message is tied to a window slot according to its Active Message ID.
 *
 * The message contains the data as content, and an offset (in bytes) as
 * a "header", a.k.a. "immediate value" (see UCT API), which refers to the
 * location to apply (write or reduce) the payload within the local buffer.
 */
typedef struct ucg_builtin_comp_desc {
    ucp_recv_desc_t      super;
    char                 padding[UCP_WORKER_HEADROOM_PRIV_SIZE];
    ucg_builtin_header_t header;
    char                 data[0];
} ucg_builtin_comp_desc_t;

struct ucg_builtin_comp_slot {
    ucg_builtin_request_t      req;
    union {
        struct {
            ucg_coll_id_t      coll_id;
            ucg_step_idx_t     step_idx;
        };
        uint16_t               local_id;
    };
    ucg_builtin_comp_recv_cb_t cb;
    ucs_list_link_t            msg_head;
    ucs_mpool_t               *mp; /* pool of @ref ucg_builtin_comp_desc_t */
};


/*
 * This number sets the number of slots available for collective operations.
 * Each operation occupies a slot, so no more than this number of collectives
 * can take place at the same time. The slot is determined by the collective
 * operation id (ucg_coll_id_t) - modulo this constant. Translating "coll_id"
 * to slot# happens on every incoming packet, so this constant is best kept
 * determinable at compile time, and set to a power of 2.
 */
#define UCG_BUILTIN_MAX_CONCURRENT_OPS 16

#define UCG_BUILTIN_NUM_PROCS_DOUBLE 2

/*
 * If send data length is zero (MPI_Barrier, etc.) , UCP_DT_IS_CONTIG(_datatype)
 * is 0 but expected be 1. So we use UCG_DT_IS_CONTIG instead.
 */
#define UCG_DT_IS_CONTIG(_params, _datatype) \
    ((_params->send.dt_len) ? (UCP_DT_IS_CONTIG(_datatype)) : 1)

END_C_DECLS

#endif
