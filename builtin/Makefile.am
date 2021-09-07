#
# Copyright (c) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
# See file LICENSE for terms.
#

if HAVE_OMPI_SRC
override CC  = $(MPICC)
override CXX = $(MPICXX)
else
CXXFLAGS += "-fpermissive"
endif

noinst_LTLIBRARIES         = libucg_builtin.la
libucg_builtin_la_CXXFLAGS = $(BASE_CXXFLAGS)
libucg_builtin_la_CPPFLAGS = $(BASE_CPPFLAGS)
libucg_builtin_la_CFLAGS   = $(BASE_CFLAGS)

noinst_HEADERS = \
	ops/builtin_ops.h \
	ops/builtin_cb.inl \
	plan/builtin_plan.h \
	plan/builtin_algo_decision.h \
	plan/builtin_plan_cache.h \
	plan/builtin_topo.h

libucg_builtin_la_SOURCES = \
	builtin.c \
	ops/builtin_ops.c \
	plan/builtin_algo_select.c \
	plan/builtin_algo_check.c \
    plan/builtin_algo_decision.c \
	plan/builtin_plan_cache.c \
	plan/builtin_binomial_tree.c \
	plan/builtin_recursive.c \
	plan/builtin_ring.c \
    plan/builtin_topo_info.c \
	plan/builtin_trees.c \
    plan/builtin_topo_aware.c \
	plan/builtin_topo.c \
	plan/builtin_binary_block.c
