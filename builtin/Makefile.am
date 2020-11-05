#
# Copyright (c) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
# See file LICENSE for terms.
#

noinst_LTLIBRARIES         = libucg_builtin.la
libucg_builtin_la_CXXFLAGS = $(BASE_CXXFLAGS)
libucg_builtin_la_CPPFLAGS = $(BASE_CPPFLAGS)
libucg_builtin_la_CFLAGS   = $(BASE_CFLAGS)

noinst_HEADERS = \
    ops/builtin_ops.h \
    plan/builtin_plan.h \
    builtin.h

libucg_builtin_la_SOURCES = \
    builtin.c \
    ops/builtin_ops.c \
    plan/builtin_binomial_tree.c \
    plan/builtin_recursive.c \
    plan/builtin_ring.c \
    plan/builtin_topo_info.c