
#
# Copyright (C) Huawei Technologies Co., Ltd. 2019-2021. ALL RIGHTS RESERVED.
# See file LICENSE for terms
#

if HAVE_OMPI_SRC
override CC = $(MPICC)
override CXX = $(MPICXX)
else
CXXFLAGS += "-fpermissive"
endif

noinst_LTLIBRARIES   = libucg_secure.la
libucg_secure_la_CFLAGS  = $(BASE_CFLAGS)
libucg_secure_la_CPPFLAGS = $(BASE_CPPFLAGS)

noinst_HEADERS = \
		 include/securec.h \
		 include/securectype.h

libucg_secure_la_SOURCES = \
			  src/memcpy_s.c \
			  src/memmove_s.c \
			  src/memset_s.c
