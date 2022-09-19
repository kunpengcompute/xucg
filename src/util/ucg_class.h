/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_CLASS_H_
#define UCG_CLASS_H_

#include "ucg_malloc.h"

#define UCG_CLASS_CTOR_ARGS(...) __VA_ARGS__

#define UCG_CLASS_NAME(_type) ucg_class_name_##_type

#define UCG_CLASS_DECLARE(_type, _ctor_args) \
    typedef struct { \
        ucg_status_t (*ctor)(_type *self, _ctor_args); \
        void (*dtor)(_type *self); \
    } ucg_class_##_type##_t; \
    extern ucg_class_##_type##_t UCG_CLASS_NAME(_type);

/** Define a base class. */
#define UCG_CLASS_DEFINE(_type, _ctor, _dtor) \
    ucg_class_##_type##_t UCG_CLASS_NAME(_type) = { \
        .ctor = _ctor, \
        .dtor = _dtor, \
    };

/** Call the constructor of the class type. */
#define UCG_CLASS_CONSTRUCT(_type, _self, ...) \
    ({ \
        ucg_class_##_type##_t *class_type = &UCG_CLASS_NAME(_type); \
        ucg_status_t status = class_type->ctor(_self, ##__VA_ARGS__); \
        status; \
    })

/** Call the destructor of the class type. */
#define UCG_CLASS_DESTRUCT(_type, _self) \
    do { \
        ucg_class_##_type##_t *class_type = &UCG_CLASS_NAME(_type); \
        class_type->dtor(_self); \
    } while(0)

/** Allocate space and call the constructor of the class type. */
#define UCG_CLASS_NEW(_type, ...) \
    ({ \
        _type *obj = ucg_malloc(sizeof(_type), #_type); \
        if (obj != NULL) { \
            ucg_status_t status = UCG_CLASS_CONSTRUCT(_type, obj, ##__VA_ARGS__); \
            if (status != UCG_OK) { \
                ucg_free(obj); \
                obj = NULL; \
            } \
        } \
        obj; \
    })

/** Call the destructor of the class type and release the space. */
#define UCG_CLASS_DELETE(_type, _self) \
    do { \
        UCG_CLASS_DESTRUCT(_type, _self); \
        ucg_free(_self); \
    } while(0)

#endif