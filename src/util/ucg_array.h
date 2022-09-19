/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_ARRAY_H_
#define UCG_ARRAY_H_

#define UCG_ARRAY_TYPE(_type) ucg_array_##_type##_t
/* Declaring an array structure of a specific type and its routines. */
#define UCG_ARRAY_DECLARE(_type) \
    typedef struct { \
        int32_t length; \
        int32_t capacity; \
        _type buffer[0]; \
    } UCG_ARRAY_TYPE(_type); \
    \
    static inline ucg_status_t ucg_array_##_type##_init(UCG_ARRAY_TYPE(_type) **array, int32_t capacity) \
    { \
        int32_t size = sizeof(UCG_ARRAY_TYPE(_type)) + capacity * sizeof(_type); \
        UCG_ARRAY_TYPE(_type) *new_array = ucg_malloc(size, "ucg array init"); \
        if (new_array == NULL) { \
            return UCG_ERR_NO_MEMORY; \
        } \
        new_array->length = 0; \
        new_array->capacity = capacity; \
        *array = new_array; \
        return UCG_OK; \
    } \
    \
    static inline ucg_status_t ucg_array_##_type##_extend(UCG_ARRAY_TYPE(_type) **array, int32_t grow) \
    { \
        int32_t new_capacity = (*array)->capacity + grow; \
        int32_t size = sizeof(UCG_ARRAY_TYPE(_type)) + new_capacity * sizeof(_type); \
        UCG_ARRAY_TYPE(_type) *new_array = ucg_realloc(*array, size, "ucg array extend"); \
        if (new_array == NULL) { \
            return UCG_ERR_NO_MEMORY; \
        } \
        new_array->capacity = new_capacity; \
        *array = new_array; \
        return UCG_OK; \
    }

/* Declare external array routines, the capacity and length of the array are recorded externally. */
#define UCG_ARRAYX_DECLARE(_type) \
    static inline ucg_status_t ucg_arrayx_##_type##_init(_type **array, int32_t capacity) \
    { \
        int32_t size = capacity * sizeof(_type); \
        *array = ucg_malloc(size, "ucg arrayx init"); \
        if (*array == NULL) { \
            return UCG_ERR_NO_MEMORY; \
        } \
        return UCG_OK; \
    } \
    \
    static inline ucg_status_t ucg_arrayx_##_type##_extend(_type **array, int32_t *capacity, int32_t grow) \
    { \
        int32_t new_capacity = *capacity + grow; \
        int32_t size = new_capacity * sizeof(_type); \
        _type *new_array = ucg_realloc(*array, size, "ucg arrayx extend"); \
        if (new_array == NULL) { \
            return UCG_ERR_NO_MEMORY; \
        } \
        *capacity = new_capacity; \
        *array = new_array; \
        return UCG_OK; \
    }

/**
 * @brief Initialize array
 *
 * @param [in]  _type       Type of array
 * @param [out] _array      Point to begin address of array
 * @param [in]  _capacity   Capacity of array
 * @return ucg_status_t
 *
 * @note the initialized array can be freed by ucg_free()
 */
#define UCG_ARRAY_INIT(_type, _array, _capacity) \
    ucg_array_##_type##_init(_array, _capacity)

/**
 * @brief Extend array
 *
 * @param [in] _type        Type of array
 * @param [in] _array       Point to begin address of array
 * @param [in] _grow        Extend size
 * @return ucg_status_t
 *
 * @note the extended array can be freed by ucg_free()
 */
#define UCG_ARRAY_EXTEND(_type, _array, _grow) \
    ucg_array_##_type##_extend(_array, _grow)

/**
 * @brief Return length of array.
 */
#define UCG_ARRAY_LENGTH(_array) ((_array)->length)

/**
 * @brief Return capacity of array.
 */
#define UCG_ARRAY_CAPACITY(_array) ((_array)->capacity)

/**
 * @brief Return pointer to the array element.
 */
#define UCG_ARRAY_ELEM(_array, _idx) &((_array)->buffer[_idx])

/**
 * @brief Whether the array is full.
 */
#define UCG_ARRAY_IS_FULL(_array) ((_array)->length == (_array)->capacity)

/**
 * @brief Append element.
 */
#define UCG_ARRAY_APPEND(_array, _elem) ((_array)->buffer[(_array)->length++] = (_elem))

/**
 * @brief Return pointer to the first element.
 */
#define UCG_ARRAY_BEGIN(_array) &((_array)->buffer[0])

/**
 * @brief Return pointer to the end of the array.
 */
#define UCG_ARRAY_END(_array) &((_array)->buffer[(_array)->length])

/**
 * @brief Iterate over array elements
 *
 * @param _elem     Pointer to the current array element.
 * @param _array    Array to iterate over.
 */
#define UCG_ARRAY_FOR_EACH(_elem, _array) \
    for (_elem = UCG_ARRAY_BEGIN(_array); _elem < UCG_ARRAY_END(_array); ++_elem)

/**
 * @brief Initialize externalarray
 *
 * @param [in]  _type       Type of array
 * @param [out] _array      Point to begin address of array
 * @param [in]  _capacity   Capacity of array
 * @return ucg_status_t
 *
 * @note the initialized array can be freed by ucg_free()
 */
#define UCG_ARRAYX_INIT(_type, _array, _capacity) \
    ucg_arrayx_##_type##_init(_array, _capacity)

/**
 * @brief Extend externalarray
 *
 * @param [in]    _type         Type of array
 * @param [in]    _array        Point to begin address of array
 * @param [inout] _capacity     The caller must initialize it to contain the size
 *                              of the array; on return it will contain the actual
 *                              size of the array.
 * @param [in]    _grow         Extend size
 * @return ucg_status_t
 *
 * @note the extended array can be freed by ucg_free()
 */
#define UCG_ARRAYX_EXTEND(_type, _array, _capacity, _grow) \
    ucg_arrayx_##_type##_extend(_array, _capacity, _grow)

#endif