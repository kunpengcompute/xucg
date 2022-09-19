/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_HASH_H_
#define UCG_HASH_H_

#include <ucs/datastruct/khash.h>

/* define a hash map which key is int/int64/const char* and value is _valtype */
#define UCG_HASH_MAP_INIT_INT(_name, _valtype)   KHASH_MAP_INIT_INT(_name, _valtype)
#define UCG_HASH_MAP_INIT_INT64(_name, _valtype) KHASH_MAP_INIT_INT64(_name, _valtype)
#define UCG_HASH_MAP_INIT_STR(_name, _valtype)   KHASH_MAP_INIT_STR(_name, _valtype)

/* define a hash set which key is int/int64/const char* */
#define UCG_HASH_SET_INIT_INT(_name)   KHASH_SET_INIT_INT(_name)
#define UCG_HASH_SET_INIT_INT64(_name) KHASH_SET_INIT_INT64(_name)
#define UCG_HASH_SET_INIT_STR(_name)   KHASH_SET_INIT_STR(_name)

/** @brief the type of hash table, @a _name should same with UCG_HASH_XXX_INIT_XXX */
#define ucg_hash_t(_name)    khash_t(_name)
/** @brief the iterator of the hash table */
#define ucg_hiter_t          khiter_t

/** @brief init a hash table with malloc ucg_hash_t */
#define ucg_hash_init(_name) kh_init(_name)
/** @brief cleanup a hash table created by ucg_hash_init() */
#define ucg_hash_cleanup(_name, _h) kh_destroy(_name, _h)
/** @brief init the hash table @a _h */
#define ucg_hash_init_inplace(_name, _h) kh_init_inplace(_name, _h)
/** @brief  cleanup a hash table init by ucg_hash_init_inplace()*/
#define ucg_hash_cleanup_inplace(_name, _h) kh_destroy_inplace(_name, _h)

/** @brief reset the hash table without deallocating memory */
#define ucg_hash_clear(_name, _h) kh_clear(_name, _h)
/** @brief resize the hash table */
#define ucg_hash_resize(_name, _h, _s) kh_resize(_name, _h, _s)

typedef enum ucg_hash_put_status {
    UCG_HASH_PUT_FAILED       = -1,
    UCG_HASH_PUT_KEY_PRESENT  = 0,
    UCG_HASH_PUT_BUCKET_EMPTY = 1,
    UCG_HASH_PUT_BUCKET_CLEAR = 2,
} ucg_hash_put_status_t;

/**
 * @brief Put the key into the hash table @a _h
 *
 * @param [in]  _name   the name of hash table, should keep same with UCG_HASH_XXX_INIT_XX [symbol]
 * @param [in]  _h      the pointer to the hash table [ucg_hash_t]
 * @param [in]  _key    the key you want put to hash table
 * @param [out] _result the return code [int *] [ucg_hash_put_status_t]
 *                      -1 if failed;
 *                      0 if the key is present in the hash table;
 *                      1 if the bucket is empty (never used);
 *                      2 if the element in the bucket has been deleted
 * @return the iterator to the inserted element [ucg_hiter_t *]
 */
#define ucg_hash_put(_name, _h, _key, _result) kh_put(_name, _h, _key, _result)

/**
 * @brief Get the iterator of key from the hash table
 * @return the iterator for found or ucg_hash_end() for not found
 */
#define ucg_hash_get(_name, _h, _key) kh_get(_name, _h, _key)

/**
 * @brief Get the iterator of key from the hash table
 * @return the iterator to the element for deleted
 */
#define ucg_hash_del(_name, _h, _key) kh_del(_name, _h, _key)

/** @brief Get the key from an iterator */
#define ucg_hash_key(_h, _iter) kh_key(_h, _iter)
/** @brief Get the value from an iterator */
#define ucg_hash_value(_h, _iter) kh_val(_h, _iter)

/** @brief The begin/end iterator of the hash table */
#define ucg_hash_begin(_h) kh_begin(_h)
#define ucg_hash_end(_h) kh_end(_h)
#define ucg_hash_size(_h) kh_size(_h)

/**
 * @brief check whether the iterator has valid data
 *
 * @param [in] _h       the hash table
 * @param [in] _iter    the iterator
 * @return 1 for valid data; 0 for empty
 */
#define ucg_hash_exist(_h, _iter) kh_exist(_h, _iter)

/**
 * @brief Iterate over the hash table and get the valid key and value and do code
 *
 * @param [in]  _h      the hash table
 * @param [out] _kvar   the key variable which will be assigned
 * @param [out] _vvar   the value variable which will be assigned
 * @param [in]  _code   the code need be executed
 */
#define ucg_hash_foreach(_h, _kvar, _vvar, _code) kh_foreach(_h, _kvar, _vvar, _code)
#define ucg_hash_foreach_key(_h, _kvar, _code) kh_foreach_key(_h, _kvar, _code)
#define ucg_hash_foreach_value(_h, _vvar, _code) kh_foreach_value(_h, _vvar, _code)

#endif