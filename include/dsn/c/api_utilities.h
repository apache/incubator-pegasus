/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * Description:
 *     useful utilities in rDSN exposed via C API
 *
 * Revision history:
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/c/api_common.h>

#ifdef __cplusplus
extern "C" {
#endif

/*!
@defgroup service-api-utilities Utility Service API
@ingroup service-api

 Commonly used utility API for building distributed systems.
*/

/*!
@defgroup error-t Error Code
@ingroup service-api-utilities

 Error code registration and translation between string and integer.

 See \ref error_code for more details.
 @{
 */

/*!
register error code

\param name error code in string format

\return interger value representing this error.

 For the same input string, rDSN returns the same interger error code.
*/
extern DSN_API dsn_error_t dsn_error_register(const char *name);

/*!
 translate interger error code to a string

 \param err integer error code

 \return string format of the error code
 */
extern DSN_API const char *dsn_error_to_string(dsn_error_t err);

/*!
 parse string error code into integer code

 \param s           the input error in string format
 \param default_err to-be-returned error code if the string is not registered.

 \return integer format of error code
 */
extern DSN_API dsn_error_t dsn_error_from_string(const char *s, dsn_error_t default_err);
/*@}*/

/*!
@defgroup app-cli Command-Line Interface (cli)
@ingroup service-api-utilities

 Built-in command line interface that can be accessed via local/tcp/http consoles.

 @{
 */

/*!
 run a given cli command

 \param command_line given command line.

 \return null if it fails, else execution response
 */
extern DSN_API const char *dsn_cli_run(const char *command_line);

/*!
 free memory occupied by cli response

 \param command_output result from \ref dsn_cli_run
 */
extern DSN_API void dsn_cli_free(const char *command_output);

/*! cli response definition */
typedef struct dsn_cli_reply
{
    const char *message; ///< zero-ended reply message
    uint64_t size;       ///< message_size
    void *context;       ///< context for free_handler
} dsn_cli_reply;

/*! cli request handler definition */
typedef void (*dsn_cli_handler)(void *context,     ///< context registered by \ref dsn_cli_register
                                int argc,          ///< argument count
                                const char **argv, ///< arguments
                                /*out*/ dsn_cli_reply *reply ///< reply message
                                );

/*! cli response resource gc handler definition */
typedef void (*dsn_cli_free_handler)(dsn_cli_reply reply);

/*!
 register a customized cli command handler

 \param command       command name
 \param help_one_line one line help information
 \param help_long     long help information
 \param context       context used by \ref cmd_handler
 \param cmd_handler   command handler
 \param output_freer  command result resource free handler

 \return the handle of this registered command
 */
extern DSN_API dsn_handle_t dsn_cli_register(const char *command,
                                             const char *help_one_line,
                                             const char *help_long,
                                             void *context,
                                             dsn_cli_handler cmd_handler,
                                             dsn_cli_free_handler output_freer);

/*!
 same as \ref dsn_cli_register, except that the command
 name is auto-augmented by rDSN as $app_full_name.$command
 */
extern DSN_API dsn_handle_t dsn_cli_app_register(const char *command,
                                                 const char *help_one_line,
                                                 const char *help_long,
                                                 void *context,
                                                 dsn_cli_handler cmd_handler,
                                                 dsn_cli_free_handler output_freer);

/*!
 remove a cli handler

 \param cli_handle handle of the cli returned from \ref dsn_cli_register or \ref
 dsn_cli_app_register
 */
extern DSN_API void dsn_cli_deregister(dsn_handle_t cli_handle);
/*@}*/

/*!
@defgroup config Configuration Service
@ingroup service-api-utilities

 Configuration Service (e.g., config.ini)

@{
*/

extern DSN_API const char *
dsn_config_get_value_string(const char *section,       ///< [section]
                            const char *key,           ///< key = value
                            const char *default_value, ///< if [section] key is not present
                            const char *dsptr          ///< what it is for, as help-info in config
                            );
extern DSN_API bool dsn_config_get_value_bool(const char *section,
                                              const char *key,
                                              bool default_value,
                                              const char *dsptr);
extern DSN_API uint64_t dsn_config_get_value_uint64(const char *section,
                                                    const char *key,
                                                    uint64_t default_value,
                                                    const char *dsptr);
extern DSN_API double dsn_config_get_value_double(const char *section,
                                                  const char *key,
                                                  double default_value,
                                                  const char *dsptr);
// return all section count (may greater than buffer_count)
extern DSN_API int dsn_config_get_all_sections(const char **buffers,
                                               /*inout*/ int *buffer_count);
// return all key count (may greater than buffer_count)
extern DSN_API int dsn_config_get_all_keys(const char *section,
                                           const char **buffers,
                                           /*inout*/ int *buffer_count);
extern DSN_API void dsn_config_dump(const char *file);
/*@}*/

/*!
@defgroup logging Logging Service
@ingroup service-api-utilities

 Logging Service

 Note developers can plug into rDSN their own logging libraryS
 implementation, so as to integrate rDSN logs into
 their own cluster operation systems.
@{
*/

typedef enum dsn_log_level_t {
    LOG_LEVEL_INFORMATION,
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_WARNING,
    LOG_LEVEL_ERROR,
    LOG_LEVEL_FATAL,
    LOG_LEVEL_COUNT,
    LOG_LEVEL_INVALID
} dsn_log_level_t;

// logs with level smaller than this start_level will not be logged
extern DSN_API dsn_log_level_t dsn_log_start_level;
extern DSN_API dsn_log_level_t dsn_log_get_start_level();
extern DSN_API void dsn_log_set_start_level(dsn_log_level_t level);
extern DSN_API void dsn_logv(const char *file,
                             const char *function,
                             const int line,
                             dsn_log_level_t log_level,
                             const char *title,
                             const char *fmt,
                             va_list args);
extern DSN_API void dsn_logf(const char *file,
                             const char *function,
                             const int line,
                             dsn_log_level_t log_level,
                             const char *title,
                             const char *fmt,
                             ...);
extern DSN_API void dsn_log(const char *file,
                            const char *function,
                            const int line,
                            dsn_log_level_t log_level,
                            const char *title);
extern DSN_API void dsn_coredump();

#define dlog(level, title, ...)                                                                    \
    do {                                                                                           \
        if (level >= dsn_log_start_level)                                                          \
            dsn_logf(__FILE__, __FUNCTION__, __LINE__, level, title, __VA_ARGS__);                 \
    } while (false)
#define dinfo(...) dlog(LOG_LEVEL_INFORMATION, __TITLE__, __VA_ARGS__)
#define ddebug(...) dlog(LOG_LEVEL_DEBUG, __TITLE__, __VA_ARGS__)
#define dwarn(...) dlog(LOG_LEVEL_WARNING, __TITLE__, __VA_ARGS__)
#define derror(...) dlog(LOG_LEVEL_ERROR, __TITLE__, __VA_ARGS__)
#define dfatal(...) dlog(LOG_LEVEL_FATAL, __TITLE__, __VA_ARGS__)
#define dassert(x, ...)                                                                            \
    do {                                                                                           \
        if (!(x)) {                                                                                \
            dlog(LOG_LEVEL_FATAL, __FILE__, "assertion expression: " #x);                          \
            dlog(LOG_LEVEL_FATAL, __FILE__, __VA_ARGS__);                                          \
            dsn_coredump();                                                                        \
        }                                                                                          \
    } while (false)

#define dreturn_not_ok_logged(err, ...)                                                            \
    do {                                                                                           \
        if ((err) != dsn::ERR_OK) {                                                                \
            derror(__VA_ARGS__);                                                                   \
            return err;                                                                            \
        }                                                                                          \
    } while (0)

#ifndef NDEBUG
#define dbg_dassert dassert
#else
#define dbg_dassert(x, ...)
#endif

#ifdef DSN_MOCK_TEST
#define mock_private public
#define mock_virtual virtual
#else
#define mock_private private
#define mock_virtual
#endif

/*@}*/

#define dverify(exp)                                                                               \
    if (!(exp))                                                                                    \
    return false
#define dverify_logged(exp, level, ...)                                                            \
    do {                                                                                           \
        if (!(exp)) {                                                                              \
            dlog(level, __TITLE__, __VA_ARGS__);                                                   \
            return false;                                                                          \
        }                                                                                          \
    } while (0)

#define dstop_on_false(exp)                                                                        \
    if (!(exp))                                                                                    \
    return
#define dstop_on_false_logged(exp, level, ...)                                                     \
    do {                                                                                           \
        if (!(exp)) {                                                                              \
            dlog(level, __TITLE__, __VA_ARGS__);                                                   \
            return;                                                                                \
        }                                                                                          \
    } while (0)

/*!
@defgroup checksum Checksum
@ingroup service-api-utilities

Checksum

@{
*/
extern DSN_API uint32_t dsn_crc32_compute(const void *ptr, size_t size, uint32_t init_crc);

//
// Given
//      x_final = dsn_crc32_compute (x_ptr, x_size, x_init);
// and
//      y_final = dsn_crc32_compute (y_ptr, y_size, y_init);
// compute CRC of concatenation of A and B
//      x##y_crc = dsn_crc32_compute (x##y, x_size + y_size, xy_init);
// without touching A and B
//
extern DSN_API uint32_t dsn_crc32_concatenate(uint32_t xy_init,
                                              uint32_t x_init,
                                              uint32_t x_final,
                                              size_t x_size,
                                              uint32_t y_init,
                                              uint32_t y_final,
                                              size_t y_size);

extern DSN_API uint64_t dsn_crc64_compute(const void *ptr, size_t size, uint64_t init_crc);

//
// Given
//      x_final = dsn_crc64_compute (x_ptr, x_size, x_init);
// and
//      y_final = dsn_crc64_compute (y_ptr, y_size, y_init);
// compute CRC of concatenation of A and B
//      x##y_crc = dsn_crc64_compute (x##y, x_size + y_size, xy_init);
// without touching A and B
//

extern DSN_API uint64_t dsn_crc64_concatenate(uint32_t xy_init,
                                              uint64_t x_init,
                                              uint64_t x_final,
                                              size_t x_size,
                                              uint64_t y_init,
                                              uint64_t y_final,
                                              size_t y_size);
/*@}*/

/*!
@defgroup memory Memory Management
@ingroup service-api-utilities

Memory Management

@{
*/

/*! high-performance malloc for transient objects, i.e., their life-time is short */
extern DSN_API void *dsn_transient_malloc(uint32_t size);

/*! high-performance free for transient objects, paired with \ref dsn_transient_malloc */
extern DSN_API void dsn_transient_free(void *ptr);

/*@}*/

#ifdef __cplusplus
}
#endif
