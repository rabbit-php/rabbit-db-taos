<?php
declare(strict_types=1);

namespace Rabbit\DB\Taos;

use FFI;

/**
 * Class TaosHelper
 * @package Taos
 */
class TaosHelper
{
    private static string $header = <<<HEADER
#ifndef TDENGINE_TAOS_H
#define TDENGINE_TAOS_H

#include <stdint.h>

typedef void    TAOS;
typedef void**  TAOS_ROW;
typedef void    TAOS_RES;
typedef void    TAOS_SUB;
typedef void    TAOS_STREAM;
typedef void    TAOS_STMT;

#define TSDB_DATA_TYPE_NULL       0     // 1 bytes
#define TSDB_DATA_TYPE_BOOL       1     // 1 bytes
#define TSDB_DATA_TYPE_TINYINT    2     // 1 byte
#define TSDB_DATA_TYPE_SMALLINT   3     // 2 bytes
#define TSDB_DATA_TYPE_INT        4     // 4 bytes
#define TSDB_DATA_TYPE_BIGINT     5     // 8 bytes
#define TSDB_DATA_TYPE_FLOAT      6     // 4 bytes
#define TSDB_DATA_TYPE_DOUBLE     7     // 8 bytes
#define TSDB_DATA_TYPE_BINARY     8     // string
#define TSDB_DATA_TYPE_TIMESTAMP  9     // 8 bytes
#define TSDB_DATA_TYPE_NCHAR      10    // unicode string

typedef enum {
  TSDB_OPTION_LOCALE,
  TSDB_OPTION_CHARSET,
  TSDB_OPTION_TIMEZONE,
  TSDB_OPTION_CONFIGDIR,
  TSDB_OPTION_SHELL_ACTIVITY_TIMER,
  TSDB_MAX_OPTIONS
} TSDB_OPTION;

typedef struct taosField {
  char     name[65];
  uint8_t  type;
  int16_t  bytes;
} TAOS_FIELD;

void  taos_init();
void  taos_cleanup();
int   taos_options(TSDB_OPTION option, const void *arg, ...);
TAOS *taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port);
void  taos_close(TAOS *taos);

typedef struct TAOS_BIND {
  int            buffer_type;
  void *         buffer;
  unsigned long  buffer_length;  // unused
  unsigned long *length;
  int *          is_null;
  int            is_unsigned;  // unused
  int *          error;        // unused
} TAOS_BIND;

TAOS_STMT *taos_stmt_init(TAOS *taos);
int        taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length);
int        taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_BIND *bind);
int        taos_stmt_add_batch(TAOS_STMT *stmt);
int        taos_stmt_execute(TAOS_STMT *stmt);
TAOS_RES * taos_stmt_use_result(TAOS_STMT *stmt);
int        taos_stmt_close(TAOS_STMT *stmt);

TAOS_RES *taos_query(TAOS *taos, const char *sql);
TAOS_ROW taos_fetch_row(TAOS_RES *res);
int taos_result_precision(TAOS_RES *res);  // get the time precision of result
void taos_free_result(TAOS_RES *res);
int taos_field_count(TAOS_RES *tres);
int taos_num_fields(TAOS_RES *res);
int taos_affected_rows(TAOS_RES *taos);
TAOS_FIELD *taos_fetch_fields(TAOS_RES *res);
int taos_select_db(TAOS *taos, const char *db);
int taos_print_row(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields);
void taos_stop_query(TAOS_RES *res);

int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows);
int taos_validate_sql(TAOS *taos, const char *sql);

int* taos_fetch_lengths(TAOS_RES *res);

char *taos_get_server_info(TAOS *taos);
char *taos_get_client_info();
char *taos_errstr(TAOS_RES *tres);

int taos_errno(TAOS_RES *tres);

void taos_query_a(TAOS *taos, const char *sql, void (*fp)(void *param, TAOS_RES *, int code), void *param);
void taos_fetch_rows_a(TAOS_RES *res, void (*fp)(void *param, TAOS_RES *, int numOfRows), void *param);
void taos_fetch_row_a(TAOS_RES *res, void (*fp)(void *param, TAOS_RES *, TAOS_ROW row), void *param);

typedef void (*TAOS_SUBSCRIBE_CALLBACK)(TAOS_SUB* tsub, TAOS_RES *res, void* param, int code);
TAOS_SUB *taos_subscribe(TAOS* taos, int restart, const char* topic, const char *sql, TAOS_SUBSCRIBE_CALLBACK fp, void *param, int interval);
TAOS_RES *taos_consume(TAOS_SUB *tsub);
void      taos_unsubscribe(TAOS_SUB *tsub, int keepProgress);

TAOS_STREAM *taos_open_stream(TAOS *taos, const char *sql, void (*fp)(void *param, TAOS_RES *, TAOS_ROW row),
                              int64_t stime, void *param, void (*callback)(void *));
void taos_close_stream(TAOS_STREAM *tstr);

int taos_load_table_info(TAOS *taos, const char* tableNameList);

HEADER;

    private static ?FFI $taos = null;
    private static ?FFI $async = null;

    /**
     * @param string|null $libPath
     * @return FFI
     */
    public static function getTaos(?string $libPath = null): FFI
    {
        if (self::$taos !== null) {
            return self::$taos;
        }
        self::$taos = FFI::cdef(self::$header, $libPath ?? "libtaos.so");
        return self::$taos;
    }

    /**
     * @return FFI
     */
    public static function getAsync(): FFI
    {
        if (self::$async !== null) {
            return self::$async;
        }
        self::$async = FFI::cdef(<<<HEADER
typedef void (*__async_cb_func_t)(void *tres, int code);

typedef struct
{
    __async_cb_func_t callback;
} CB_DATA;

void *init();
HEADER
, __DIR__ . '/libs/async.so');
        return self::$async;
    }

    /**
     * @param int $type
     * @param FFI\CData $row
     * @return string
     */
    public static function getValue(int $type, FFI\CData $row)
    {
        switch ($type) {
            case Schema::TSDB_DATA_TYPE_TINYINT:
                return FFI::cast('int8_t', $row)->cdata;
            case Schema::TSDB_DATA_TYPE_SMALLINT:
                return FFI::cast('int16_t', $row)->cdata;
            case Schema::TSDB_DATA_TYPE_INT:
                return FFI::cast('int32_t', $row)->cdata;
            case Schema::TSDB_DATA_TYPE_BIGINT:
                return FFI::cast('int64_t', $row)->cdata;
            case Schema::TSDB_DATA_TYPE_FLOAT:
                return FFI::cast('float', $row)->cdata;
            case Schema::TSDB_DATA_TYPE_DOUBLE:
                return FFI::cast('double', $row)->cdata;
            case Schema::TSDB_DATA_TYPE_BINARY:
            case Schema::TSDB_DATA_TYPE_NCHAR:
                return FFI::string(FFI::cast('char *', $row))->cdata;
            case Schema::TSDB_DATA_TYPE_TIMESTAMP:
                $value = FFI::cast('int64_t', $row)->cdata;
                $mtimestamp = sprintf("%.3f", $value / 1000);
                [$timestamp, $milliseconds] = explode('.', $mtimestamp);
                return date('Y-m-d H:i:s', (int)$timestamp) . '.' . $milliseconds;
            case Schema::TSDB_DATA_TYPE_BOOL:
                return FFI::cast('bool', $row)->cdata;

        }
    }
}