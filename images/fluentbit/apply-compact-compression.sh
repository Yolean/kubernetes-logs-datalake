#!/bin/bash
set -ex

# Apply compact-compression changes to fluent-bit source tree
# This script runs from the fluent-bit repo root

# 1. flb_aws_compress.h — add new compression type constants
sed -i '/#define FLB_AWS_COMPRESS_ZSTD/a\
#define FLB_AWS_COMPRESS_ARROW_COMPACT   5\
#define FLB_AWS_COMPRESS_PARQUET_COMPACT 6' \
  include/fluent-bit/aws/flb_aws_compress.h

# 2. compress.h — declare new functions at end of file
cat >> src/aws/compression/arrow/compress.h << 'EOF'

#ifdef FLB_HAVE_ARROW_PARQUET
int out_s3_compress_arrow_compact(void *json, size_t size, void **out_buf, size_t *out_size);
int out_s3_compress_parquet_compact(void *json, size_t size, void **out_buf, size_t *out_size);
#endif
EOF

# 3. compress.c — add #include for compact_columns.h after parquet-glib include
sed -i '/#include <parquet-glib\/parquet-glib.h>/a\
#include "compact_columns.h"' \
  src/aws/compression/arrow/compress.c

# 4. compress.c — add new functions before final #endif
#    The last line of the file is "#endif" (closing FLB_HAVE_ARROW_PARQUET)
#    Insert the new functions before it
sed -i '$ i\
\
int out_s3_compress_arrow_compact(void *json, size_t size, void **out_buf, size_t *out_size)\
{\
        GArrowTable *table;\
        GArrowTable *compacted;\
        GArrowResizableBuffer *buffer;\
        GBytes *bytes;\
        gconstpointer ptr;\
        gsize len;\
        uint8_t *buf;\
\
        table = parse_json((uint8_t *) json, size);\
        if (table == NULL) {\
            flb_error("[aws][compress] Failed to parse JSON for arrow-compact");\
            return -1;\
        }\
\
        compacted = compact_parquet_columns(table);\
        g_object_unref(table);\
\
        buffer = table_to_arrow_ipc_buffer(compacted);\
        g_object_unref(compacted);\
        if (buffer == NULL) {\
            flb_error("[aws][compress] Failed to convert compacted table to arrow IPC buffer (arrow-compact)");\
            return -1;\
        }\
\
        bytes = garrow_buffer_get_data(GARROW_BUFFER(buffer));\
        if (bytes == NULL) {\
            g_object_unref(buffer);\
            return -1;\
        }\
\
        ptr = g_bytes_get_data(bytes, &len);\
        if (ptr == NULL) {\
            g_object_unref(buffer);\
            g_bytes_unref(bytes);\
            return -1;\
        }\
\
        buf = flb_malloc(len);\
        if (buf == NULL) {\
            flb_errno();\
            g_object_unref(buffer);\
            g_bytes_unref(bytes);\
            return -1;\
        }\
        memcpy(buf, ptr, len);\
        *out_buf = (void *) buf;\
        *out_size = len;\
\
        g_object_unref(buffer);\
        g_bytes_unref(bytes);\
        return 0;\
}\
\
int out_s3_compress_parquet_compact(void *json, size_t size, void **out_buf, size_t *out_size)\
{\
        GArrowTable *table;\
        GArrowTable *compacted;\
        GArrowResizableBuffer *buffer;\
        GBytes *bytes;\
        gconstpointer ptr;\
        gsize len;\
        uint8_t *buf;\
\
        table = parse_json((uint8_t *) json, size);\
        if (table == NULL) {\
            flb_error("[aws][compress] Failed to parse JSON for parquet-compact");\
            return -1;\
        }\
\
        compacted = compact_parquet_columns(table);\
        g_object_unref(table);\
\
        buffer = table_to_parquet_buffer(compacted);\
        g_object_unref(compacted);\
        if (buffer == NULL) {\
            flb_error("[aws][compress] Failed to convert compacted table to parquet buffer (parquet-compact)");\
            return -1;\
        }\
\
        bytes = garrow_buffer_get_data(GARROW_BUFFER(buffer));\
        if (bytes == NULL) {\
            g_object_unref(buffer);\
            return -1;\
        }\
\
        ptr = g_bytes_get_data(bytes, &len);\
        if (ptr == NULL) {\
            g_object_unref(buffer);\
            g_bytes_unref(bytes);\
            return -1;\
        }\
\
        buf = flb_malloc(len);\
        if (buf == NULL) {\
            flb_errno();\
            g_object_unref(buffer);\
            g_bytes_unref(bytes);\
            return -1;\
        }\
        memcpy(buf, ptr, len);\
        *out_buf = (void *) buf;\
        *out_size = len;\
\
        g_object_unref(buffer);\
        g_bytes_unref(bytes);\
        return 0;\
}' \
  src/aws/compression/arrow/compress.c

# 5. CMakeLists.txt — add compact_columns.c to source list
sed -i 's/    compress.c)/    compress.c\n    compact_columns.c)/' \
  src/aws/compression/arrow/CMakeLists.txt

# 6. flb_aws_compress.c — add new entries to compression_options array
#    Insert after the parquet entry (before #endif)
sed -i '/&out_s3_compress_parquet/,/},/ {
  /},/ a\
    {\
        FLB_AWS_COMPRESS_ARROW_COMPACT,\
        "arrow-compact",\
        \&out_s3_compress_arrow_compact\
    },\
    {\
        FLB_AWS_COMPRESS_PARQUET_COMPACT,\
        "parquet-compact",\
        \&out_s3_compress_parquet_compact\
    },
}' \
  src/aws/flb_aws_compress.c

# 7. s3.c — add new compression types to use_put_object validation
sed -i 's/ret == FLB_AWS_COMPRESS_PARQUET)) {/ret == FLB_AWS_COMPRESS_PARQUET ||\n         ret == FLB_AWS_COMPRESS_ARROW_COMPACT ||\n         ret == FLB_AWS_COMPRESS_PARQUET_COMPACT)) {/' \
  plugins/out_s3/s3.c

echo "=== compact-compression changes applied ==="
