/*
 * test_compact_columns.c - Unit test for compact_parquet_columns()
 *
 * Standalone C program compiled and run during Docker build.
 * Tests both arrow-compact (no tz) and parquet-compact (UTC) modes.
 */

#include <arrow-glib/arrow-glib.h>
#include <parquet-glib/parquet-glib.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "compact_columns.h"

#define TEST_JSON \
    "{\"time\":\"2024-01-15T10:30:45.123456789Z\",\"stream\":\"stdout\",\"logtag\":\"F\",\"message\":\"hello\",\"cluster\":\"test\"}\n" \
    "{\"time\":\"2024-01-15T10:30:46.000000000Z\",\"stream\":\"stderr\",\"logtag\":\"P\",\"message\":\"world\",\"cluster\":\"test\"}\n" \
    "{\"time\":\"2024-01-15T10:30:47.999999999Z\",\"stream\":\"stdout\",\"logtag\":\"F\",\"message\":\"again\",\"cluster\":\"test\"}\n"

static int tests_passed = 0;
static int tests_failed = 0;

#define ASSERT_MSG(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL: %s (line %d)\n", msg, __LINE__); \
        tests_failed++; \
    } else { \
        printf("  PASS: %s\n", msg); \
        tests_passed++; \
    } \
} while(0)

static GArrowTable *parse_test_json(void)
{
    GArrowBuffer *buffer;
    GArrowBufferInputStream *input;
    GArrowJSONReadOptions *options;
    GArrowJSONReader *reader;
    GArrowTable *table;
    GError *error = NULL;

    const char *json = TEST_JSON;
    buffer = garrow_buffer_new((const guint8 *)json, strlen(json));
    assert(buffer);
    input = garrow_buffer_input_stream_new(buffer);
    assert(input);
    options = garrow_json_read_options_new();
    assert(options);
    reader = garrow_json_reader_new(GARROW_INPUT_STREAM(input), options, &error);
    if (!reader) {
        fprintf(stderr, "Failed to create JSON reader: %s\n", error->message);
        exit(1);
    }
    table = garrow_json_reader_read(reader, &error);
    if (!table) {
        fprintf(stderr, "Failed to read JSON: %s\n", error->message);
        exit(1);
    }
    g_object_unref(reader);
    g_object_unref(options);
    g_object_unref(input);
    g_object_unref(buffer);
    return table;
}

static GArrowDataType *get_column_type(GArrowTable *table, const char *name)
{
    GArrowSchema *schema = garrow_table_get_schema(table);
    int idx = garrow_schema_get_field_index(schema, name);
    if (idx < 0) {
        g_object_unref(schema);
        return NULL;
    }
    GArrowField *field = garrow_schema_get_field(schema, idx);
    GArrowDataType *type = garrow_field_get_data_type(field);
    /* garrow_field_get_data_type is transfer-none; ref before releasing field */
    if (type) g_object_ref(type);
    g_object_unref(field);
    g_object_unref(schema);
    return type;
}

static gboolean write_and_read_parquet(GArrowTable *table, const char *path,
                                       GArrowTable **out_table)
{
    GError *error = NULL;
    GArrowSchema *schema;
    GParquetArrowFileWriter *writer;
    GParquetArrowFileReader *reader;
    gboolean success;

    /* Write */
    GArrowFileOutputStream *file_out =
        garrow_file_output_stream_new(path, FALSE, &error);
    if (!file_out) {
        fprintf(stderr, "Failed to open %s for writing: %s\n", path, error->message);
        g_error_free(error);
        return FALSE;
    }

    schema = garrow_table_get_schema(table);
    writer = gparquet_arrow_file_writer_new_arrow(
        schema, GARROW_OUTPUT_STREAM(file_out), NULL, &error);
    g_object_unref(schema);
    if (!writer) {
        fprintf(stderr, "Failed to create writer: %s\n", error->message);
        g_error_free(error);
        g_object_unref(file_out);
        return FALSE;
    }

    gint64 n_rows = garrow_table_get_n_rows(table);
    success = gparquet_arrow_file_writer_write_table(writer, table, n_rows, &error);
    if (!success) {
        fprintf(stderr, "Failed to write table: %s\n", error->message);
        g_error_free(error);
        g_object_unref(writer);
        g_object_unref(file_out);
        return FALSE;
    }

    success = gparquet_arrow_file_writer_close(writer, &error);
    g_object_unref(writer);
    g_object_unref(file_out);
    if (!success) {
        fprintf(stderr, "Failed to close writer: %s\n", error->message);
        g_error_free(error);
        return FALSE;
    }

    /* Read back */
    reader = gparquet_arrow_file_reader_new_path(path, &error);
    if (!reader) {
        fprintf(stderr, "Failed to open %s for reading: %s\n", path, error->message);
        g_error_free(error);
        return FALSE;
    }

    *out_table = gparquet_arrow_file_reader_read_table(reader, &error);
    g_object_unref(reader);
    if (!*out_table) {
        fprintf(stderr, "Failed to read table: %s\n", error->message);
        g_error_free(error);
        return FALSE;
    }

    return TRUE;
}

static void test_arrow_compact(void)
{
    printf("\n--- Test: arrow-compact (no timezone) ---\n");
    GArrowTable *table = parse_test_json();
    GArrowTable *compacted = compact_parquet_columns(table, FALSE);
    g_object_unref(table);

    ASSERT_MSG(compacted != NULL, "compact_parquet_columns returned non-NULL");

    /* Check time column type */
    GArrowDataType *time_type = get_column_type(compacted, "time");
    ASSERT_MSG(time_type != NULL, "time column exists");
    ASSERT_MSG(GARROW_IS_TIMESTAMP_DATA_TYPE(time_type),
               "time is GArrowTimestampDataType");

    if (GARROW_IS_TIMESTAMP_DATA_TYPE(time_type)) {
        GArrowTimestampDataType *ts =
            GARROW_TIMESTAMP_DATA_TYPE(time_type);
        GArrowTimeUnit unit =
            garrow_timestamp_data_type_get_unit(ts);
        ASSERT_MSG(unit == GARROW_TIME_UNIT_NANO,
                   "time unit is NANO");
    }
    if (time_type) g_object_unref(time_type);

    /* Check stream is dictionary-encoded */
    GArrowDataType *stream_type = get_column_type(compacted, "stream");
    ASSERT_MSG(stream_type != NULL, "stream column exists");
    ASSERT_MSG(GARROW_IS_DICTIONARY_DATA_TYPE(stream_type),
               "stream is dictionary-encoded");
    if (stream_type) g_object_unref(stream_type);

    /* Check logtag is dictionary-encoded */
    GArrowDataType *logtag_type = get_column_type(compacted, "logtag");
    ASSERT_MSG(logtag_type != NULL, "logtag column exists");
    ASSERT_MSG(GARROW_IS_DICTIONARY_DATA_TYPE(logtag_type),
               "logtag is dictionary-encoded");
    if (logtag_type) g_object_unref(logtag_type);

    /* Write to parquet and read back */
    GArrowTable *readback = NULL;
    gboolean ok = write_and_read_parquet(compacted, "/tmp/test_arrow_compact.parquet",
                                         &readback);
    ASSERT_MSG(ok, "parquet write+read round-trip succeeded");

    if (readback) {
        ASSERT_MSG(garrow_table_get_n_rows(readback) == 3,
                   "round-trip preserved 3 rows");

        /* Verify time type persists through parquet */
        GArrowDataType *rt_time = get_column_type(readback, "time");
        ASSERT_MSG(rt_time != NULL, "time column exists after round-trip");
        ASSERT_MSG(GARROW_IS_TIMESTAMP_DATA_TYPE(rt_time),
                   "time is still timestamp after round-trip");
        if (rt_time) g_object_unref(rt_time);

        g_object_unref(readback);
    }

    g_object_unref(compacted);
}

static void test_parquet_compact(void)
{
    printf("\n--- Test: parquet-compact (UTC timezone) ---\n");
    GArrowTable *table = parse_test_json();
    GArrowTable *compacted = compact_parquet_columns(table, TRUE);
    g_object_unref(table);

    ASSERT_MSG(compacted != NULL, "compact_parquet_columns returned non-NULL");

    /* Check time column type */
    GArrowDataType *time_type = get_column_type(compacted, "time");
    ASSERT_MSG(time_type != NULL, "time column exists");
    ASSERT_MSG(GARROW_IS_TIMESTAMP_DATA_TYPE(time_type),
               "time is GArrowTimestampDataType");

    if (GARROW_IS_TIMESTAMP_DATA_TYPE(time_type)) {
        GArrowTimestampDataType *ts =
            GARROW_TIMESTAMP_DATA_TYPE(time_type);
        GArrowTimeUnit unit =
            garrow_timestamp_data_type_get_unit(ts);
        ASSERT_MSG(unit == GARROW_TIME_UNIT_NANO,
                   "time unit is NANO");
    }
    g_object_unref(time_type);

    /* Check stream is dictionary-encoded */
    GArrowDataType *stream_type = get_column_type(compacted, "stream");
    ASSERT_MSG(stream_type != NULL, "stream column exists");
    ASSERT_MSG(GARROW_IS_DICTIONARY_DATA_TYPE(stream_type),
               "stream is dictionary-encoded");
    g_object_unref(stream_type);

    /* Check logtag is dictionary-encoded */
    GArrowDataType *logtag_type = get_column_type(compacted, "logtag");
    ASSERT_MSG(logtag_type != NULL, "logtag column exists");
    ASSERT_MSG(GARROW_IS_DICTIONARY_DATA_TYPE(logtag_type),
               "logtag is dictionary-encoded");
    g_object_unref(logtag_type);

    /* Write to parquet and read back */
    GArrowTable *readback = NULL;
    gboolean ok = write_and_read_parquet(compacted, "/tmp/test_parquet_compact.parquet",
                                         &readback);
    ASSERT_MSG(ok, "parquet write+read round-trip succeeded");

    if (readback) {
        ASSERT_MSG(garrow_table_get_n_rows(readback) == 3,
                   "round-trip preserved 3 rows");

        GArrowDataType *rt_time = get_column_type(readback, "time");
        ASSERT_MSG(rt_time != NULL, "time column exists after round-trip");
        ASSERT_MSG(GARROW_IS_TIMESTAMP_DATA_TYPE(rt_time),
                   "time is still timestamp after round-trip");
        if (rt_time) g_object_unref(rt_time);

        g_object_unref(readback);
    }

    g_object_unref(compacted);
}

static void test_timestamp_values(void)
{
    printf("\n--- Test: timestamp value correctness ---\n");
    GArrowTable *table = parse_test_json();
    GArrowTable *compacted = compact_parquet_columns(table, TRUE);
    g_object_unref(table);

    GArrowSchema *schema = garrow_table_get_schema(compacted);
    int idx = garrow_schema_get_field_index(schema, "time");
    g_object_unref(schema);
    ASSERT_MSG(idx >= 0, "time column found");

    GArrowChunkedArray *chunked = garrow_table_get_column_data(compacted, idx);
    GArrowArray *chunk = garrow_chunked_array_get_chunk(chunked, 0);

    /* First row: 2024-01-15T10:30:45.123456789Z */
    /* Expected: 1705312245 * 1e9 + 123456789 = 1705312245123456789 */
    GArrowTimestampArray *ts_arr = GARROW_TIMESTAMP_ARRAY(chunk);
    gint64 val0 = garrow_timestamp_array_get_value(ts_arr, 0);
    ASSERT_MSG(val0 == 1705314645123456789LL,
               "first timestamp value correct (nanosecond precision)");

    /* Second row: 2024-01-15T10:30:46.000000000Z */
    gint64 val1 = garrow_timestamp_array_get_value(ts_arr, 1);
    ASSERT_MSG(val1 == 1705314646000000000LL,
               "second timestamp value correct (whole seconds)");

    /* Third row: 2024-01-15T10:30:47.999999999Z */
    gint64 val2 = garrow_timestamp_array_get_value(ts_arr, 2);
    ASSERT_MSG(val2 == 1705314647999999999LL,
               "third timestamp value correct (max nanoseconds)");

    g_object_unref(chunk);
    g_object_unref(chunked);
    g_object_unref(compacted);
}

int main(void)
{
    printf("=== compact_columns unit tests ===\n");

    test_arrow_compact();
    test_parquet_compact();
    test_timestamp_values();

    printf("\n=== Results: %d passed, %d failed ===\n",
           tests_passed, tests_failed);

    if (tests_failed > 0) {
        return 1;
    }
    return 0;
}
