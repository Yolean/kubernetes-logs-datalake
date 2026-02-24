/*
 * compact_columns.c - Transform Arrow table columns for compact storage
 *
 * - time: ISO 8601 CRI string -> Timestamp(ns) with optional UTC timezone
 * - stream, logtag: string -> dictionary-encoded
 */

#include "compact_columns.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

/*
 * Parse CRI timestamp "2024-01-15T10:30:45.123456789Z" into nanoseconds
 * since Unix epoch. Returns 0 on success, -1 on failure.
 */
static int parse_cri_timestamp(const char *str, int64_t *nanos_out)
{
    int year, month, day, hour, min, sec;
    long frac_nanos = 0;
    int n;
    struct tm tm_val;
    time_t epoch;
    const char *p;

    n = sscanf(str, "%d-%d-%dT%d:%d:%d", &year, &month, &day, &hour, &min, &sec);
    if (n != 6) {
        return -1;
    }

    /* Parse fractional seconds after the '.' */
    p = strchr(str, '.');
    if (p) {
        p++; /* skip '.' */
        char frac_buf[10] = "000000000";
        int i = 0;
        while (i < 9 && p[i] >= '0' && p[i] <= '9') {
            frac_buf[i] = p[i];
            i++;
        }
        frac_nanos = strtol(frac_buf, NULL, 10);
    }

    memset(&tm_val, 0, sizeof(tm_val));
    tm_val.tm_year = year - 1900;
    tm_val.tm_mon = month - 1;
    tm_val.tm_mday = day;
    tm_val.tm_hour = hour;
    tm_val.tm_min = min;
    tm_val.tm_sec = sec;

    epoch = timegm(&tm_val);
    if (epoch == (time_t)-1) {
        return -1;
    }

    *nanos_out = (int64_t)epoch * 1000000000LL + frac_nanos;
    return 0;
}

/*
 * Replace the "time" column (string) with a Timestamp(ns) column.
 * No timezone annotation — DuckDB reads Timestamp(ns) as TIMESTAMP_NS
 * preserving nanosecond precision. (With isAdjustedToUTC=true, DuckDB maps
 * to TIMESTAMP WITH TIME ZONE which is only microsecond precision.)
 * Returns a new table with the column replaced, or NULL on error.
 */
static GArrowTable *compact_time_column(GArrowTable *table)
{
    GArrowSchema *schema;
    int col_idx;
    GArrowChunkedArray *chunked;
    GArrowTimestampDataType *ts_type;
    GArrowTable *result = NULL;
    GError *error = NULL;

    schema = garrow_table_get_schema(table);
    col_idx = garrow_schema_get_field_index(schema, "time");
    g_object_unref(schema);

    if (col_idx < 0) {
        g_warning("[compact_columns] 'time' column not found, skipping");
        return NULL;
    }

    chunked = garrow_table_get_column_data(table, col_idx);
    if (!chunked) {
        return NULL;
    }

    /* Timestamp(ns) without timezone — both formats store UTC by convention */
    ts_type = garrow_timestamp_data_type_new(GARROW_TIME_UNIT_NANO, NULL);

    /* Process each chunk */
    guint n_chunks = garrow_chunked_array_get_n_chunks(chunked);
    GList *new_chunks = NULL;
    gboolean ok = TRUE;

    for (guint c = 0; c < n_chunks && ok; c++) {
        GArrowArray *chunk = garrow_chunked_array_get_chunk(chunked, c);
        gint64 len = garrow_array_get_length(chunk);

        GArrowTimestampArrayBuilder *builder =
            garrow_timestamp_array_builder_new(ts_type);
        if (!builder) {
            g_warning("[compact_columns] failed to create timestamp builder");
            g_object_unref(chunk);
            ok = FALSE;
            break;
        }

        for (gint64 i = 0; i < len; i++) {
            if (garrow_array_is_null(chunk, i)) {
                garrow_array_builder_append_null(
                    GARROW_ARRAY_BUILDER(builder), &error);
                if (error) { g_error_free(error); error = NULL; }
                continue;
            }

            GArrowStringArray *str_arr = GARROW_STRING_ARRAY(chunk);
            gchar *val = garrow_string_array_get_string(str_arr, i);
            int64_t nanos;

            if (parse_cri_timestamp(val, &nanos) == 0) {
                garrow_timestamp_array_builder_append_value(builder, nanos, &error);
                if (error) {
                    g_warning("[compact_columns] failed to append timestamp: %s",
                             error->message);
                    g_error_free(error);
                    error = NULL;
                }
            } else {
                g_warning("[compact_columns] failed to parse time: %s", val);
                garrow_array_builder_append_null(
                    GARROW_ARRAY_BUILDER(builder), &error);
                if (error) { g_error_free(error); error = NULL; }
            }
            g_free(val);
        }

        GArrowArray *new_arr = garrow_array_builder_finish(
            GARROW_ARRAY_BUILDER(builder), &error);
        if (!new_arr) {
            g_warning("[compact_columns] failed to finish timestamp array: %s",
                     error->message);
            g_error_free(error);
            error = NULL;
            g_object_unref(builder);
            g_object_unref(chunk);
            ok = FALSE;
            break;
        }

        new_chunks = g_list_append(new_chunks, new_arr);
        g_object_unref(builder);
        g_object_unref(chunk);
    }

    if (ok && new_chunks) {
        GArrowChunkedArray *new_chunked = garrow_chunked_array_new(
            new_chunks, &error);
        if (new_chunked) {
            GArrowField *new_field = garrow_field_new("time",
                GARROW_DATA_TYPE(ts_type));
            result = garrow_table_replace_column(table, col_idx,
                new_field, new_chunked, &error);
            if (!result) {
                g_warning("[compact_columns] failed to replace time column: %s",
                         error->message);
                g_error_free(error);
                error = NULL;
            }
            g_object_unref(new_field);
            g_object_unref(new_chunked);
        } else {
            g_warning("[compact_columns] failed to create chunked array: %s",
                     error->message);
            g_error_free(error);
            error = NULL;
        }
    }

    /* Cleanup */
    g_list_free_full(new_chunks, g_object_unref);
    g_object_unref(ts_type);
    g_object_unref(chunked);

    return result;
}

/*
 * Re-index a dictionary-encoded array from int32 to int8 indices.
 * stream/logtag have very low cardinality (2-3 values), so int8 is sufficient
 * and saves 3 bytes per row vs the default int32.
 * Returns a new array, or NULL on error.
 */
static GArrowArray *dict_reindex_int8(GArrowDictionaryArray *dict_arr)
{
    GError *error = NULL;
    GArrowArray *indices = garrow_dictionary_array_get_indices(dict_arr);
    GArrowArray *dictionary = garrow_dictionary_array_get_dictionary(dict_arr);

    GArrowInt8DataType *int8_type = garrow_int8_data_type_new();
    GArrowArray *int8_indices = garrow_array_cast(
        indices, GARROW_DATA_TYPE(int8_type), NULL, &error);
    if (!int8_indices) {
        g_warning("[compact_columns] failed to cast indices to int8: %s",
                 error->message);
        g_error_free(error);
        g_object_unref(int8_type);
        g_object_unref(indices);
        g_object_unref(dictionary);
        return NULL;
    }

    GArrowStringDataType *str_type = garrow_string_data_type_new();
    GArrowDictionaryDataType *new_dict_type = garrow_dictionary_data_type_new(
        GARROW_DATA_TYPE(int8_type), GARROW_DATA_TYPE(str_type), FALSE);

    GArrowDictionaryArray *new_arr = garrow_dictionary_array_new(
        GARROW_DATA_TYPE(new_dict_type), int8_indices, dictionary, &error);

    g_object_unref(new_dict_type);
    g_object_unref(str_type);
    g_object_unref(int8_type);
    g_object_unref(int8_indices);
    g_object_unref(indices);
    g_object_unref(dictionary);

    if (!new_arr) {
        g_warning("[compact_columns] failed to create int8 dictionary array: %s",
                 error->message);
        g_error_free(error);
        return NULL;
    }

    return GARROW_ARRAY(new_arr);
}

/*
 * Dictionary-encode a string column by name, using int8 indices.
 * Returns a new table with the column replaced, or NULL on error.
 */
static GArrowTable *dict_encode_column(GArrowTable *table, const char *col_name)
{
    GArrowSchema *schema;
    int col_idx;
    GArrowChunkedArray *chunked;
    GArrowTable *result = NULL;
    GError *error = NULL;

    schema = garrow_table_get_schema(table);
    col_idx = garrow_schema_get_field_index(schema, col_name);
    g_object_unref(schema);

    if (col_idx < 0) {
        g_warning("[compact_columns] '%s' column not found, skipping", col_name);
        return NULL;
    }

    chunked = garrow_table_get_column_data(table, col_idx);
    if (!chunked) {
        return NULL;
    }

    /* Dictionary-encode each chunk, then re-index to int8 */
    guint n_chunks = garrow_chunked_array_get_n_chunks(chunked);
    GList *new_chunks = NULL;
    gboolean ok = TRUE;

    for (guint c = 0; c < n_chunks && ok; c++) {
        GArrowArray *chunk = garrow_chunked_array_get_chunk(chunked, c);
        GArrowDictionaryArray *dict_arr =
            garrow_array_dictionary_encode(chunk, &error);
        if (!dict_arr) {
            g_warning("[compact_columns] failed to dict-encode '%s': %s",
                     col_name, error->message);
            g_error_free(error);
            error = NULL;
            g_object_unref(chunk);
            ok = FALSE;
            break;
        }

        GArrowArray *int8_arr = dict_reindex_int8(dict_arr);
        g_object_unref(dict_arr);
        if (!int8_arr) {
            g_object_unref(chunk);
            ok = FALSE;
            break;
        }

        new_chunks = g_list_append(new_chunks, int8_arr);
        g_object_unref(chunk);
    }

    if (ok && new_chunks) {
        /* Get the data type from the first encoded chunk */
        GArrowArray *first = GARROW_ARRAY(new_chunks->data);
        GArrowDataType *dict_type = garrow_array_get_value_data_type(first);

        GArrowChunkedArray *new_chunked = garrow_chunked_array_new(
            new_chunks, &error);
        if (new_chunked) {
            GArrowField *new_field = garrow_field_new(col_name, dict_type);
            result = garrow_table_replace_column(table, col_idx,
                new_field, new_chunked, &error);
            if (!result) {
                g_warning("[compact_columns] failed to replace '%s' column: %s",
                         col_name, error->message);
                g_error_free(error);
                error = NULL;
            }
            g_object_unref(new_field);
            g_object_unref(new_chunked);
        } else {
            g_warning("[compact_columns] failed to create dict chunked array: %s",
                     error->message);
            g_error_free(error);
            error = NULL;
        }
        g_object_unref(dict_type);
    }

    g_list_free_full(new_chunks, g_object_unref);
    g_object_unref(chunked);

    return result;
}

GArrowTable *compact_parquet_columns(GArrowTable *table)
{
    GArrowTable *current = table;
    GArrowTable *next;
    gboolean owns_current = FALSE;

    /* 1. Compact time column */
    next = compact_time_column(current);
    if (next) {
        if (owns_current) {
            g_object_unref(current);
        }
        current = next;
        owns_current = TRUE;
    }

    /* 2-3. Dictionary-encode stream and logtag.
     * DuckDB/nanoarrow reads these back as plain VARCHAR, but tools using
     * the official Arrow library (e.g. pyarrow) see dictionary type. */
    next = dict_encode_column(current, "stream");
    if (next) {
        if (owns_current) {
            g_object_unref(current);
        }
        current = next;
        owns_current = TRUE;
    }

    next = dict_encode_column(current, "logtag");
    if (next) {
        if (owns_current) {
            g_object_unref(current);
        }
        current = next;
        owns_current = TRUE;
    }

    /* If no transformations succeeded, ref the original so caller can unref */
    if (!owns_current) {
        g_object_ref(current);
    }

    return current;
}

GArrowResizableBuffer *table_to_arrow_ipc_buffer(GArrowTable *table)
{
    GArrowResizableBuffer *buffer;
    GArrowBufferOutputStream *sink;
    GArrowFeatherWriteProperties *props;
    GError *error = NULL;
    gboolean success;

    buffer = garrow_resizable_buffer_new(0, &error);
    if (!buffer) {
        g_warning("[compact_columns] failed to create buffer: %s",
                 error->message);
        g_error_free(error);
        return NULL;
    }

    sink = garrow_buffer_output_stream_new(buffer);
    if (!sink) {
        g_object_unref(buffer);
        return NULL;
    }

    /* ZSTD: nanoarrow/DuckDB supports ZSTD but not LZ4 for Arrow IPC bodies */
    props = garrow_feather_write_properties_new();
    g_object_set(props, "compression",
                 GARROW_COMPRESSION_TYPE_ZSTD, NULL);

    success = garrow_table_write_as_feather(
        table, GARROW_OUTPUT_STREAM(sink), props, &error);
    g_object_unref(props);
    g_object_unref(sink);

    if (!success) {
        g_warning("[compact_columns] failed to write feather: %s",
                 error->message);
        g_error_free(error);
        g_object_unref(buffer);
        return NULL;
    }

    return buffer;
}
