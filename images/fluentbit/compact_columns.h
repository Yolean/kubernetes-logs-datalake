#ifndef FLB_S3_COMPACT_COLUMNS_H
#define FLB_S3_COMPACT_COLUMNS_H
#include <arrow-glib/arrow-glib.h>

/* is_utc: true -> Timestamp(ns, tz="UTC"), false -> Timestamp(ns, tz=NULL) */
GArrowTable *compact_parquet_columns(GArrowTable *table, gboolean is_utc);

/* Serialize table to Arrow IPC (Feather v2) without body compression.
 * Uncompressed IPC is readable by nanoarrow/DuckDB (LZ4 default is not). */
GArrowResizableBuffer *table_to_arrow_ipc_buffer(GArrowTable *table);
#endif
