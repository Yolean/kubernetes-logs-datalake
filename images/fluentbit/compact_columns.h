#ifndef FLB_S3_COMPACT_COLUMNS_H
#define FLB_S3_COMPACT_COLUMNS_H
#include <arrow-glib/arrow-glib.h>

/* is_utc: true -> Timestamp(ns, tz="UTC"), false -> Timestamp(ns, tz=NULL) */
GArrowTable *compact_parquet_columns(GArrowTable *table, gboolean is_utc);
#endif
