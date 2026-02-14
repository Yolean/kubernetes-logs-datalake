INSTALL httpfs;
LOAD httpfs;

CREATE SECRET datalake (
    TYPE S3,
    KEY_ID getenv('S3_ACCESS_KEY_ID'),
    SECRET getenv('S3_SECRET_ACCESS_KEY'),
    ENDPOINT getenv('S3_ENDPOINT'),
    REGION getenv('S3_REGION'),
    URL_STYLE getenv('S3_URL_STYLE'),
    USE_SSL false
);

CREATE VIEW logs AS
SELECT
  string_split(filename, '/')[5] AS namespace,
  string_split(filename, '/')[6] AS pod,
  string_split(filename, '/')[7] AS container,
  epoch_ms(time) AT TIME ZONE 'UTC' AS time,
  stream, logtag, message, cluster, filename
FROM read_parquet(
  's3://' || getenv('S3_BUCKET') || '/' || getenv('SUBSET_CLUSTER') || '/' ||
  COALESCE(NULLIF(getenv('SUBSET_NAMESPACE'), ''), '*') || '/**/*.parquet',
  filename=true, hive_partitioning=false
);

INSTALL ui;
LOAD ui;
SELECT * FROM start_ui();
