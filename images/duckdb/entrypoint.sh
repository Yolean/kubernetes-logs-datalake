#!/bin/sh
set -e

cat > /tmp/init.sql << 'SQLINIT'
INSTALL ui;
LOAD ui;
SELECT * FROM start_ui();
SQLINIT

# Keep stdin open so duckdb doesn't exit
tail -f /dev/null | duckdb -init /tmp/init.sql
