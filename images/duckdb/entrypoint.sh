#!/bin/sh
set -e

if [ -f /etc/duckdb/init.sql ]; then
  INIT=/etc/duckdb/init.sql
else
  cat > /tmp/init.sql << 'SQLINIT'
INSTALL ui;
LOAD ui;
SELECT * FROM start_ui();
SQLINIT
  INIT=/tmp/init.sql
fi

# Keep stdin open so duckdb doesn't exit
tail -f /dev/null | duckdb -init "$INIT"
