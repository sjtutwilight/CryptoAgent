#!/usr/bin/env bash
set -euo pipefail

cat <<'EOF'
init:data is not configured yet.
Add a concrete data initialization script (for example, Spark import + MinIO load),
or skip data init with init:all --skip-data.
EOF
exit 1
