#!/usr/bin/env bash
set -euo pipefail
pressure_owner_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${SQLPOOL1_ROOT:?set SQLPOOL1_ROOT to task-owned scratch}"
test "$(uname -s)" = Linux
mkdir -p "$SQLPOOL1_ROOT/tmp"
cc -Wall -Wextra -Werror -shared -fPIC "$pressure_owner_dir/tests/fixtures/slow_fsync.c" -ldl -pthread -o "$SQLPOOL1_ROOT/slow-fsync.so"
export TMPDIR="$SQLPOOL1_ROOT/tmp" TMP="$SQLPOOL1_ROOT/tmp" TEMP="$SQLPOOL1_ROOT/tmp"
python3 "$pressure_owner_dir/tests/fixtures/fsync_writer.py" "$SQLPOOL1_ROOT/pressure-writer-$$.bin" &
pressure_writer_pid=$!
trap 'kill "$pressure_writer_pid" 2>/dev/null || true; wait "$pressure_writer_pid" || true' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
export LD_PRELOAD="$SQLPOOL1_ROOT/slow-fsync.so${LD_PRELOAD:+:$LD_PRELOAD}"
"$@"
