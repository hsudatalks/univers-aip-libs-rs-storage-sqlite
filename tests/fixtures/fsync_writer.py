"""A bounded, task-owned concurrent durable writer for the Linux pressure gate."""
import json
import os
from pathlib import Path
import signal
import sys
import time

path = Path(sys.argv[1])
running = True


def stop(_signal, _frame):
    global running
    running = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
block = b"sqlpool1" * 8192  # 64 KiB, cycling over at most 16 MiB.
count = 0
started = time.monotonic()
with path.open("wb", buffering=0) as output:
    while running:
        output.seek((count % 256) * len(block))
        output.write(block)
        os.fdatasync(output.fileno())
        count += 1
        time.sleep(0.01)
path.with_suffix(".json").write_text(
    json.dumps({"fsyncs": count, "bytes_written": count * len(block),
                "elapsed_seconds": round(time.monotonic() - started, 2)}) + "\n"
)
path.unlink()
