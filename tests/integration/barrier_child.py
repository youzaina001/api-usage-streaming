"""Run the normal usage-stream CLI with a file barrier at private seams. Test only.

USAGE_TEST_BARRIER_DIR names a private directory and USAGE_TEST_BARRIER_STAGES lists
seams in order. At each listed seam the child writes `<stage>.reached` with the seam
offset and waits for `<stage>.release`. Every wait is bounded so an abandoned child
exits on its own.
"""
import os
import sys
import time
from pathlib import Path

from usage_stream import _seams, cli

WAIT_SECONDS = 60


def main() -> int:
    directory = Path(os.environ["USAGE_TEST_BARRIER_DIR"])
    pending = [stage for stage in os.environ["USAGE_TEST_BARRIER_STAGES"].split(",") if stage]

    def barrier(stage: str, offset: int) -> None:
        if not pending or stage != pending[0]:
            return
        pending.pop(0)
        partial = directory / f"{stage}.reached.tmp"
        partial.write_text(str(offset), encoding="ascii")
        partial.rename(directory / f"{stage}.reached")
        release = directory / f"{stage}.release"
        deadline = time.monotonic() + WAIT_SECONDS
        while not release.exists():
            if time.monotonic() > deadline:
                os._exit(98)
            time.sleep(0.02)

    _seams.install(barrier)
    return cli.main()


if __name__ == "__main__":
    sys.exit(main())
