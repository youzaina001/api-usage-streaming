"""Collection gate for the opt-in performance module.

Mirrors the integration suite's USAGE_TEST_INTEGRATION fixture guard, but at
collection time: with USAGE_TEST_PERF unset or any value other than 1, the
perf test module is not collected at all. Default runs and CI therefore never
collect, import, or run any perf code, and no timing threshold can exist for
them to depend on.
"""
import os

collect_ignore: list[str] = []
if os.environ.get("USAGE_TEST_PERF") != "1":
    collect_ignore = ["test_perf_measurements.py"]
