"""Versioned, process-local environment for generated reversible programs."""
import locale
import os
import time

EXECUTION_ENVIRONMENT = {"version": 1, "timezone": "UTC0", "lc_time": "C"}


def activate_execution_environment(policy):
    if (not isinstance(policy, dict) or type(policy.get("version")) is not int
            or policy != EXECUTION_ENVIRONMENT):
        raise ValueError("Missing or unsupported execution environment; legacy archives require their legacy decoder and original environment")
    if not hasattr(time, "tzset"):
        raise RuntimeError("This execution policy requires a Unix runtime with time.tzset")
    os.environ["TZ"] = "UTC0"
    os.environ["LC_TIME"] = "C"
    time.tzset()
    locale.setlocale(locale.LC_TIME, "C")
