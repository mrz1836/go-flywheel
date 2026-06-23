#!/usr/bin/env python3
"""Sample Python task for the flywheel local-tasks example.

Arguments arrive in sys.argv; flywheel captures stdout, stderr, the exit code,
and timing into the job_runs audit trail, and retries on failure.
"""
import sys
from datetime import datetime

who = sys.argv[1] if len(sys.argv) > 1 else "world"
print(f"hello from python, {who} — {datetime.now():%H:%M:%S}")
