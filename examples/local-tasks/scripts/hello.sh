#!/bin/sh
# Sample shell task for the flywheel local-tasks example.
#
# Arguments arrive as $1, $2, ...; flywheel captures stdout, stderr, the exit
# code, and timing into the job_runs audit trail, and retries on failure.
set -eu

who="${1:-world}"
echo "hello from shell, ${who} — $(date '+%H:%M:%S')"
