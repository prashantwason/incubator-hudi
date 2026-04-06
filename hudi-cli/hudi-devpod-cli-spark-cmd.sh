#!/usr/bin/env bash
set -eou pipefail

DIR="$(dirname "${BASH_SOURCE[0]}")"
source /usr/local/bin/hadoop_lib.sh

setup_tunnel
"$DIR/hudi-cli-spark-cmd.sh" "$@"
