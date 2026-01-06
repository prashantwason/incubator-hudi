#!/bin/bash
# This file is only to build the entire hoodie oss and check jar validation in every diff
set -x
set -o pipefail
set -e

PROJECT_ROOT="${PROJECT_ROOT:-$(git rev-parse --show-toplevel)}"
chmod -R u+rwX "$PROJECT_ROOT"

source .buildkite/udj/util.sh

echo "Printing m2 contents, presence of existing files and settings.xml indicate that the cache is working"
ls -la ~/.m2

build_hoodie
check_validation
