#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=util.sh
source "${DIR}/util.sh"

awk '{ print "vitor@"$4 }' "${DIR}/emulab/machines" >"${MACHINES_FILE}"