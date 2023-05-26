#!/bin/bash

if [ $# -lt 1 ]; then
    cat <<EOF
usage $0 <load: false / true>
like:
    $0 true   ## load exec first, then do cnt and new plan
    $0 false  ## just do cnt and new plan
EOF
    exit 1
fi


set -x
go build && GODEBUG=gctrace=2 ./debug $@ 2>gc.log
