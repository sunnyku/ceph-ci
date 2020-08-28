#!/usr/bin/env bash
#
# Copyright (C) 2020 Red Hat <contact@redhat.com>
#
# Author: David Zafman <dzafman@redhat.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON_A="127.0.0.1:7165" # git grep '\<7165\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7166" # git grep '\<7166\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--mon_health_to_clog_tick_interval=1.0 "
    export ORIG_CEPH_ARGS="$CEPH_ARGS"

    local funcs=${@:-$(set | ${SED} -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        kill_daemons $dir KILL || return 1
        teardown $dir || return 1
    done
}

function TEST_check_version_health_1() {
    local dir=$1

    # Asssume MON_A is leader?
    CEPH_ARGS="$ORIG_CEPH_ARGS --mon-host=$CEPH_MON_A "
    # setup
    setup $dir || return 1

    # create a cluster with one monitor and three osds
    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_mgr $dir x || return 1
    run_mgr $dir y || return 1
    run_mds $dir m || return 1
    run_mds $dir n || return 1

    sleep 5
    ceph health detail
    # should not see this yet
    ceph health detail | grep DAE_OLDER_VERSION && return 1

    ceph tell osd.1 set-test-version 01.00.00-gversion-test
    # XXX: How do we wait for this, and shorten the time required for warning to be presented
    sleep 5

    ceph health detail
    # Should notice that osd.1 is a different version
    ceph health | grep -q "HEALTH_WARN .* There is a daemon running an older version of ceph" || return 1
    ceph health detail | grep -q "HEALTH_WARN .* There is a daemon running an older version of ceph" || return 1
    ceph health detail | grep -q "^[[]WRN[]] DAE_OLDER_VERSION: There is a daemon running an older version of ceph" || return 1
    ceph health detail | grep -q "osd.1 is running an older version of ceph: 01.00.00-gversion-test" || return 1

    ceph tell osd.2 set-test-version 01.00.00-gversion-test
    ceph tell osd.0 set-test-version 02.00.00-gversion-test
    # XXX: How do we wait for this, and shorten the time required for warning to be presented
    sleep 5

    ceph health detail
    ceph health | grep -q "HEALTH_WARN .* There are daemons running older versions of ceph" || return 1
    ceph health detail | grep -q "HEALTH_WARN .* There are daemons running older versions of ceph" || return 1
    ceph health detail | grep -q "^[[]WRN[]] DAE_OLDER_VERSION: There are daemons running older versions of ceph" || return 1
    ceph health detail | grep -q "osd.1 osd.2 are running an older version of ceph: 01.00.00-gversion-test" || return 1
    ceph health detail | grep -q "osd.0 is running an older version of ceph: 02.00.00-gversion-test" || return 1
}

main ver-health "$@"
