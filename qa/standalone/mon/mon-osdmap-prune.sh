#!/bin/bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {

  local dir=$1
  shift

  export CEPH_MON="127.0.0.1:7115"
  export CEPH_ARGS
  CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none --mon-host=$CEPH_MON "

  local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
  for func in $funcs; do
    setup $dir || return 1
    $func $dir || return 1
    teardown $dir || return 1
  done
}


function wait_for_osdmap_manifest() {

  local what=${1:-"true"}

  local -a delays=($(get_timeout_delays $TIMEOUT .1))
  local -i loop=0

  for ((i=0; i < ${#delays[*]}; ++i)); do
    has_manifest=$(ceph report | jq 'has("osdmap_manifest")')
    if [[ "$has_manifest" == "$what" ]]; then
      return 0
    fi

    sleep ${delays[$i]}
  done

  echo "osdmap_manifest never outputted on report"
  ceph report
  return 1
}

function TEST_osdmap_prune() {

  local dir=$1

  run_mon $dir a || return 1
  run_mgr $dir x || return 1
  run_osd $dir 0 || return 1
  run_osd $dir 1 || return 1
  run_osd $dir 2 || return 1

  sleep 5

  # we are getting OSD_OUT_OF_ORDER_FULL health errors, and it's not clear
  # why. so, to make the health checks happy, mask those errors.
  ceph osd set-full-ratio 0.97
  ceph osd set-backfillfull-ratio 0.97

  ceph tell osd.* injectargs '--osd-beacon-report-interval 10' || return 1

  create_pool foo 32
  wait_for_clean || return 1
  wait_for_health_ok || return 1

  ceph tell mon.a injectargs \
    '--mon-debug-block-osdmap-trim '\
    '--mon-debug-extra-checks' || return 1

  ceph daemon $(get_asok_path mon.a) test generate osdmap 1000 || return 1

  report="$(ceph report)"
  fc=$(jq '.first_committed' <<< $report)
  lc=$(jq '.last_committed' <<< $report)

  [[ $((lc-fc)) -ge 1000 ]] || return 1

  ceph tell mon.a injectargs \
    '--mon-osdmap-full-prune-enabled '\
    '--mon-osdmap-full-prune-min 200 '\
    '--mon-osdmap-full-prune-interval 10 '\
    '--mon-osdmap-full-prune-txsize 100' || return 1

  wait_for_osdmap_manifest || return 1

  manifest="$(ceph report | jq '.osdmap_manifest')"

  first_pinned=$(jq '.first_pinned' << $manifest)
  last_pinned=$(jq '.last_pinned' <<< $manifest)
  last_pruned=$(jq '.last_pruned' <<< $manifest)
  pinned_maps=( $(jq '.pinned_maps' <<< $manifest | tr '\",' ' ') ) 

  [[ $first_pinned -lt $last_pinned ]] || return 1
  [[ $last_pruned -lt $last_pinned ]] || return 1
  [[ $last_pruned -ge 0 ]] || return 1
  [[ $last_pinned -lt $lc ]] || return 1
  [[ $first_pinned -eq $fc ]] || return 1

  # ensure all the maps are available, and work as expected
  # this can take a while...

  tmp_map=$(mktemp)
  for ((i=$first_pinned; i <= $last_pinned; ++i)); do
    ceph osd getmap -i $i -o $tmp_map || return 1
    if ! osdmaptool --print $tmp_map | grep "epoch $i" ; then
      echo "failed processing osdmap epoch $i"
      return 1
    fi
  done
  rm $tmp_map

  # clean up maps
  ceph tell mon.a injectargs \
    '--mon-debug-block-osdmap-trim=false '\
    '--paxos-service-trim-min=1'

  wait_for_osdmap_manifest "false" || return 1

  return 0
}

main mon-osdmap-prune "$@"

exit 0

ceph tell mon.a injectargs \
  "--mon-debug-block-osdmap-trim " \
  "--mon-debug-extra-checks" || exit 1

ceph daemon mon.a test generate osdmap 1000 || exit 1

init-ceph restart osd

ceph tell osd.* injectargs \
  "--osd-beacon-report-interval 10"

ceph report | \
  jq '{ "fc": .osdmap_first_committed, "lc": .osdmap_last_committed }'

ceph tell mon.a injectargs \
  "--mon-osdmap-full-prune-enabled "\
  "--mon-osdmap-full-prune-min 200 "\
  "--mon-osdmap-full-prune-interval 10 "\
  "--mon-osdmap-full-prune-txsize 100 " || exit 1

for ((i=0; i < 120; ++i)); do

  ceph report | \
    jq '{ "fc": .osdmap_first_committed, "lc": .osdmap_last_committed }'

  has_manifest=$(ceph report | jq 'has("osdmap_manifest")')
  if [[ "$has_manifest" == "true" ]]; then
    ceph report | \
      jq '{ "prune": .osdmap_manifest }'

    break

  else
    echo "no manifest found :("
  fi

  sleep 2
done

ceph tell mon.a injectargs \
  "--mon-debug-block-osdmap-trim=false"

# 
# report=$(ceph report 2>/dev/null)
# first_v=$(echo $report | jq '.osdmap_first_committed')
# last_v=$(echo $report | jq '.osdmap_last_committed')
# 
# has_manifest=$(echo $report | jq 'has("osdmap_manifest")')
# [[ "$has_manifest" == "true" ]] && echo "unexpected manifest found!" && exit 1
# 
# ceph daemon mon.a osdmon_generate_maps 1000
# report="$(ceph daemon mon.a report)"

# how do we make a pg unclean?
