#!/bin/sh -ex
#
# cephfs_mirror_bootstrap.sh - test peer bootstrap
#

RBD_MIRROR_INSTANCES=${RBD_MIRROR_INSTANCES:-1}
. $(dirname $0)/cephfs_mirror_helpers.sh

setup
sleep 30
