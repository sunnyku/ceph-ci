#!/bin/sh -e

if [ -n "${VALGRIND}" ]; then
  CEPH_ARGS=--debug-rbd=30 valgrind ${VALGRIND} --suppressions=${TESTDIR}/valgrind.supp \
    --error-exitcode=1 ceph_test_librbd --gtest_filter=TestLibRBD.QuiesceWatchError --gtest_repeat=100
else
  CEPH_ARGS=--debug-rbd=30 ceph_test_librbd --gtest_filter=TestLibRBD.QuiesceWatchError --gtest_repeat=400
fi
exit 0
