#!/bin/sh -e

if [ -n "${VALGRIND}" ]; then
  CEPH_ARGS=--debug-rbd=30 valgrind ${VALGRIND} --suppressions=${TESTDIR}/valgrind.supp \
    --error-exitcode=1 ceph_test_librbd
else
  CEPH_ARGS=--debug-rbd=30 ceph_test_librbd
fi
exit 0
