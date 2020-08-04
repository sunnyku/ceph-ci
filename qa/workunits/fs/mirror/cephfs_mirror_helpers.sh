#!/bin/sh

CEPHFS_MIRROR_INSTANCES=${CEPHFS_MIRROR_INSTANCES:-2}

CLUSTER1=cluster1
CLUSTER2=cluster2
TEMPDIR=
CEPH_ID=${CEPH_ID:-mirror}
MIRROR_USER_ID_PREFIX=${MIRROR_USER_ID_PREFIX:-${CEPH_ID}.}

export CEPH_ARGS="--id ${CEPH_ID}"

LAST_MIRROR_INSTANCE=$((${CEPHFS_MIRROR_INSTANCES} - 1))

CEPH_ROOT=$(readlink -f $(dirname $0)/../../../src)
CEPH_BIN=.
CEPH_SRC=.
if [ -e CMakeCache.txt ]; then
    CEPH_SRC=${CEPH_ROOT}
    CEPH_ROOT=${PWD}
    CEPH_BIN=./bin

    # needed for ceph CLI under cmake
    export LD_LIBRARY_PATH=${CEPH_ROOT}/lib:${LD_LIBRARY_PATH}
    export PYTHONPATH=${PYTHONPATH}:${CEPH_SRC}/pybind:${CEPH_ROOT}/lib/cython_modules/lib.3
fi

if type xmlstarlet > /dev/null 2>&1; then
    XMLSTARLET=xmlstarlet
elif type xml > /dev/null 2>&1; then
    XMLSTARLET=xml
else
    echo "Missing xmlstarlet binary!"
    exit 1
fi

create_users()
{
    local cluster=$1

    CEPH_ARGS='' ceph --cluster "${cluster}" \
        auth get-or-create client.${CEPH_ID} \
        mon 'allow r' mds 'allow r' osd 'allow r' \
        osd 'allow rw object_prefix cephfs_mirror' \
        mgr 'allow r' >> ${CEPH_ROOT}/run/${cluster}/keyring
    for instance in `seq 0 ${LAST_MIRROR_INSTANCE}`; do
        CEPH_ARGS='' ceph --cluster "${cluster}" \
                 auth get-or-create client.${MIRROR_USER_ID_PREFIX}${instance} \
                 mon 'allow r' mds 'allow r' osd 'allow r' \
                 osd 'allow rw object_prefix cephfs_mirror' \
                 mgr 'allow r' >> ${CEPH_ROOT}/run/${cluster}/keyring
    done
}

daemon_asok_file()
{
    local local_cluster=$1
    local cluster=$2
    local instance

    set_cluster_instance "${local_cluster}" local_cluster instance

    echo $(ceph-conf --cluster $local_cluster --name "client.${MIRROR_USER_ID_PREFIX}${instance}" 'admin socket')
}

daemon_pid_file()
{
    local cluster=$1
    local instance

    set_cluster_instance "${cluster}" cluster instance

    echo $(ceph-conf --cluster $cluster --name "client.${MIRROR_USER_ID_PREFIX}${instance}" 'pid file')
}

setup_cluster()
{
    local cluster=$1

    CEPH_ARGS='' MON=1 MDS=1 MGR=1 OSD=3 ${CEPH_SRC}/mstart.sh ${cluster} -n ${CEPHFS_MIRROR_VARGS}

    cd ${CEPH_ROOT}
    rm -f ${TEMPDIR}/${cluster}.conf
    ln -s $(readlink -f run/${cluster}/ceph.conf) \
       ${TEMPDIR}/${cluster}.conf

    cd ${TEMPDIR}
    create_users "${cluster}"

    for instance in `seq 0 ${LAST_MIRROR_INSTANCE}`; do
        cat<<EOF >> ${TEMPDIR}/${cluster}.conf
[client.${MIRROR_USER_ID_PREFIX}${instance}]
    admin socket = ${TEMPDIR}/cephfs-mirror.\$cluster-\$name.asok
    pid file = ${TEMPDIR}/cephfs-mirror.\$cluster-\$name.pid
    log file = ${TEMPDIR}/cephfs-mirror.${cluster}_daemon.${instance}.log
EOF
    done
}

setup_tempdir()
{
    if [ -n "${CEPHFS_MIRROR_TEMPDIR}" ]; then
        test -d "${CEPHFS_MIRROR_TEMPDIR}" ||
        mkdir "${CEPHFS_MIRROR_TEMPDIR}"
        TEMPDIR="${CEPHFS_MIRROR_TEMPDIR}"
        cd ${TEMPDIR}
    else
        TEMPDIR=`mktemp -d`
    fi
}

cleanup()
{
    local error_code=$1

    set +e

    if [ "${error_code}" -ne 0 ]; then
        status
    fi

    if [ -z "${CEPHFS_MIRROR_NOCLEANUP}" ]; then
        for cluster in "${CLUSTER1}" "${CLUSTER2}"; do
            stop_mirrors "${cluster}"
        done

        if [ -z "${CEPHFS_MIRROR_USE_EXISTING_CLUSTER}" ]; then
            cd ${CEPH_ROOT}
            CEPH_ARGS='' ${CEPH_SRC}/mstop.sh ${CLUSTER1}
            CEPH_ARGS='' ${CEPH_SRC}/mstop.sh ${CLUSTER2}
        fi
        test "${CEPHFS_MIRROR_TEMPDIR}" = "${TEMPDIR}" || rm -Rf ${TEMPDIR}
    fi

    if [ "${error_code}" -eq 0 ]; then
        echo "OK"
    else
        echo "FAIL"
    fi

    exit ${error_code}
}

setup_mirroring()
{
    local cluster=$1
    local remote_cluster=$2
    local mon_map_file
    local mon_addr
    local admin_key_file
    local uuid

    local filesystems=${TEMPDIR}/${cluster}.filesystems

    CEPH_ARGS='' ceph --cluster ${cluster} fs ls --format xml > ${filesystems}
    fs=$($XMLSTARLET sel -t -v "//filesystems/filesystem/name" < ${filesystems})

    CEPH_ARGS='' ceph --cluster ${cluster} mgr module enable mirroring
    CEPH_ARGS='' ceph --cluster ${cluster} fs snapshot mirror enable ${fs}
}

setup()
{
    local c
    trap 'cleanup $?' INT TERM EXIT

    setup_tempdir
    if [ -z "${CEPHFS_MIRROR_USE_EXISTING_CLUSTER}" ]; then
        setup_cluster "${CLUSTER1}"
        setup_cluster "${CLUSTER2}"
    fi

    setup_mirroring "${CLUSTER1}" "${CLUSTER2}"
}

# Parse a value in format cluster[:instance] and set cluster and instance vars.
set_cluster_instance()
{
    local val=$1
    local cluster_var_name=$2
    local instance_var_name=$3

    cluster=${val%:*}
    instance=${val##*:}

    if [ "${instance}" =  "${val}" ]; then
        # instance was not specified, use default
        instance=0
    fi

    eval ${cluster_var_name}=${cluster}
    eval ${instance_var_name}=${instance}
}

start_mirror()
{
    local cluster=$1
    local instance

    set_cluster_instance "${cluster}" cluster instance

    test -n "${CEPHFS_MIRROR_USE_CEPHFS_MIRROR}" && return

    cephfs-mirror \
        --cluster ${cluster} \
        --id ${MIRROR_USER_ID_PREFIX}${instance} \
        --debug-cephfs-mirror=20 \
        --daemonize=true \
        ${CEPHFS_MIRROR_ARGS}
}

start_mirrors()
{
    local cluster=$1

    for instance in `seq 0 ${LAST_MIRROR_INSTANCE}`; do
        start_mirror "${cluster}:${instance}"
    done
}

stop_mirror()
{
    local cluster=$1
    local sig=$2

    test -n "${CEPHFS_MIRROR_USE_CEPHFS_MIRROR}" && return

    local pid
    pid=$(cat $(daemon_pid_file "${cluster}") 2>/dev/null) || :
    if [ -n "${pid}" ]
    then
        kill ${sig} ${pid}
        for s in 1 2 4 8 16 32; do
            sleep $s
            ps auxww | awk -v pid=${pid} '$2 == pid {print; exit 1}' && break
        done
        ps auxww | awk -v pid=${pid} '$2 == pid {print; exit 1}'
    fi
    rm -f $(daemon_asok_file "${cluster}" "${CLUSTER1}")
    rm -f $(daemon_asok_file "${cluster}" "${CLUSTER2}")
    rm -f $(daemon_pid_file "${cluster}")
}

stop_mirrors()
{
    local cluster=$1
    local sig=$2

    for instance in `seq 0 ${LAST_MIRROR_INSTANCE}`; do
        stop_mirror "${cluster}:${instance}" "${sig}"
    done
}

#
# Main
#

if [ "$#" -gt 0 ]
then
    if [ -z "${CEPHFS_MIRROR_TEMPDIR}" ]
    then
       echo "CEPHFS_MIRROR_TEMPDIR is not set" >&2
       exit 1
    fi

    TEMPDIR="${CEPHFS_MIRROR_TEMPDIR}"
    cd ${TEMPDIR}
    $@
    exit $?
fi
