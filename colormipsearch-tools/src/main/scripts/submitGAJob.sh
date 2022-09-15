#!/bin/bash

function runGAJob {
    CDGA_INPUT_PARAM=$1
    shift

    NEGATIVE_RADIUS=20
    MASK_THRESHOLD=20

    REQUESTED_CORES=$((${CORES_RESOURCE:-0}))
    CPU_RESERVE=$((${CPU_RESERVE:-1}))
    CONCURRENCY=$((2 * REQUESTED_CORES - CPU_RESERVE))
    if [ ${CONCURRENCY} -lt 0 ] ; then
        CONCURRENCY=0
    fi
    CONCURRENCY_OPTS=${CONCURRENCY_OPTS:-"--cdsConcurrency ${CONCURRENCY}"}

    MEM_OPTS="-Xmx${MEM_RESOURCE}G -Xms${MEM_RESOURCE}G"

    if [ -n "${LOGCONFIGFILE}" ] && [ -f "${LOGCONFIGFILE}" ] ; then
        echo "Using Log config: ${LOGCONFIGFILE}"
        LOG_OPTS="-Dlog4j.configuration=file://${LOGCONFIGFILE}"
    else
        LOG_OPTS=""
    fi

    MIPS_CACHE_SIZE=${MIPS_CACHE_SIZE:-200000}

    JAVA_EXEC=${JAVA_EXEC:-java}
    CDS_JAR=${CDS_JAR:-target/colormipsearch-${CDS_JAR_VERSION}-jar-with-dependencies.jar}
    cmd="${JAVA_EXEC} ${JAVA_OPTS} ${GC_OPTS} ${MEM_OPTS} ${LOG_OPTS} \
        -jar ${CDS_JAR} \
        --cacheSize ${MIPS_CACHE_SIZE} \
        gradientScore \
        --maskThreshold ${MASK_THRESHOLD} \
        --negativeRadius ${NEGATIVE_RADIUS} \
        --mirrorMask \
        --nBestLines ${TOP_RESULTS} \
        --nBestSamplesPerLine ${SAMPLES_PER_LINE} \
        --processingPartitionSize ${PROCESSING_PARTITION_SIZE} \
        ${CONCURRENCY_OPTS} \
        -md ${CDGA_INPUT_PARAM} \
        $*"

    echo "Running on $HOSTNAME: ${cmd}"
    ($cmd)
}

CDGA_INPUT=${MASKS_LIBRARY:-$1}
LSB_JOBINDEX=${LSB_JOBINDEX:-$3}

JOB_INDEX=$((LSB_JOBINDEX - 1))
JOB_START_MIP_INDEX=$((JOB_INDEX * MIP_IDS_PER_JOB + START_MIP_ID_INDEX))

JOB_LOGPREFIX=${JOB_LOGPREFIX:-}
echo "$(date) Gradient Adjustment Job $LSB_JOBINDEX: $JOB_START_MIP_INDEX"
runGAJob "${CDGA_INPUT}:${JOB_START_MIP_INDEX}:${MIP_IDS_PER_JOB}" > ${JOB_LOGPREFIX}ga_${LSB_JOBINDEX}_${JOB_START_MIP_INDEX}_${MIP_IDS_PER_JOB}.log 2>&1
