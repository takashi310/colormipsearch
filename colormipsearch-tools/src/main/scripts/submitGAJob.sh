#!/bin/bash

function runGAJob {
    CDGA_INPUT_PARAM=$1
    shift

    CDGA_OUTPUT_PARAM=$1
    shift

    NEGATIVE_RADIUS=20
    MASK_THRESHOLD=20

    REQUESTED_CORES=$((${CORES_RESOURCE:-0}))
    CPU_RESERVE=$((${CPU_RESERVE:-1}))
    CONCURRENCY=$((2 * REQUESTED_CORES - CPU_RESERVE))
    if [ ${CONCURRENCY} -lt 0 ] ; then
        CONCURRENCY=0
    fi
    CONCURRENCY_OPTS="--cdsConcurrency ${CONCURRENCY}"

    MEM_OPTS="-Xmx${MEM_RESOURCE}G -Xms${MEM_RESOURCE}G"
    CDGA_ZGAP_SUFFIX=${CDGA_ZGAP_SUFFIX:_20pxRGBMAX}
    CDGA_GRAD_OPTS="-gp ${CDGA_GRADIENTS_LOCATION}"
    CDGA_ZGAP_OPTS="-zgp ${CDGA_ZGAP_LOCATION} --zgapSuffix ${CDGA_ZGAP_SUFFIX}"

    if [ -n "${LOGFILE}" ] && [ -f "${LOGFILE}" ] ; then
        echo "Using Log config: ${LOGFILE}"
        LOG_OPTS="-Dlog4j.configuration=file://${LOGFILE}"
    else
        LOG_OPTS=""
    fi

    MIPS_CACHE_SIZE=${MIPS_CACHE_SIZE:-200000}
    MIPS_CACHE_EXPIRATION=${MIPS_CACHE_EXPIRATION:-60}

    cmd="${JAVA_HOME}/bin/java ${GC_OPTS} ${MEM_OPTS} ${LOG_OPTS} \
        -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
        --cacheSize ${MIPS_CACHE_SIZE} --cacheExpirationInSeconds ${MIPS_CACHE_EXPIRATION} \
        gradientScore \
        --maskThreshold ${MASK_THRESHOLD} \
        --negativeRadius ${NEGATIVE_RADIUS} \
        --mirrorMask \
        --topPublishedNameMatches ${TOP_RESULTS} \
        --topPublishedSampleMatches ${SAMPLES_PER_LINE} \
        --libraryPartitionSize ${PROCESSING_PARTITION_SIZE} \
        ${CDGA_GRAD_OPTS} \
        ${CDGA_ZGAP_OPTS} \
        ${CONCURRENCY_OPTS} \
        -rd ${CDGA_INPUT_PARAM} \
        -od ${CDGA_OUTPUT_PARAM} \
        $*"

    echo "Running on $HOSTNAME: ${cmd}"
    ($cmd)
}

CDGA_INPUT_DIR=${CDGA_INPUT_DIR:-$1}
CDGA_OUTPUT_DIR=${CDGA_OUTPUT_DIR:-$2}
LSB_JOBINDEX=${LSB_JOBINDEX:-$3}

JOB_INDEX=$((LSB_JOBINDEX - 1))
JOB_START_FILE_INDEX=$((JOB_INDEX * FILES_PER_JOB + START_FILE_INDEX))

echo "Gradient Adjustment Job $LSB_JOBINDEX: $INPUT_INDEX"
runGAJob \
  "${CDGA_INPUT_DIR}:${JOB_START_FILE_INDEX}:${FILES_PER_JOB}" \
  "${CDGA_OUTPUT_DIR}" > ga_${LSB_JOBINDEX}_${JOB_START_FILE_INDEX}_${FILES_PER_JOB}.log
