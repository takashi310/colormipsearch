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
    CONCURRENCY_OPTS=${CONCURRENCY_OPTS:-"--cdsConcurrency ${CONCURRENCY}"}

    MEM_OPTS="-Xmx${MEM_RESOURCE}G -Xms${MEM_RESOURCE}G"
    CDGA_ZGAP_SUFFIX=${CDGA_ZGAP_SUFFIX:_20pxRGBMAX}

    if [ -n "${CDGA_GRADIENTS_LOCATION}" ] ; then
        CDGA_GRAD_OPTS="-gp ${CDGA_GRADIENTS_LOCATION}"
    else
        CDGA_GRAD_OPTS=
    fi

    if [ -n "${CDGA_ZGAP_LOCATION}" ] ; then
        CDGA_ZGAP_OPTS="-zgp ${CDGA_ZGAP_LOCATION}"
        if [ -n "${CDGA_ZGAP_SUFFIX}" ] ; then
            CDGA_ZGAP_OPTS="${CDGA_ZGAP_OPTS} --zgapSuffix ${CDGA_ZGAP_SUFFIX}"
        fi
    else
        CDGA_ZGAP_OPTS=
    fi

    if [ -n "${LOGCONFIGFILE}" ] && [ -f "${LOGCONFIGFILE}" ] ; then
        echo "Using Log config: ${LOGCONFIGFILE}"
        LOG_OPTS="-Dlog4j.configuration=file://${LOGCONFIGFILE}"
    else
        LOG_OPTS=""
    fi

    MIPS_CACHE_SIZE=${MIPS_CACHE_SIZE:-200000}
    MIPS_CACHE_EXPIRATION=${MIPS_CACHE_EXPIRATION:-60}

    JAVA_EXEC=${JAVA_EXEC:-java}
    CDS_JAR=${CDS_JAR:-target/colormipsearch-${CDS_JAR_VERSION}-jar-with-dependencies.jar}
    cmd="${JAVA_EXEC} ${JAVA_OPTS} ${GC_OPTS} ${MEM_OPTS} ${LOG_OPTS} \
        -jar ${CDS_JAR} \
        --cacheSize ${MIPS_CACHE_SIZE} --cacheExpirationInSeconds ${MIPS_CACHE_EXPIRATION} \
        gradientScore \
        --maskThreshold ${MASK_THRESHOLD} \
        --negativeRadius ${NEGATIVE_RADIUS} \
        --mirrorMask \
        --topPublishedNameMatches ${TOP_RESULTS} \
        --topPublishedSampleMatches ${SAMPLES_PER_LINE} \
        --processingPartitionSize ${PROCESSING_PARTITION_SIZE} \
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

JOB_LOGPREFIX=${JOB_LOGPREFIX:-}
echo "$(date) Gradient Adjustment Job $LSB_JOBINDEX: $INPUT_INDEX"
runGAJob \
  "${CDGA_INPUT_DIR}:${JOB_START_FILE_INDEX}:${FILES_PER_JOB}" \
  "${CDGA_OUTPUT_DIR}" > ${JOB_LOGPREFIX}ga_${LSB_JOBINDEX}_${JOB_START_FILE_INDEX}_${FILES_PER_JOB}.log
