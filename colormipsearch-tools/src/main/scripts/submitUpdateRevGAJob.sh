#!/bin/bash

function updateRevGAJob {
    CDGA_INPUT_PARAM=$1
    shift

    CDGA_OUTPUT_PARAM=$1
    shift

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
    MIPS_CACHE_EXPIRATION=${MIPS_CACHE_EXPIRATION:-60}

    JAVA_EXEC=${JAVA_EXEC:-java}
    CDS_JAR=${CDS_JAR:-target/colormipsearch-${CDS_JAR_VERSION}-jar-with-dependencies.jar}

    cmd="${JAVA_EXEC} ${JAVA_OPTS} ${GC_OPTS} ${LOG_OPTS} ${MEM_OPTS} \
         -jar ${CDS_JAR} \
         --cacheSize ${MIPS_CACHE_SIZE} --cacheExpirationInSeconds ${MIPS_CACHE_EXPIRATION} \
         gradientScoresFromMatchedResults \
        ${CONCURRENCY_OPTS} \
        --topPublishedNameMatches ${TOP_RESULTS} \
        --topPublishedSampleMatches ${SAMPLES_PER_LINE} \
        --processingPartitionSize ${PROCESSING_PARTITION_SIZE} \
         -revd ${CDGAS_RESULTS_DIR}/${RESULTS_SUBDIR_FOR_MASKS} \
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
echo "Update Reverse Gradient Scores Job $LSB_JOBINDEX: $INPUT_INDEX"
updateRevGAJob \
  "${CDGA_INPUT_DIR}:${JOB_START_FILE_INDEX}:${FILES_PER_JOB}" \
  "${CDGA_OUTPUT_DIR}" > ${JOB_LOGPREFIX}updaterevga_${LSB_JOBINDEX}_${JOB_START_FILE_INDEX}_${FILES_PER_JOB}.log
