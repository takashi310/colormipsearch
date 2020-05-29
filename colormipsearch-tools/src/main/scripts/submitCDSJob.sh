#!/bin/bash

function runCDSJob {
    MASK_INDEX=$(($1))
    LIBRARY_INDEX=$(($2))

    shift
    shift

    MASK_OFFSET=$((MASK_INDEX * MASKS_PER_JOB))
    LIBRARY_OFFSET=$((LIBRARY_INDEX * LIBRARIES_PER_JOB))

    REQUESTED_CORES=$((${CORES_RESOURCE:-0}))
    CPU_RESERVE=$((${CPU_RESERVE:-1}))
    CONCURRENCY=$((2 * REQUESTED_CORES - CPU_RESERVE))
    if [ ${CONCURRENCY} -lt 0 ] ; then
        CONCURRENCY=0
    fi
    CONCURRENCY_OPTS="--cdsConcurrency ${CONCURRENCY}"

    MEM_OPTS="-Xmx${MEM_RESOURCE}G -Xms${MEM_RESOURCE}G"
    if [ -n "${LOGFILE}" ] && [ -f "${LOGFILE}" ] ; then
        echo "Using Log config: ${LOGFILE}"
        LOG_OPTS="-Dlog4j.configuration=file://${LOGFILE}"
    else
        LOG_OPTS=""
    fi

    MIPS_CACHE_SIZE=${MIPS_CACHE_SIZE:-200000}
    MIPS_CACHE_EXPIRATION=${MIPS_CACHE_EXPIRATION:-60}

    JAVA_EXEC=${JAVA_EXEC:java}
    cmd="${JAVA_EXEC} ${GC_OPTS} ${MEM_OPTS} ${LOG_OPTS} \
        -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
        --cacheSize ${MIPS_CACHE_SIZE} --cacheExpirationInSeconds ${MIPS_CACHE_EXPIRATION} \
        searchFromJSON \
        -m ${MASKS_FILES} \
        --masks-index ${MASK_OFFSET} --masks-length ${MASKS_PER_JOB} \
        -i ${LIBRARIES_FILES} \
        --images-index ${LIBRARY_OFFSET} --images-length ${LIBRARIES_PER_JOB} \
        --maskThreshold ${MASK_THRESHOLD} \
        --dataThreshold ${DATA_THRESHOLD} \
        --xyShift ${XY_SHIFT} \
        --pixColorFluctuation ${PIX_FLUCTUATION} \
        --pctPositivePixels ${PIX_PCT_MATCH} \
        --mirrorMask \
        --libraryPartitionSize ${PROCESSING_PARTITION_SIZE} \
        --perMaskSubdir ${RESULTS_SUBDIR_FOR_MASKS} \
        --perLibrarySubdir ${RESULTS_SUBDIR_FOR_LIBRARIES} \
        ${CONCURRENCY_OPTS} \
        -od ${CDSMATCHES_RESULTS_DIR} \
        $*"

    echo "Running on $HOSTNAME: ${cmd}"
    ($cmd)
}

LSB_JOBINDEX=$((${LSB_JOBINDEX:-$1}))
JOB_INDEX=$((LSB_JOBINDEX - 1))
LIBRARY_INDEX=$((JOB_INDEX / JOBS_FOR_MASKS))
MASK_INDEX=$((JOB_INDEX % JOBS_FOR_MASKS))

echo "Run Job ($LSB_JOBINDEX, ${MASK_INDEX}, ${LIBRARY_INDEX})"
runCDSJob ${MASK_INDEX} ${LIBRARY_INDEX} > cds_${LSB_JOBINDEX}.log
