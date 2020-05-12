#!/bin/bash

export TOTAL_MASKS=35000
export TOTAL_LIBRARIES=200000
export MASKS_PER_JOB=5000
export LIBRARIES_PER_JOB=40000
export PROCESSING_PARTITION_SIZE=4000

# round up the total numbers because the operations are integer divisions
export JOBS_FOR_LIBRARIES=$((TOTAL_LIBRARIES / LIBRARIES_PER_JOB))
export JOBS_FOR_MASKS=$((TOTAL_MASKS / MASKS_PER_JOB))
export TOTAL_JOBS=$((JOBS_FOR_LIBRARIES * JOBS_FOR_MASKS))

# first run was 16 cores x 300GB mem but apparently 16 cores and 240GB may be OK
export CORES_RESOURCE=16
export MEM_RESOURCE=240

export MASKS_FILE=local/testData/cdsresults/flyem_hemibrain.json
export LIBRARIES_FILE=local/testData/cdsresults/flylight_split_gal4_published.json
export RESULTS_DIR=local/testData/cdsresults
export PER_MASKS_RESULTS_SUBDIR=flyem_hemibrain-vs-sgal4
export PER_LIBRARY_RESULTS_SUBDIR=flylight_split_gal4_published

function runCDSBatch {
    is_range_re=':[0-9]+(:[0-9]+)?$'
    is_number_re='^[0-9]+$'

    if [[ $1 =~ $is_range_re ]] ; then
        mask_range="$1"
        shift
    else
        mask_range=""
    fi

    if [[ $1 =~ $is_range_re ]] ; then
        input_range="$1"
        shift
    else
        input_range=""
    fi

    MASK_THRESHOLD=20
    DATA_THRESHOLD=20

    MEM_OPTS="-Xmx${MEM_RESOURCE}G -Xms${MEM_RESOURCE}G"
    if [ -f ${LOGFILE} ] ; then
        LOG_OPTS="-Dlog4j.configuration=file://${LOGFILE}"
    else
        LOG_OPTS=""
    fi

    cmd="java ${MEM_OPTS} ${LOG_OPTS} \
        -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
        searchFromJSON \
        -m ${MASKS_FILE}${mask_range} \
        -i ${LIBRARIES_FILE}${input_range} \
        --maskThreshold ${MASK_THRESHOLD} \
        --dataThreshold ${DATA_THRESHOLD} \
        --xyShift 2 \
        --pixColorFluctuation 1 \
        --pctPositivePixels 1 \
        --mirrorMask \
        --libraryPartitionSize ${PROCESSING_PARTITION_SIZE} \
        --perMaskSubdir ${PER_MASKS_RESULTS_SUBDIR} \
        --perLibrarySubdir ${PER_LIBRARY_RESULTS_SUBDIR} \
        -od ${RESULTS_DIR} \
        $*"

    echo "Running: ${cmd}"
    ($cmd)
}

function submitCDSJobs {
    LSB_JOBINDEX=${LSB_JOBINDEX:-$1}
    JOB_INDEX=$((LSB_JOBINDEX - 1))
    LIBRARY_INDEX=$((JOB_INDEX / JOBS_FOR_MASKS))
    MASK_INDEX=$((JOB_INDEX % JOBS_FOR_MASKS))
    LIBRARY_OFFSET=$((LIBRARY_INDEX * LIBRARIES_PER_JOB))
    MASK_OFFSET=$((MASK_INDEX * MASKS_PER_JOB))

    runCDSBatch \
      ":${MASK_OFFSET}:${MASKS_PER_JOB}" \
      ":${LIBRARY_OFFSET}:${LIBRARIES_PER_JOB}" \
      ${MEM_RESOURCE} > cds_${LSB_JOBINDEX}.log
}

function localRun {
    for ((LSB_JOBINDEX=1; LSB_JOBINDEX<=$TOTAL_JOBS; LSB_JOBINDEX++)) ; do
        submitCDSJobs $LSB_JOBINDEX
    done
}

function gridRun {
    # this is tricky and has not been tested yet because we have to run a function from this file
    bsub -n ${CORES_RESOURCE} -J CDS[1-${TOTAL_JOBS}] -P emlm \
      bin/bash -rcfile submitCDSBatch.sh -i -c "submitCDSJobs"
}

echo "Total jobs: $TOTAL_JOBS"

# to run locally use localRun
# to run on the grid use gridRun
