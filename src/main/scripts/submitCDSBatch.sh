#!/bin/bash

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"

export TOTAL_MASKS=34717
export TOTAL_LIBRARIES=7708
export MASKS_PER_JOB=34717
export LIBRARIES_PER_JOB=7708
export PROCESSING_PARTITION_SIZE=1700

# round up the total numbers because the operations are integer divisions
export JOBS_FOR_LIBRARIES=$((TOTAL_LIBRARIES / LIBRARIES_PER_JOB))
export JOBS_FOR_MASKS=$((TOTAL_MASKS / MASKS_PER_JOB))
export TOTAL_JOBS=$((JOBS_FOR_LIBRARIES * JOBS_FOR_MASKS))

# first run was 16 cores x 300GB mem but apparently 16 cores and 240GB may be OK
export CORES_RESOURCE=16
export MEM_RESOURCE=180

export LOGFILE=
export MASK_THRESHOLD=20
export DATA_THRESHOLD=20
export XY_SHIFT=2
export PIX_FLUCTUATION=1
export PIX_PCT_MATCH=1

export MASKS_FILE=local/testData/mips/flyem_hemibrain.json
export LIBRARIES_FILE=local/testData/mips/flylight_split_gal4_published.json
export RESULTS_DIR=local/testData/cdsresults
export PER_MASKS_RESULTS_SUBDIR=flyem_hemibrain-vs-flylight_split_gal4_published
export PER_LIBRARY_RESULTS_SUBDIR=flylight_split_gal4_published

function localRun {
    if [[ $# -lt 2 ]] ; then
      echo "localRun <from> <to>"
            exit 1
    fi
    from=$1
    to=$2
    for ((LSB_JOBINDEX=${from}; LSB_JOBINDEX<=${to}; LSB_JOBINDEX++)) ; do
        ${SCRIPT_DIR}/submitCDSJob.sh $LSB_JOBINDEX
    done
}

function gridRun {
    if [[ $# -lt 2 ]] ; then
      echo "gridRun <from> <to>"
            exit 1
    fi
    from=$1
    to=$2
    # this is tricky and has not been tested yet because we have to run a function from this file
    bsub -n ${CORES_RESOURCE} -J CDS[${from}-${to}] -P emlm \
        ${SCRIPT_DIR}/submitCDSJob.sh
}

echo "Total jobs: $TOTAL_JOBS"

# to run locally use localRun <from> <to>
# to run on the grid use gridRun <from> <to>
localRun 1 $TOTAL_JOBS
