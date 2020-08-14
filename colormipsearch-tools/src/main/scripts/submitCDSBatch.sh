#!/bin/bash

SCRIPT_DIR="$(dirname "$0")"

CDSPARAMS=${CDSPARAMS:-${SCRIPT_DIR}/cdsparams.sh}
source ${SCRIPT_DIR}/cdsparams.sh
source ${CDSPARAMS}

# round up the total numbers because the operations are integer divisions
export JOBS_FOR_LIBRARIES=$((TOTAL_LIBRARIES / LIBRARIES_PER_JOB))
export JOBS_FOR_MASKS=$((TOTAL_MASKS / MASKS_PER_JOB))
export TOTAL_JOBS=$((JOBS_FOR_LIBRARIES * JOBS_FOR_MASKS))

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
FIRST_JOB=${FIRST_JOB:-1}
LAST_JOB=${LAST_JOB:-${TOTAL_JOBS}}
startcmd="${RUN_CMD} ${FIRST_JOB} ${LAST_JOB}"
echo $startcmd
($startcmd)
