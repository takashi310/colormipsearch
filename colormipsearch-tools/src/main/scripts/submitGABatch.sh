#!/bin/bash

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"

source ${SCRIPT_DIR}/cdsparams.sh

export CDGA_INPUT_DIR=${CDSMATCHES_RESULTS_DIR}/${RESULTS_SUBDIR_FOR_MASKS}
export CDGA_OUTPUT_DIR=${CDGA_SRESULTS_DIR}/${RESULTS_SUBDIR_FOR_MASKS}

export TOTAL_JOBS=$(((TOTAL_FILES - START_FILE_INDEX) / FILES_PER_JOB))

function localRun {
    if [[ $# -lt 2 ]] ; then
      echo "localRun <from> <to>"
            exit 1
    fi
    from=$1
    to=$2
    echo "Running jobs: ${from} - ${to}"
    for ((LSB_JOBINDEX=${from}; LSB_JOBINDEX<=${to}; LSB_JOBINDEX++)) ; do
        ${SCRIPT_DIR}/submitGAJob.sh ${CDGA_INPUT_DIR} ${CDGA_OUTPUT_DIR} ${LSB_JOBINDEX}
    done
}

function gridRun {
    if [[ $# -lt 2 ]] ; then
      echo "gridRun <from> <to>"
            exit 1
    fi
    from=$1
    to=$2
    echo "Running jobs: ${from} - ${to}"
    bsub -n ${CORES_RESOURCE} -J CDGA[${from}-${to}] -P emlm \
        ${SCRIPT_DIR}/submitGAJob.sh ${CDGA_INPUT_DIR} ${CDGA_OUTPUT_DIR}
}

echo "Total jobs: ${TOTAL_JOBS}"

# to run locally use localRun <from> <to>
# to run on the grid use gridRun <from> <to>
FIRST_JOB=${FIRST_JOB:-1}
LAST_JOB=${LAST_JOB:-${TOTAL_JOBS}}
$(RUN_CMD ${FIRST_JOB} ${LAST_JOB})
