#!/bin/bash

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"

WORKING_DIR=/nrs/scicompsoft/goinac/em-lm-cds/work/all_sgal4
CDSMATCHES_SUBDIR=cdsresults.matches_including_fl
CDSGA_SUBDIR=cdsresults.ga_including_fl
LIB_SUB_DIR=flyem_hemibrain_all-vs-flylight_split_gal4_all
CDSRESULTS_DIR=${WORKING_DIR}/${CDSMATCHES_SUBDIR}
GACDSRESULTS_DIR=${WORKING_DIR}/${CDSGA_SUBDIR}

export CDGA_INPUT_DIR=${CDSRESULTS_DIR}/${LIB_SUB_DIR}
export CDGA_OUTPUT_DIR=${GACDSRESULTS_DIR}/${LIB_SUB_DIR}
export CDGA_GRADIENTS_LOCATION=/nrs/jacs/jacsData/filestore/system/SS_Split/SS_Split_ALL_Segmented_gradient
export CDGA_ZGAP_LOCATION=/nrs/jacs/jacsData/filestore/system/SS_Split/SS_Split_ALL_Segmented_20pxRGB
export CDGA_ZGAP_SUFFIX=_20pxRGB

export LOGFILE=

export START_FILE_INDEX=0
export TOTAL_FILES=34800
export FILES_PER_JOB=200
export PROCESSING_PARTITION_SIZE=5
export TOTAL_JOBS=$(((TOTAL_FILES - START_FILE_INDEX) / FILES_PER_JOB))

export CORES_RESOURCE=20
export TOP_RESULTS=500
export SAMPLES_PER_LINE=0
export MEM_RESOURCE=180

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
localRun 2 ${TOTAL_JOBS}
