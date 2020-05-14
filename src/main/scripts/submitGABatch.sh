#!/bin/bash

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"

LIB_SUB_DIR=flyem_hemibrain
CDSRESULTS_DIR=local/testData/cdsresults
GACDSRESULTS_DIR=local/testData/cdsresults.ga

export CDGA_INPUT_DIR=${CDSRESULTS_DIR}/${LIB_SUB_DIR}
export CDGA_OUTPUT_DIR=${GACDSRESULTS_DIR}/${LIB_SUB_DIR}
export CDGA_GRADIENTS_LOCATION=/nrs/jacs/jacsData/filestore/system/40x_MCFO
export CDGA_ZGAP_LOCATION=/nrs/jacs/jacsData/filestore/system/40x_MCFO
export CDGA_ZGAP_SUFFIX=_20pxRGBMAX

export LOG_FILE=

export START_FILE_INDEX=0
export TOTAL_FILES=50
export FILES_PER_JOB=50
export PROCESSING_PARTITION_SIZE=5
export TOTAL_JOBS=$(((TOTAL_FILES - START_FILE_INDEX)/ FILES_PER_JOB))

export CORES_RESOURCE=20
export TOP_RESULTS=500
export SAMPLES_PER_LINE=0
export MEM_RESOURCE=300

function runGradAdjustment {
    CDGA_INPUT_PARAM=$1
    shift

    CDGA_OUTPUT_PARAM=$1
    shift

    NEGATIVE_RADIUS=20
    MASK_THRESHOLD=20

    MEM_OPTS="-Xmx${MEM_RESOURCE}G -Xms${MEM_RESOURCE}G"
    CDGA_ZGAP_SUFFIX=${CDGA_ZGAP_SUFFIX:_20pxRGBMAX}
    CDGA_GRAD_OPTS="-gp ${CDGA_GRADIENTS_LOCATION}"
    CDGA_ZGAP_OPTS="-zgp ${CDGA_ZGAP_LOCATION} --zgapSuffix ${CDGA_ZGAP_SUFFIX}"

    if [ -f ${LOGFILE} ] ; then
        LOG_OPTS="-Dlog4j.configuration=file://${LOGFILE}"
    else
        LOG_OPTS=""
    fi

    cmd="java ${MEM_OPTS} ${LOG_OPTS} \
        -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
        gradientScore \
        --maskThreshold ${MASK_THRESHOLD} \
        --negativeRadius ${NEGATIVE_RADIUS} \
        --mirrorMask \
        --topPublishedNameMatches ${TOP_RESULTS} \
        --topPublishedSampleMatches ${SAMPLES_PER_LINE} \
        --libraryPartitionSize ${PROCESSING_PARTITION_SIZE} \
        ${CDGA_GRAD_OPTS} \
        ${CDGA_ZGAP_OPTS} \
        -rd ${CDGA_INPUT_PARAM} \
        -od ${CDGA_OUTPUT_PARAM} \
        $*"

    echo "Running: ${cmd}"
    ($cmd)

}

function submitGradAdjustmentJobs {
    CDGA_INPUT_DIR=${CDGA_INPUT_DIR:-$1}
    CDGA_OUTPUT_DIR=${CDGA_OUTPUT_DIR:-$2}
    LSB_JOBINDEX=${LSB_JOBINDEX:-$3}

    JOB_INDEX=$((LSB_JOBINDEX - 1))
    JOB_START_FILE_INDEX=$((JOB_INDEX * FILES_PER_JOB + START_FILE_INDEX))

    echo "Gradient Adjustment Job $LSB_JOBINDEX: $INPUT_INDEX"

    runGradAdjustment \
      "${CDGA_INPUT_DIR}:${JOB_START_FILE_INDEX}:${FILES_PER_JOB}" \
      "${CDGA_OUTPUT_DIR}" > ga_${LSB_JOBINDEX}_${JOB_START_FILE_INDEX}_${FILES_PER_JOB}.log
}

function localRun {
    if [[ $# -lt 2 ]] ; then
      echo "localRun <from> <to>"
            exit 1
    fi
    from=$1
    to=$2
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
    bsub -n ${CORES_RESOURCE} -J CDGA[1-${TOTAL_JOBS}] -P emlm \
        ${SCRIPT_DIR}/submitGAJob.sh ${CDGA_INPUT_DIR} ${CDGA_OUTPUT_DIR}
}

echo "Total jobs: ${TOTAL_JOBS}"

# to run locally use localRun <from> <to>
# to run on the grid use gridRun <from> <to>
