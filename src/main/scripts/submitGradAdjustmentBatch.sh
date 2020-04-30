#!/bin/bash

LIB_SUB_DIR=flyem_hemibrain
ORIGINAL_RESULTS_DIR=/nrs/scicompsoft/goinac/em-lm-cds/em-mcfo-cdsresults
FINAL_RESULTS_DIR=local/testData/cdsresults.ga

export CDGA_INPUT_DIR=${ORIGINAL_RESULTS_DIR}/${LIB_SUB_DIR}
export CDGA_OUTPUT_DIR=${FINAL_RESULTS_DIR}/${LIB_SUB_DIR}

export TOTAL_FILES=50
export FILES_PER_JOB=50
export LIB_PARTITION_SIZE=100
export TOTAL_JOBS=$((TOTAL_FILES / FILES_PER_JOB))

export CORES_RESOURCE=20
export TOP_RESULTS=300
export SAMPLES_PER_LINE=0
export MEM_RESOURCE=300

for ((LSB_JOBINDEX=1; LSB_JOBINDEX<=$TOTAL_JOBS; LSB_JOBINDEX++)) ; do
    local/scripts/submitGradAdjustmentJobs.sh $CDGA_INPUT_DIR $CDGA_OUTPUT_DIR $LSB_JOBINDEX
done

#bsub -n ${CORES_RESOURCE} -J CDGA[1-${TOTAL_JOBS}] -P emlm \
#    local/scripts/submitGradAdjustmentJobs.sh $CDGA_INPUT_DIR $CDGA_OUTPUT_DIR
