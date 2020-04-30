#!/bin/bash

LSB_JOBINDEX=${LSB_JOBINDEX:-$1}

echo "CDS JOB $LSB_JOBINDEX"

JOB_INDEX=$((LSB_JOBINDEX - 1))

MASKS_PER_JOB=$((${MASKS_PER_JOB:5000}))
LIBRARIES_PER_JOB=$((${LIBRARIES_PER_JOB:80000}))
JOBS_FOR_MASKS=$((${JOBS_FOR_MASKS:7}))

LIBRARY_INDEX=`expr $JOB_INDEX / $JOBS_FOR_MASKS`
MASK_INDEX=`expr $JOB_INDEX % $JOBS_FOR_MASKS`

LIBRARY_OFFSET=$((LIBRARY_INDEX * LIBRARIES_PER_JOB))
MASK_OFFSET=$((MASK_INDEX * MASKS_PER_JOB))

MEM_RESOURCE=${MEM_RESOURCE:180}

local/scripts/runCDSBatch.sh \
  ":${MASK_OFFSET}:${MASKS_PER_JOB}" \
  ":${LIBRARY_OFFSET}:${LIBRARIES_PER_JOB}" \
  "${MEM_RESOURCE}" \
  -m /nrs/scicompsoft/goinac/em-lm-cds/flyem_hemibrain.json${mask_range} \
  -i /nrs/scicompsoft/goinac/em-lm-cds/em-mcfo-cdsresults/flylight_gen1_mcfo_published.json${input_range} \
  --perMaskSubdir flyem_hemibrain \
  --perLibrarySubdir flylight_gen1_mcfo_published \
  -od /nrs/scicompsoft/goinac/em-lm-cds/em-mcfo-bsub-cdsresults \
  > cds_${LSB_JOBINDEX}.log
