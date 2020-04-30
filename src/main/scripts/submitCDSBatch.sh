#!/bin/bash

export MASKS_PER_JOB=5000
export LIBRARIES_PER_JOB=40000
# first run was 16 cores x 300GB mem but apparently 16 cores and 240GB may be OK
export CORES_RESOURCE=16
export MEM_RESOURCE=240

# round up the total numbers because the operations are integer divisions
export JOBS_FOR_LIBRARIES=`expr 200000 / $LIBRARIES_PER_JOB`
export JOBS_FOR_MASKS=`expr 35000 / $MASKS_PER_JOB`
export TOTAL_JOBS=$((JOBS_FOR_LIBRARIES * JOBS_FOR_MASKS))

# to test or run locally
# for ((LSB_JOBINDEX=1; LSB_JOBINDEX<=$TOTAL_JOBS; LSB_JOBINDEX++)) ; do local/scripts/submitCDSJobs.sh $LSB_JOBINDEX; done

bsub -n ${CORES_RESOURCE} -J CDS[1-${TOTAL_JOBS}] -P emlm \
    local/scripts/submitCDSJobs.sh
