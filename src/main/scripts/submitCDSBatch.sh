#!/bin/bash

# based on the stats gathered from the first grid run which requested 16 cores x 300GB mem
# if we partition 5000 masks and 40000 libraries per job 16 cores and 240GB may be OK because it only needed 219GB of mem
export MASKS_PER_JOB=5000
export LIBRARIES_PER_JOB=40000
export CORES_RESOURCE=16
export MEM_RESOURCE=240

JOBS_FOR_LIBRARIES=`expr 160000 / $LIBRARIES_PER_JOB`
JOBS_FOR_MASKS=`expr 35000 / $MASKS_PER_JOB`
TOTAL_JOBS=$((JOBS_FOR_LIBRARIES * JOBS_FOR_MASKS))

# to test
# for ((LSB_JOBINDEX=1; LSB_JOBINDEX<=$TOTAL_JOBS; LSB_JOBINDEX++)) ; do `command`; done

bsub -n ${CORES_RESOURCE} -J CDS[1-${TOTAL_JOBS}] -P emlm \
    local/scripts/submitCDSJobs.sh
