WORKING_DIR=/nrs/scicompsoft/goinac/em-lm-cds/work/em-fl-run
MASK_NAME=flyem_hemibrain_with_fl
LIBRARY_NAME=flylight_gen1_mcfo_published
GA_PRECOMPUTED_FILES_LOCATION=/nrs/jacs/jacsData/filestore/system/40x_MCFO_Segmented_PackBits_forPublicRelease.zip

export MASKS_FILES="$WORKING_DIR/mips/flyem_hemibrain_with_fl.json"
export LIBRARIES_FILES="$WORKING_DIR/mips/flylight_gen1_mcfo_published_gamma1_4.json"

export TOTAL_MASKS=47454
export TOTAL_LIBRARIES=177894
export MASKS_PER_JOB=7909
export LIBRARIES_PER_JOB=9883
# this is the partition value used both for CDS and GA so it might need to be set differently for GA than it is for CDS
# for CDS the recommended value is between 100-500
# for GA the recommended value is between 5-50
export PROCESSING_PARTITION_SIZE=10

export CORES_RESOURCE=20
export CPU_RESERVE=1
export MEM_RESOURCE=180
export MIPS_CACHE_SIZE=100000
export MIPS_CACHE_EXPIRATION=60

CDSMATCHES_SUBDIR=cdsresults.matches
export CDSMATCHES_RESULTS_DIR=$WORKING_DIR/${CDSMATCHES_SUBDIR}

export RESULTS_SUBDIR_FOR_MASKS="${MASK_NAME}-vs-${LIBRARY_NAME}"
export RESULTS_SUBDIR_FOR_LIBRARIES="${LIBRARY_NAME}-vs-${MASK_NAME}"

CDSGA_SUBDIR=cdsresults.ga
export CDGAS_RESULTS_DIR=${WORKING_DIR}/${CDSGA_SUBDIR}
export CDGA_GRADIENTS_LOCATION=${GA_PRECOMPUTED_FILES_LOCATION}/40x_MCFO_Segmented_PackBits-forPublicRelease_gradient
export CDGA_ZGAP_LOCATION=${GA_PRECOMPUTED_FILES_LOCATION}/40x_MCFO_Segmented_PackBits-forPublicRelease_20pxRGBMAX
export CDGA_ZGAP_SUFFIX=_20pxRGBMAX

export LOGFILE=

# JAVA OPTS
export JAVA_HOME=${JDK_8}
export GC_OPTS=""
# Color depth search params
export MASK_THRESHOLD=20
export DATA_THRESHOLD=20
export XY_SHIFT=2
export PIX_FLUCTUATION=1
export PIX_PCT_MATCH=1

# GA params
export START_FILE_INDEX=0
export TOTAL_FILES=34800
export FILES_PER_JOB=100

export TOP_RESULTS=500
export SAMPLES_PER_LINE=0

FIRST_JOB=2
LAST_JOB=2
RUN_CMD="localRun"
