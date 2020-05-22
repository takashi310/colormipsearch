export TOTAL_MASKS=34718
export TOTAL_LIBRARIES=9603
export MASKS_PER_JOB=17359
export LIBRARIES_PER_JOB=9603
export PROCESSING_PARTITION_SIZE=500

export CORES_RESOURCE=20
export MEM_RESOURCE=180

WORKING_DIR=/nrs/scicompsoft/goinac/em-lm-cds/work/all_sgal4
MASK_NAME=flyem_hemibrain_with_fl
LIBRARY_NAME=flylight_split_gal4_all

export MASKS_FILES="$WORKING_DIR/mips/flyem_hemibrain_with_fl.json"
export LIBRARIES_FILES="$WORKING_DIR/mips/flylight_split_gal4_published.json \
                        $WORKING_DIR/mips/flylight_split_gal4_drivers_missing_from_published.json"

CDSMATCHES_SUBDIR=cdsresults.matches
export CDSMATCHES_RESULTS_DIR=$WORKING_DIR/${CDSMATCHES_SUBDIR}

export RESULTS_SUBDIR_FOR_MASKS="${MASK_NAME}-vs-${LIBRARY_NAME}"
export RESULTS_SUBDIR_FOR_LIBRARIES="${LIBRARY_NAME}-vs-${MASK_NAME}"

CDSGA_SUBDIR=cdsresults.ga
export CDGAS_RESULTS_DIR=${WORKING_DIR}/${CDSGA_SUBDIR}
export CDGA_GRADIENTS_LOCATION=/nrs/jacs/jacsData/filestore/system/SS_Split/SS_Split_ALL_Segmented_gradient
export CDGA_ZGAP_LOCATION=/nrs/jacs/jacsData/filestore/system/SS_Split/SS_Split_ALL_Segmented_20pxRGB
export CDGA_ZGAP_SUFFIX=_20pxRGB

export LOGFILE=

# Color depth search params
export MASK_THRESHOLD=20
export DATA_THRESHOLD=20
export XY_SHIFT=2
export PIX_FLUCTUATION=1
export PIX_PCT_MATCH=1

# GA params
export START_FILE_INDEX=0
export TOTAL_FILES=${TOTAL_MASKS}
export FILES_PER_JOB=200
export PROCESSING_PARTITION_SIZE=5

export TOP_RESULTS=500
export SAMPLES_PER_LINE=0

FIRST_JOB=
LAST_JOB=
RUN_CMD=echo
