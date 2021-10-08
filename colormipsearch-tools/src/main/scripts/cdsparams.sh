WORKING_DIR=/nrs/scicompsoft/goinac/em-lm-cds/work/em_1_1-fl-run
INPUT_DIR=/groups/scicompsoft/informatics/data/release_libraries
MASK_NAME=flyem_hemibrain_1_1_with_fl
LIBRARY_NAME=all_flylight_split_gal4
GA_PRECOMPUTED_FILES_LOCATION=/nrs/scicompsoft/otsuna/SS_vol

# MASKS_FILES and LIBRARIES_FILES are - comma or space separated - json files
# created with the "createColorDepthSearchJSONInput" command
export MASKS_FILES="${INPUT_DIR}/flyem_hemibrain_1_1_with_fl.json"
export LIBRARIES_FILES="${INPUT_DIR}/flylight_split_gal4_published.json ${INPUT_DIR}/flylight_split_gal4_drivers_missing_from_published.json"

# to get the value for TOTAL_MASKS we can simply run `grep imageURL "$WORKING_DIR/mips/flyem_hemibrain_with_fl.json | wc`
export TOTAL_MASKS=44593
# to get the value for TOTAL_LIBRARIES we can simply run `grep imageURL "$WORKING_DIR/mips/flylight_gen1_mcfo_published_gamma1_4.json | wc`
export TOTAL_LIBRARIES=7391
# the selection of the number of masks or libraries per job is empirical based on the size of the libraries and/or masks
export MASKS_PER_JOB=7391
export LIBRARIES_PER_JOB=44593
# this is the partition value used both for CDS and GA so it might need to be set differently for GA than it is for CDS
# for CDS the recommended value is between 100-500
# for GA the recommended value is between 5-50
export PROCESSING_PARTITION_SIZE=500

export CORES_RESOURCE=20
export CPU_RESERVE=1
# MEM_RESOURCE value is the memory in GB available on the host on which this runs
export MEM_RESOURCE=170
# a cache size of 100000 is OK if there are at least 160GB of memory - otherwise set it to 50000 or
# to some other reasonable value based on the available memory
export MIPS_CACHE_SIZE=100000

CDSMATCHES_SUBDIR=cdsresults.matches
export CDSMATCHES_RESULTS_DIR=$WORKING_DIR/${CDSMATCHES_SUBDIR}

export RESULTS_SUBDIR_FOR_MASKS="${MASK_NAME}-vs-${LIBRARY_NAME}"
export RESULTS_SUBDIR_FOR_LIBRARIES="${LIBRARY_NAME}-vs-${MASK_NAME}"

CDSGA_SUBDIR=cdsresults.ga
export CDGAS_RESULTS_DIR=${WORKING_DIR}/${CDSGA_SUBDIR}
export CDGA_GRADIENTS_LOCATION=${GA_PRECOMPUTED_FILES_LOCATION}/SSnew_05202020_gradient.zip
export CDGA_ZGAP_LOCATION=${GA_PRECOMPUTED_FILES_LOCATION}/SSnew_05202020_RGB20px.zip
export CDGA_ZGAP_SUFFIX=_RGB20px

export LOGCONFIGFILE=

# JAVA OPTS
# if needed one can set java runtime like this:
# export JAVA_HOME=${HOME}/tools/jdk-14.0.1
# export JAVA_EXEC=${JAVA_HOME}/bin/java

# ZGC option if jdk14 is used
# ZGC="-XX:+UnlockExperimentalVMOptions -XX:+UseZGC"
export GC_OPTS=

# this only needs to change on a new release
export CDS_JAR_VERSION="2.4"
export CDS_JAR=${CDS_JAR:-target/colormipsearch-${CDS_JAR_VERSION}-jar-with-dependencies.jar}

# Color depth search params
export MASK_THRESHOLD=20
export DATA_THRESHOLD=20
export XY_SHIFT=2
export PIX_FLUCTUATION=1
export PIX_PCT_MATCH=1

# GA params
# TOTAL_FILES is the least number > the number of files containing matches by EM that is divisible by FILES_PER_JOB
# the reason for that is that in bash TOTAL_FILES/FILES_PER_JOB is an integer division and if it does not divide exactly
# we may not process all the files
# so to calculate it `ls ${CDGAS_RESULTS_DIR}/${RESULTS_SUBDIR_FOR_MASKS} | wc` then take the least number > the value
# that is divisible by the selected value for FILES_PER_JOB
export TOTAL_FILES=34800
export START_FILE_INDEX=0
# the value depends on the CPU and memory resources available on the machine. If running on the grid requesting 20 cores
# for split gal4 drivers we can use up to 200 files per job - for MCFO we cannot go higher than 100 since the number of MCFOs
# is much larger
export FILES_PER_JOB=100

# this specifies the number of lines to select for gradient scoring.
export TOP_RESULTS=300
export SAMPLES_PER_LINE=0

# FIRST_JOB and LAST_JOB specify the job range - if not set they default to first and last job respectivelly
FIRST_JOB=${FIRST_JOB:-}
LAST_JOB=${LAST_JOB:-}
# use localRun to run on the host on which the command is invoked or gridRun to invoke it using bsub
RUN_CMD="localRun"
