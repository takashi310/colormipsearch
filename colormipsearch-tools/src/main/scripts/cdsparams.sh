MASKS_LIBRARY=flyem_hemibrain_1_2_1
TARGETS_LIBRARY=flylight_gen1_mcfo_published

# TOTAL_MASKS is the total number of mask images used for color depth search,
# e.g. for EM is the number of MIPs for the EM neurons plus the number of FL neurons if some neurons require an FL (flip) transformation
export TOTAL_MASKS=44593
# TOTAL_TARGETS is the total number of target images,
# e.g. for LM this is the total number of segmented MIPs used for color depth search
export TOTAL_TARGETS=7391
# the selection of the number of masks or libraries per job is empirical based on the size of the libraries and/or masks
export MASKS_PER_JOB=44593
export TARGETS_PER_JOB=7391
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

export LOGCONFIGFILE=

# JAVA OPTS
# if needed one can set java runtime like this:
# export JAVA_HOME=${HOME}/tools/jdk-14.0.1
# export JAVA_EXEC=${JAVA_HOME}/bin/java

# ZGC option if jdk14 is used
# ZGC="-XX:+UnlockExperimentalVMOptions -XX:+UseZGC"
export GC_OPTS=

# this only needs to change on a new release
export CDS_JAR_VERSION="3.0.0"
export CDS_JAR=${CDS_JAR:-target/colormipsearch-${CDS_JAR_VERSION}-jar-with-dependencies.jar}

# Color depth search params
export MASK_THRESHOLD=20
export DATA_THRESHOLD=20
export XY_SHIFT=2
export PIX_FLUCTUATION=1
export PIX_PCT_MATCH=1

# GA params
# TOTAL_MIP_IDS is the number of "distinct" MIP IDs containing matches by EM that is divisible by MIP_IDS_PER_JOB
# the reason for that is that in bash TOTAL_MIP_IDS/MIP_IDS_PER_JOB is an integer division and if it does not divide exactly
# we may not process all the files
export TOTAL_MIP_IDS=34800
# an offset of the MIPs to be excluded from the gradient scoring process
export START_MIP_ID_INDEX=0
# the value depends on the CPU and memory resources available on the machine. If running on the grid requesting 20 cores
# for split gal4 drivers we can use up to 200 files per job - for MCFO we cannot go higher than 100 since the number of MCFOs
# is much larger
export MIP_IDS_PER_JOB=100

# this specifies the number of lines to select for gradient scoring.
export TOP_RESULTS=300
export SAMPLES_PER_LINE=0

# FIRST_JOB and LAST_JOB specify the job range - if not set they default to first and last job respectivelly
FIRST_JOB=${FIRST_JOB:-}
LAST_JOB=${LAST_JOB:-}
# use localRun to run on the host on which the command is invoked or gridRun to invoke it using bsub
RUN_CMD="localRun"
