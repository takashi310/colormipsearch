#!/bin/bash

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"

CDSPARAMS=${CDSPARAMS:-${SCRIPT_DIR}/cdsparams.sh}
source ${SCRIPT_DIR}/cdsparams.sh
source ${CDSPARAMS}

if [ -n "${LOGCONFIGFILE}" ] && [ -f "${LOGCONFIGFILE}" ] ; then
    echo "Using Log config: ${LOGCONFIGFILE}"
    LOG_OPTS="-Dlog4j.configuration=file://${LOGCONFIGFILE}"
else
    LOG_OPTS=""
fi

MIPS_CACHE_SIZE=${MIPS_CACHE_SIZE:-200000}
MIPS_CACHE_EXPIRATION=${MIPS_CACHE_EXPIRATION:-60}

JAVA_EXEC=${JAVA_EXEC:java}
MEM_OPTS="-Xmx${MEM_RESOURCE}G -Xms${MEM_RESOURCE}G"

cmd="${JAVA_EXEC} ${GC_OPTS} ${LOG_OPTS} ${MEM_OPTS} \
     -jar ${CDS_JAR} \
     --cacheSize ${MIPS_CACHE_SIZE} --cacheExpirationInSeconds ${MIPS_CACHE_EXPIRATION} \
     gradientScoresFromMatchedResults \
     -revd ${CDGAS_RESULTS_DIR}/${RESULTS_SUBDIR_FOR_MASKS} \
     -rd ${CDSMATCHES_RESULTS_DIR}/${RESULTS_SUBDIR_FOR_LIBRARIES} \
     -od ${CDGAS_RESULTS_DIR}/${RESULTS_SUBDIR_FOR_LIBRARIES} \
     $*"

echo "Running on $HOSTNAME: ${cmd}"
($cmd)
