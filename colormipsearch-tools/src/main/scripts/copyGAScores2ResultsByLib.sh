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

JAVA_EXEC=${JAVA_EXEC:java}

cmd="${JAVA_EXEC} ${GC_OPTS} ${LOG_OPTS} -Xms460G -Xmx460G \
     -jar ${CDS_JAR} \
     gradientScoresFromMatchedResults \
     -revd ${CDGAS_RESULTS_DIR}/${RESULTS_SUBDIR_FOR_MASKS} \
     -rd ${CDSMATCHES_RESULTS_DIR}/${RESULTS_SUBDIR_FOR_LIBRARIES} \
     -od ${CDGAS_RESULTS_DIR}/${RESULTS_SUBDIR_FOR_LIBRARIES} \
     $*"

echo "Running on $HOSTNAME: ${cmd}"
($cmd)
