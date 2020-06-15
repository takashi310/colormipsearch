#!/bin/bash

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"

source ${SCRIPT_DIR}/cdsparams.sh

if [ -n "${LOGCONFIGFILE}" ] && [ -f "${LOGCONFIGFILE}" ] ; then
    echo "Using Log config: ${LOGCONFIGFILE}"
    LOG_OPTS="-Dlog4j.configuration=file://${LOGCONFIGFILE}"
else
    LOG_OPTS=""
fi

JAVA_EXEC=${JAVA_EXEC:java}

${JAVA_EXEC} ${GC_OPTS} ${LOG_OPTS} -Xms460G -Xmx460G \
  -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
  gradientScoresFromMatchedResults \
  -revd ${CDGAS_RESULTS_DIR}/${RESULTS_SUBDIR_FOR_MASKS} \
  -rd ${CDSMATCHES_RESULTS_DIR}/${RESULTS_SUBDIR_FOR_LIBRARIES} \
  -od ${CDGAS_RESULTS_DIR}/${RESULTS_SUBDIR_FOR_LIBRARIES}
