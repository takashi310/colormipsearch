#!/bin/bash

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"

source ${SCRIPT_DIR}/cdsparams.sh

java -Xms360G -Xmx360G \
  -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
  gradientScoresFromMatchedResults \
  -revd ${CDGAS_RESULTS_DIR}/${RESULTS_SUBDIR_FOR_MASKS} \
  -rd ${CDSMATCHES_RESULTS_DIR}/${RESULTS_SUBDIR_FOR_LIBRARIES} \
  -od ${CDGAS_RESULTS_DIR}/${RESULTS_SUBDIR_FOR_LIBRARIES}
