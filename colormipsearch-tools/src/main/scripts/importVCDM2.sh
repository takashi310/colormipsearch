DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
LOG_OPTS="-Dlog4j.configuration=file://$PWD/local/log4j.properties"
JAR_VERSION=3.0.0

# The inputs are in a tuple that contains: (#tag #baseDir #matches_subdir)
INPUTS1=(2.2.0 /nrs/neuronbridge/v2.2 hemibrain1.2.1-vs-split_gal4)
INPUTS2=(2.3.0 /nrs/neuronbridge/v2.3.0/brain hemibrain1.2.1-vs-mcfo)
INPUTS3=(2.4.0 /nrs/neuronbridge/v2.4.0/brain hemibrain_1_2_1-vs-annotator_gen1_mcfo_brain)

IMPORT_INPUTS=(${INPUTS1[@]})
TAG="--tag  ${IMPORT_INPUTS[0]}"

MATCHES_DIR=${IMPORT_INPUTS[1]}/cdsresults.ga/${IMPORT_INPUTS[2]}

PROD_CONFIG="--config local/proddb-config.properties"
DEV_CONFIG="--config local/devdb-config.properties"
RUNNER=echo

${RUNNER} java ${DEBUG_OPTS} ${LOG_OPTS} \
    -jar target/colormipsearch-${JAR_VERSION}-jar-with-dependencies.jar \
    legacyImport \
    ${DEV_CONFIG} \
    ${TAG} \
    -r ${MATCHES_DIR} \
    --imported-neuron-tag "Created by ${IMPORT_INPUTS[0]}-${IMPORT_INPUTS[2]} import" \
    --suspicious-match-tag "Suspicious match from ${IMPORT_INPUTS[0]}-${IMPORT_INPUTS[2]} import" \
    -ps 500 \
    $*
