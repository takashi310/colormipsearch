DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
LOG_OPTS="-Dlog4j.configuration=file://$PWD/local/log4j.properties"
JAR_VERSION=3.0.0

SOURCE_PPP_RESULTS=/nrs/saalfeld/maisl/flymatch/all_hemibrain_1.2_NB/setup22_nblast_20/results
PPP_RES_SUBDIR=lm_cable_length_20_v4_iter_2_tanh

NEURON_DIR=${SOURCE_PPP_RESULTS}
RES_SUBDIR=${PPP_RES_SUBDIR}

NEURON_SUBDIRS="\
00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 \
20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 \
40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 \
60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 \
80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99"

MISSING="06 08 22 24 31 61 73 79 85 96"

DEV_CONFIG="--config local/devdb-config.properties"
PROD_CONFIG="--config local/proddb-config.properties"
RUNNER=

for nd in ${NEURON_SUBDIRS} ; do
    echo "$(date) Process dir ${NEURON_DIR}/${nd}"
    ${RUNNER} java ${LOG_OPTS} \
        -jar target/colormipsearch-${JAR_VERSION}-jar-with-dependencies.jar \
        importPPPResults \
        ${PROD_CONFIG} \
        -as JRC2018_Unisex_20x_HR \
        --em-library flyem_hemibrain_1_2_1 \
        --lm-library flylight_gen1_mcfo_published \
        -rd ${NEURON_DIR}/${nd} \
        --neuron-matches-sub-dir ${RES_SUBDIR} \
        -ps 20 \
        --only-best-skeleton-matches \
        --processing-tag 2.3.0 \
        $*
    echo "$(date) Completed dir ${NEURON_DIR}/${nd}"
done
