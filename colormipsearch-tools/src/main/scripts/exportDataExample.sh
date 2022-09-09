#!/bin/bash

JAR_VERSION=3.0.0

AREA=brain
# EXPORT_TYPE can be one of [EM_CD_MATCHES, LM_CD_MATCHES, EM_PPP_MATCHES, EM_MIPS, LM_MIPS]
EXPORT_TYPE=EM_PPP_MATCHES
OUTPUT_DIR=/nrs/neuronbridge/v3.0.0/${AREA}
CONFIG="--config local/proddb-config.properties"
RUNNER=

# Typically no change is required below this point
EM_HEMIBRAIN_LIB=flyem_hemibrain_1_2_1
EM_VNC_LIB=flyem_vnc_0_6

SGAL4_LIB=flylight_split_gal4_published
MCFO_LIB=flylight_gen1_mcfo_published
ANNOTATOR_MCFO_LIB=flylight_annotator_gen1_mcfo_published

LM_LIBS="${SGAL4_LIB} ${MCFO_LIB} ${ANNOTATOR_MCFO_LIB}"
EM_LIBS="${EM_HEMIBRAIN_LIB} ${EM_VNC_LIB}"

case $EXPORT_TYPE in
  EM_CD_MATCHES)
    LIBNAME="${EM_LIBS}"
    SUBDIR=cdmatches/em-vs-lm
    ;;
  LM_CD_MATCHES)
    LIBNAME="${LM_LIBS}"
    SUBDIR=cdmatches/lm-vs-em
    ;;
  EM_PPP_MATCHES)
    LIBNAME="${EM_LIBS}"
    SUBDIR=pppmatches/em-vs-lm
    ;;
  EM_MIPS)
    LIBNAME="${EM_LIBS}"
    SUBDIR=mips/embodies
    ;;
  LM_MIPS)
    LIBNAME="${LM_LIBS}"
    SUBDIR=mips/lmlines
    ;;
  *)
    echo "Invalid export type: ${EXPORT_TYPE}"
    exit 1
    ;;
esac

case ${AREA} in
  brain)
    ALIGNMENT_SPACE=JRC2018_Unisex_20x_HR
    ;;
  vnc)
    ALIGNMENT_SPACE=JRC2018_VNC_Unisex_40x_DS
    ;;
  *)
    echo "Invalid area: ${AREA}"
    exit 1
    ;;
esac

# AS: "JRC2018_Unisex_20x_HR", "JRC2018_VNC_Unisex_40x_DS"
AS_ARG="-as ${ALIGNMENT_SPACE}"

$RUNNER java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
    -Xmx270G -Xms270G \
    -jar target/colormipsearch-${JAR_VERSION}-jar-with-dependencies.jar \
    --cacheSize 20000 \
    exportData \
    ${CONFIG} \
    ${AS_ARG} \
    --exported-result-type ${EXPORT_TYPE} \
    --jacs-url http://e03u04.int.janelia.org:8800/api/rest-v2 \
    --authorization "APIKEY MyKey" \
    -l ${LIBNAME} \
    --read-batch-size 2000 \
    -ps 50 \
    --relativize-urls-to-component 1 \
    -od ${OUTPUT_DIR} \
    --subdir ${SUBDIR} \
    --offset 0 --size 0 \
    $*
