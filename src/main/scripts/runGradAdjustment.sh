#!/bin/bash

CDGA_INPUT_PARAM=$1
shift

CDGA_OUTPUT_PARAM=$1
shift

CD_GRADIENTS_LOCATION=/nrs/jacs/jacsData/filestore/system/40x_MCFO
#_Segmented_PackBits_forPublicRelease_gradient.zip
CD_ZGAP_LOCATION=/nrs/jacs/jacsData/filestore/system/40x_MCFO
#_Segmented_PackBits_forPublicRelease_20pxRGBMAX.zip

MEM_RESOURCE=$((${MEM_RESOURCE:=180}))
TOP_RESULTS=$((${TOP_RESULTS:=100}))
SAMPLES_PER_LINE=$((${SAMPLES_PER_LINE:=1}))
LIB_PARTITION_SIZE=$((${LIB_PARTITION_SIZE:=100}))

NEGATIVE_RADIUS=20
MASK_THRESHOLD=20

DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
MEM_OPTS="-Xmx${MEM_RESOURCE}G -Xms${MEM_RESOURCE}G"
LOG_OPTS="-Dlog4j.configuration=file://$PWD/local/log4j.properties"
CD_GRAD_OPTS="-gp ${CD_GRADIENTS_LOCATION}"
CD_ZGAP_OPTS="-zgp ${CD_ZGAP_LOCATION} --zgapSuffix _20pxRGBMAX"

cmd="java ${DEBUG_OPTS} ${MEM_OPTS} ${LOG_OPTS} \
    -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    gradientScore \
    --maskThreshold ${MASK_THRESHOLD} \
    --negativeRadius ${NEGATIVE_RADIUS} \
    --mirrorMask \
    --cdsConcurrency 0 \
    --topPublishedNameMatches ${TOP_RESULTS} \
    --topPublishedSampleMatches ${SAMPLES_PER_LINE} \
    --libraryPartitionSize ${LIB_PARTITION_SIZE} \
    ${CD_GRAD_OPTS} \
    ${CD_ZGAP_OPTS} \
    -rd ${CDGA_INPUT_PARAM} \
    -od ${CDGA_OUTPUT_PARAM} \
    $*"

echo "Running: ${cmd}"
($cmd)
