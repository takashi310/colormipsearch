#!/bin/bash

is_range_re=':[0-9]+(:[0-9]+)?$'
is_number_re='^[0-9]+$'

if [[ $1 =~ $is_range_re ]] ; then
  mask_range="$1"
  shift
else
  mask_range=""
fi

if [[ $1 =~ $is_range_re ]] ; then
  input_range="$1"
  shift
else
  input_range=""
fi

if [[ $1 =~ $is_number_re ]] ; then
  app_memory="$1"
  shift
else
  app_memory="180"
fi

cmd="java -Xmx${app_memory}G -Xms${app_memory}G \
    -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    searchFromJSON \
    --maskThreshold 20 \
    --dataThreshold 30 \
    --xyShift 2 \
    --pixColorFluctuation 1 \
    --pctPositivePixels 2 \
    --mirrorMask \
    --libraryPartitionSize 200 \
    --cdsConcurrency 0 \
    $*"

echo "Running: ${cmd}"
($cmd)
