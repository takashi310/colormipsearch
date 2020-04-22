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

java -Xmx${app_memory}G -Xms${app_memory}G \
    -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    searchFromJSON \
    -m /nrs/scicompsoft/goinac/em-lm-cds/flyem_hemibrain.json${mask_range} \
    -i /nrs/scicompsoft/goinac/em-lm-cds/em-mcfo-cdsresults/flylight_gen1_mcfo_published.json${input_range} \
    --maskThreshold 20 \
    --dataThreshold 30 \
    --xyShift 2 \
    --pixColorFluctuation 1 \
    --pctPositivePixels 2 \
    --mirrorMask \
    --libraryPartitionSize 200 \
    --perMaskSubdir flyem_hemibrain \
    --perLibrarySubdir flylight_gen1_mcfo_published \
    -od /nrs/scicompsoft/goinac/em-lm-cds/em-mcfo-bsub-cdsresults \
    --cdsConcurrency 0 \
    $*
