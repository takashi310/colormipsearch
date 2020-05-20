# Distributed Color MIP Mask Search

This is a distributed version of the [ColorMIP_Mask_Search](https://github.com/JaneliaSciComp/ColorMIP_Mask_Search) Fiji plugin, running on Apache Spark. 

## Build

```./mvnw install```
or just
```./mvnw package```

This will produce a jar called `target/colormipsearch-<VERSION>-jar-with-dependencies.jar` which can be run 
either with Spark or on the local host or on the cluster by using bsub.

## Run

### Perform color depth search for one mask and one image only
```
java  -Xmx120G -Xms120G -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    searchLocalFiles \
    -i /nrs/jacs/jacsData/filestore/system/ColorDepthMIPs/JRC2018_Unisex_20x_HR/flylight_splitgal4_drivers/GMR_MB242A-20121208_31_H4-20x-Brain-JRC2018_Unisex_20x_HR-1846510864534863970-CH1_CDM.png \
    -m /nrs/jacs/jacsData/filestore/system/ColorDepthMIPs/JRC2018_Unisex_20x_HR/flyem_hemibrain/5901194966_RT_18U.tif \
    --maskThreshold 100 \
    -od /groups/jacs/jacsDev/devstore/goinac/cdtest/test3
```

Here's another example to compare images from a zip archive in which we compare the first 1000
images from `/nrs/jacs/jacsData/filestore/system/flylight_40xMCFO_Segmented_PackBits.zip` with 
all images from `/groups/scicomp/scicompsoft/otsuna/Brain/EM_Hemibrain/Masayoshi_selected.zip`

```
java -Xmx480G -Xms480G -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    searchLocalFiles \
    -i /nrs/jacs/jacsData/filestore/system/flylight_40xMCFO_Segmented_PackBits.zip:0:1000 \
    -m /groups/scicomp/scicompsoft/otsuna/Brain/EM_Hemibrain/Masayoshi_selected.zip \
    -od local/testData/masayoshi-cds \
    -gp /nrs/jacs/jacsData/filestore/system/flylight_40xMCFO_gradient_20px.zip \
    -result masayoshiResult-0-1000 \
    -lps 1000
```

### Calculating the gradient score for a set of existing results
```
java -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    gradientScore \
    -rf local/testData/results/qq.json \
    -gp local/testData/flylight_40xMCFO_gradient_20px.zip \
    -rd local/testData/results.withscore
```

## Pre-computed color depth search results

A more detailed description can be found in [PrecomputedData.md](PrecomputedData.md).

## Generating EM - LM precomputed color depth search

This section shows the steps and the commands used for generating 
the precomputed EM vs LM color depth search results

### Generate the MIPs metadata for LM lines and EM bodies

This step actually is not important for the color depth search
but it is important for line or body lookup.
 
####Generate LM SplitGal4 MIPs:
```bash
java \
    -cp target/colormipsearch-1.1-jar-with-dependencies.jar \
    org.janelia.colormipsearch.cmd.ExtractColorMIPsMetadata \
    groupMIPS \
    --jacsURL http://goinac-ws1.int.janelia.org:8800/api/rest-v2 \
    --authorization "Bearer tokenvalue" \
    -l flylight_split_gal4_published \
    --segmented-image-handling 0 \
    -od local/testData/mips \
    -lmdir split_gal4_lines
```

####Generate LM MCFO MIPs:
```bash
java \
    -cp target/colormipsearch-1.1-jar-with-dependencies.jar \
    org.janelia.colormipsearch.cmd.ExtractColorMIPsMetadata \
    groupMIPS \
    --jacsURL http://goinac-ws1.int.janelia.org:8800/api/rest-v2 \
    --authorization "Bearer tokenvalue" \
    -l flylight_gen1_mcfo_published \
    --segmented-image-handling 0 \
    -od local/testData/mips \
    -lmdir gen1_mcfo_lines
```

####Generate EM MIPs:
```bash
java \
    -cp target/colormipsearch-1.1-jar-with-dependencies.jar \
    org.janelia.colormipsearch.cmd.ExtractColorMIPsMetadata \
    groupMIPS \
    --jacsURL http://goinac-ws1.int.janelia.org:8800/api/rest-v2 \
    --authorization "Bearer tokenvalue" \
    -l flyem_hemibrain \
    -od local/testData/mips \
    -emdir em_bodies
```

### Generate EM - LM color depth search results

#### Step 1: Prepare LM and EM JSON input for color depth search

For MCFO and SplitGal4 input we use the corresponding segmented images
as input (see the '--segmented-mips-base-dir' argument).

Prepare MCFO input:
```bash
java \
    -cp target/colormipsearch-1.1-jar-with-dependencies.jar \
    org.janelia.colormipsearch.cmd.ExtractColorMIPsMetadata \
    prepareCDSArgs \
    --jacsURL http://goinac-ws1.int.janelia.org:8800/api/rest-v2 \
    --authorization "Bearer tokenvalue" \
    -l flylight_gen1_mcfo_published \
    --segmented-mips-base-dir /nrs/jacs/jacsData/filestore/system/40x_MCFO_Segmented_PackBits_forPublicRelease.zip \
    --segmented-image-handling 0 \
    -od local/testData/cdsresults \
    $*
```
Prepare SplitGal4 input:
```bash
java \
    -cp target/colormipsearch-1.1-jar-with-dependencies.jar \
    org.janelia.colormipsearch.cmd.ExtractColorMIPsMetadata \
    prepareCDSArgs \
    --jacsURL http://goinac-ws1.int.janelia.org:8800/api/rest-v2 \
    --authorization "Bearer tokenvalue" \
    -l flylight_split_gal4_published \
    --segmented-mips-base-dir /nrs/jacs/jacsData/filestore/system/SS_Split/SS_Split_ALL_Segmented_CDM \
    --segmented-image-handling 0 \
    -od local/testData/cdsresults \
    $*
```
Prepare EM input:
```bash
java \
    -cp target/colormipsearch-1.1-jar-with-dependencies.jar \
    org.janelia.colormipsearch.cmd.ExtractColorMIPsMetadata \
    prepareCDSArgs \
    --jacsURL http://goinac-ws1.int.janelia.org:8800/api/rest-v2 \
    --authorization "Bearer tokenvalue" \
    -l flyem_hemibrain \
    -od local/testData/cdsresults \
    $*
```

#### Step 2: Generate the color depth search results

Even though this process is run only once it outputs two sets of result files:
one for the EM -> LM results and one for the LM -> EM results, 
indexed by the EM mip IDs or LM mip IDs respectively.

For EM vs MCFO this requires using the grid, otherwise it will
take too long. For EM vs SplitGal4 this can run on a host that has the 
proper resources.

Calculate EM vs SplitGal4 results
```bash
java -Xmx180G -Xms180G \
    -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    searchFromJSON  \
    -m local/testData/cdsresults/flyem_hemibrain.json:0:5000 \
    -i local/testData/cdsresults/flylight_gen1_mcfo_published.json:0:40000 \
    --maskThreshold 20 \
    --dataThreshold 30 \
    --xyShift 2 \
    --pixColorFluctuation 1 \
    --pctPositivePixels 1 \
    --mirrorMask \
    --libraryPartitionSize 4000 \
    --perMaskSubdir flyem_hemibrain-vs-gen1_mcfo \
    --perLibrarySubdir flylight_gen1_mcfo_published \
    -od local/testData/cdsresults
```

```bash
java -Xmx180G -Xms180G \
    -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    searchFromJSON  \
    -m local/testData/cdsresults/flyem_hemibrain.json:0:35000 \
    -i local/testData/cdsresults/flylight_split_gal4_published.json:0:7800 \
    --maskThreshold 20 \
    --dataThreshold 30 \
    --xyShift 2 \
    --pixColorFluctuation 1 \
    --pctPositivePixels 1 \
    --mirrorMask \
    --libraryPartitionSize 4000 \
    --perMaskSubdir flyem_hemibrain-vs-split_gal4 \
    --perLibrarySubdir flylight_split_gal4_published \
    -od local/testData/cdsresults
```

#### Step 3: Calculate gradient score for the EM -> LM color depth search results
```bash
java -Xmx240G -Xms240G \
    -Dlog4j.configuration=file:///groups/scicompsoft/home/goinac/Work/color-depth-spark/local/log4j.properties \
    -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    --cacheSize 5000 --cacheExpirationInMin 1 \
    gradientScore \
    --maskThreshold 20
    --negativeRadius 20 \
    --mirrorMask \
    --topPublishedNameMatches 500 \
    --topPublishedSampleMatches 0 \
    --libraryPartitionSize 5 \
    -gp /nrs/jacs/jacsData/filestore/system/40x_MCFO_Segmented_PackBits_forPublicRelease_gradient.zip \
    -zgp /nrs/jacs/jacsData/filestore/system/40x_MCFO_Segmented_PackBits_forPublicRelease_20pxRGBMAX.zip --zgapSuffix _20pxRGBMAX \
    -rd local/testData/cdsresults/flyem_hemibrain-vs-gen1_mcfo:1400:20 \
    -od local/testData/cdsresults.ga/flyem_hemibrain-vs-gen1_mcfo
```

```bash
java -Xmx240G -Xms240G \
    -Dlog4j.configuration=file:///groups/scicompsoft/home/goinac/Work/color-depth-spark/local/log4j.properties \
    -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    --cacheSize 5000 --cacheExpirationInMin 1 \
    gradientScore \
    --maskThreshold 20
    --negativeRadius 20 \
    --mirrorMask \
    --topPublishedNameMatches 500 \
    --topPublishedSampleMatches 0 \
    --libraryPartitionSize 5 \
    -gp /nrs/jacs/jacsData/filestore/system/SS_Split/SS_Split_ALL_Segmented_gradient \
    -zgp /nrs/jacs/jacsData/filestore/system/SS_Split/SS_Split_ALL_Segmented_20pxRGB --zgapSuffix _20pxRGB \
    -rd local/testData/cdsresults/flyem_hemibrain-vs-split_gal4:1400:100 \
    -od local/testData/cdsresults.ga/flyem_hemibrain-vs-split_gal4
```

#### Step 4: Update the gradient score for the LM -> EM color depths search results

This step actually transfers the gradient score from the 
ones calculated for EM -> LM results to the corresponding LM -> EM results.

```bash
java -Xms480G -Xmx480G \
    -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    gradientScoresFromMatchedResults \
    -rd local/testData/cdsresults/flylight_gen1_mcfo_published \
    -revd local/testData/cdsresults.ga/flyem_hemibrain-vs-gen1_mcfo \
    -od local/testData/cdsresults.ga/flylight_gen1_mcfo_published \
    -ps 500
```

```bash
java -Xms480G -Xmx480G \
    -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    gradientScoresFromMatchedResults \
    -rd local/testData/cdsresults/flylight_split_gal4_published \
    -revd local/testData/cdsresults.ga/flyem_hemibrain-vs-split_gal4 \
    -od local/testData/cdsresults.ga/flylight_split_gal4_published \
    -ps 500
```

#### Step 5: Merge flyem results

When we search EM matches we want to see both Spligal4 matches and MCFO matches,
therefore the flyem-vs-mcfo and flyem-vs-sgal4 will have to be merged in a single 
directory.
```bash
java \
    -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    mergeResults \
    -rd \
    local/testData/cdsresults.ga/flyem_hemibrain-vs-gen1_mcfo \
    local/testData/cdsresults.ga/flyem_hemibrain-vs-split_gal4 \
    -od local/testData/cdsresults.merged/flyem_hemibrain-vs-flylight
```

#### Step 5': Replace image URLs

This step requires a JSON file that contains the new image URLs 
that will replace the ones that are set in the JSON created from 
'flylight_gen1_mcfo_published' library. The new URLs for example could
come from the MIPs generated with a different gamma value. There are 
two sets of JSON files that will have to have the imageURLs update:
 * the MCFO lines will have to be changed to reference the new URLs
 * the EM to MCFO color depth search results reference the image of 
 the MCFO MIP that matches the EM MIP entry, so these will have to be updated
 to reference the new URL.

```bash
java \
    -cp target/colormipsearch-1.1-jar-with-dependencies.jar \
    org.janelia.colormipsearch.cmd.ExtractColorMIPsMetadata \
    prepareCDSArgs \
    --jacsURL http://goinac-ws1.int.janelia.org:8800/api/rest-v2 \
    --authorization "Bearer tokenvalue" \
    -l flylight_gen1_mcfo_case_1_gamma1_4 \
    -od local/testData/cdsresults \
    $*
```

Update image URLs for the lines:
```bash
java \
    -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    replaceImageURLs \
    -src local/testData/cdsresult/flylight_gen1_mcfo_published.json \
    -target local/testData/cdsresult/flylight_gen1_mcfo_case_1_gamma1_4.json \
    --input-dirs local/testData/mips/gen1_mcfo_lines \
    --result-id-field id \
    -od local/testData/mips.gamma1.4/gen1_mcfo_lines
```

Update image URLs for the color depth results:
```bash
java \
    -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    replaceImageURLs \
    -src local/testData/cdsresult/flylight_gen1_mcfo_published.json \
    -target local/testData/cdsresult/flylight_gen1_mcfo_case_1_gamma1_4.json \
    --input-dirs local/testData/cdsresults.merged/flyem_hemibrain-vs-flylight \
    --result-id-field matchedId  \
    -od local/testData/updateURLS/cdsresults/flyem_hemibrain
```

#### Step 6: Normalize and rank results:
```bash
java \
    -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    normalizeGradientScores \
    -rd local/testData/updateURLS/cdsresults/flyem_hemibrain-vs-flylight \
    --pctPositivePixels 2.0 \
    -cleanup \
    -od local/testData/cdsresults.normalized/flyem_hemibrain-vs-flylight
```

```bash
java \
    -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    normalizeGradientScores \
    -rd local/testData/cdsresults.ga/flylight_split_gal4_published \
    --pctPositivePixels 2.0 \
    -cleanup \
    -od local/testData/cdsresults.normalized/flylight_split_gal4_published
```

```bash
java \
    -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    normalizeGradientScores \
    -rd local/testData/cdsresults.ga/flylight_gen1_mcfo_published \
    --pctPositivePixels 2.0 \
    -cleanup \
    -od local/testData/cdsresults.normalized/flylight_gen1_mcfo_published
```

#### Step 7: Upload to AWS S3

This step requires aws-cli installed on the host from which we upload the
data to S3.

Upload MIPs

```bash
# generate publishedNames.txt
find . local/testData/mips \
    -type f \
    -name "*.json" \
    -printf "%f\n" > publishedNames.txt
aws s3 cp publishedNames.txt s3://janelia-neuronbridge-data-prod/publishedNames.txt

# create and upload data version file
echo 1.0.0 > local/testData/DATA_VERSION

aws s3 cp \
    local/testData/DATA_VERSION \
    s3://janelia-neuronbridge-data-prod/metadata/cdsresults/DATA_VERSION

# upload MCFO lines
aws s3 cp \
    local/testData/mips.gamma1.4/gen1_mcfo_lines \
    s3://janelia-neuronbridge-data-prod/metadata/by_line --recursive
# upload SplitGal4 lines
aws s3 cp \
    local/testData/mips/split_gal4_lines \
    s3://janelia-neuronbridge-data-prod/metadata/by_line --recursive
# upload EM skeletons
aws s3 cp \
    local/testData/mips/em_bodies \
    s3://janelia-neuronbridge-data-prod/metadata/by_body --recursive
```

Upload Results

```bash
aws s3 cp \
    local/testData/cdsresults.normalized/flyem_hemibrain-vs-flylight \
    s3://janelia-neuronbridge-data-prod/metadata/cdsresults --recursive

aws s3 cp \
    local/testData/cdsresults.normalized/flylight_split_gal4_published \
    s3://janelia-neuronbridge-data-prod/metadata/cdsresults --recursive

aws s3 cp \
    local/testData/cdsresults.normalized/flylight_gen1_mcfo_published \
    s3://janelia-neuronbridge-data-prod/metadata/cdsresults --recursive
```