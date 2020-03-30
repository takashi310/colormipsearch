# Distributed Color MIP Mask Search

This is a distributed version of the [ColorMIP_Mask_Search](https://github.com/JaneliaSciComp/ColorMIP_Mask_Search) Fiji plugin, running on Apache Spark. 

## Build

```./mvnw install```
or just
```./mvnw package```

This will produce a jar called `target/colormipsearch-<VERSION>-jar-with-dependencies.jar` which can be run 
either with Spark or on the local host or on the cluster by using bsub.

## Run

### Group mips by line name or by skeleton id
```
java -cp target/colormipsearch-1.1-jar-with-dependencies.jar org.janelia.colormipsearch.ExtractColorMIPsMetadata \
    groupMIPS \
    --jacsURL http://goinac-ws1.int.janelia.org:8800/api/rest-v2 \
    --authorization "Bearer PutTheTokenHere" \
    -l flyem_hemibrain flylight_splitgal4_drivers \
    -od local/testData
```

### Create input for color depth search
```
java -cp target/colormipsearch-1.1-jar-with-dependencies.jar org.janelia.colormipsearch.ExtractColorMIPsMetadata \
     prepareCDSArgs \
     --jacsURL http://goinac-ws1.int.janelia.org:8800/api/rest-v2 \
     --authorization "Bearer PutTheTokenHere" \
    -l flylight_gen1_mcfo_case_1 flyem_hemibrain \
    -od local/testData \
    --segmented-mips-base-dir /nrs/scicompsoft/otsuna/SS_vol/CDM
```

### Run color depth search locally
```
java  -Xmx120G -Xms120G -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    searchFromJSON \
    -m /groups/jacs/jacsDev/devstore/goinac/cdtest/input/flyem_hemibrain.json \
    -i "/groups/jacs/jacsDev/devstore/goinac/cdtest/input/flylight_splitgal4_drivers.json:4:1" \
    --maskThreshold 100 \
    -od /groups/jacs/jacsDev/devstore/goinac/cdtest/test3 \
    -gd /nrs/scicompsoft/otsuna/SS_vol/Gradient
```

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

### Sorting the results
```
java -jar target/colormipsearch-1.1-jar-with-dependencies.jar \
    sortResults \
    -rf local/testData/results.withscore/qq.json \
    -rd local/testData/results.sorted
```
