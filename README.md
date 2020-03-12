# Distributed Color MIP Mask Search

This is a distributed version of the [ColorMIP_Mask_Search](https://github.com/JaneliaSciComp/ColorMIP_Mask_Search) Fiji plugin, running on Apache Spark. 

## Build

```./mvnw install```

This will produce a jar called `target/colormipsearch-<VERSION>-jar-with-dependencies.jar` which can be run with Spark.

## Run

### Group mips by line name or by skeleton id
```
java -cp target/colormipsearch-1.1-jar-with-dependencies.jar org.janelia.colormipsearch.ExtractColorMIPsMetadata groupMIPS --jacsURL http://goinac-ws1.int.janelia.org:8800/api/rest-v2 --authorization "Bearer PutTheTokenHere" -l flyem_hemibrain flylight_splitgal4_drivers -od local/testData
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
    batch \
    -locally \
    -m /groups/jacs/jacsDev/devstore/goinac/cdtest/input/flyem_hemibrain.json \
    -i "/groups/jacs/jacsDev/devstore/goinac/cdtest/input/flylight_splitgal4_drivers.json:4:1" \
    --maskThreshold 100 \
    -od /groups/jacs/jacsDev/devstore/goinac/cdtest/test3 \
    -gd /nrs/scicompsoft/otsuna/SS_vol/Gradient
```

### Perform color depth search for one mask and one image only
```
java  -Xmx120G -Xms120G -jar target/colormipsearch-1.1-jar-with-dependencies.jar batch -locally -i /nrs/jacs/jacsData/filestore/system/ColorDepthMIPs/JRC2018_Unisex_20x_HR/flylight_splitgal4_drivers/GMR_MB242A-20121208_31_H4-20x-Brain-JRC2018_Unisex_20x_HR-1846510864534863970-CH1_CDM.png -m /nrs/jacs/jacsData/filestore/system/ColorDepthMIPs/JRC2018_Unisex_20x_HR/flyem_hemibrain/5901194966_RT_18U.tif --maskThreshold 100 -od /groups/jacs/jacsDev/devstore/goinac/cdtest/test3
```
