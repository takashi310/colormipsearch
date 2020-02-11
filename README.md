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
java -cp target/colormipsearch-1.1-jar-with-dependencies.jar org.janelia.colormipsearch.ExtractColorMIPsMetadata prepareCDSArgs --jacsURL http://goinac-ws1.int.janelia.org:8800/api/rest-v2 --authorization "Bearer PutTheTokenHere" -l flylight_gen1_mcfo_case_1 flyem_hemibrain -od local/testData
```

### Run color depth search locally
```
java  -Xmx120G -Xms120G -jar target/colormipsearch-1.1-jar-with-dependencies.jar -locally -m /groups/jacs/jacsDev/devstore/goinac/cdtest/input/flyem_hemibrain.json -i "/groups/jacs/jacsDev/devstore/goinac/cdtest/input/flylight_splitgal4_drivers.json:4:1" --maskThreshold 100 -od /groups/jacs/jacsDev/devstore/goinac/cdtest/test3
```

