# Distributed Color MIP Mask Search

This is a distributed version of the [ColorMIP_Mask_Search](https://github.com/JaneliaSciComp/ColorMIP_Mask_Search) Fiji plugin, running on Apache Spark. 

## Build

```mvn install```

This will produce a jar called `target/colormipsearch-<VERSION>-jar-with-dependencies.jar` which can be run with Spark.

## Run

The class org.janelia.colormipsearch.SparkMaskSearch contains the main method, and usage information about the parameters.

