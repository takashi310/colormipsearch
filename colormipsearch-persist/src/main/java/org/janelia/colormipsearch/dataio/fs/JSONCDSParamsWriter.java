package org.janelia.colormipsearch.dataio.fs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.dataio.CDSParamsWriter;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONCDSParamsWriter implements CDSParamsWriter {
    private static final Logger LOG = LoggerFactory.getLogger(JSONCDSParamsWriter.class);

    private final Path outputDir;
    private final ObjectMapper mapper;

    public JSONCDSParamsWriter(Path outputDir, ObjectMapper mapper) {
        this.outputDir = outputDir;
        this.mapper = mapper;
    }

    @Override
    public void writeParams(List<DataSourceParam> masksInputs,
                            List<DataSourceParam> targetsInputs,
                            Map<String, Object> params) {
        String masksInputValues = inputValues(masksInputs);
        String targetsInputValues = inputValues(targetsInputs);
        File outputFile;
        if (outputDir != null) {
            FSUtils.createDirs(outputDir);
            outputFile = outputDir.resolve("masks-" + masksInputValues + "-targets-" + targetsInputValues + "-cdsParameters.json").toFile();
        } else {
            outputFile = null;
        }
        try {
            if (outputFile != null) {
                mapper.writerWithDefaultPrettyPrinter().
                        writeValue(outputFile, params);
            } else {
                mapper.writerWithDefaultPrettyPrinter().
                        writeValue(System.out, params);
            }
        } catch (IOException e) {
            LOG.error("Error persisting color depth search parameters to {}", outputFile, e);
            throw new IllegalStateException(e);
        }

    }

    private String inputValues(List<DataSourceParam> inputs) {
        return inputs.stream()
                .map(DataSourceParam::getLocation)
                .reduce("", (l1, l2) -> StringUtils.isBlank(l1) ? l2 : l1 + "-" + l2);
    }

}
