package org.janelia.colormipsearch.cmd_v2;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.api_v2.cdmips.MIPImage;
import org.janelia.colormipsearch.api_v2.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api_v2.cdmips.MIPsUtils;
import org.janelia.colormipsearch.api_v2.cdsearch.ColorMIPSearch;
import org.janelia.colormipsearch.imageprocessing.ImageArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
abstract class AbstractColorDepthSearchCmd extends AbstractCmd {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractColorDepthSearchCmd.class);

    AbstractColorDepthSearchCmd(String commandName) {
        super(commandName);
    }

    void saveCDSParameters(ColorMIPSearch colorMIPSearch, Path outputDir, String fname) {

        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        if (outputDir != null) {
            File outputFile = outputDir.resolve(fname).toFile();
            try {
                mapper.writerWithDefaultPrettyPrinter().
                        writeValue(outputFile, colorMIPSearch.getCDSParameters());
            } catch (IOException e) {
                LOG.error("Error persisting color depth search parameters to {}", outputFile, e);
                throw new IllegalStateException(e);
            }
        }
    }

    ImageArray<?> loadQueryROIMask(String queryROIMask) {
        if (StringUtils.isBlank(queryROIMask)) {
            return null;
        } else {
            List<MIPMetadata> queryROIMIPs = MIPsUtils.readMIPsFromLocalFiles(queryROIMask, 0, 1, Collections.emptySet());
            return queryROIMIPs.stream()
                    .findFirst()
                    .map(MIPsUtils::loadMIP)
                    .map(MIPImage::getImageArray)
                    .orElse(null);
        }
    }

}
