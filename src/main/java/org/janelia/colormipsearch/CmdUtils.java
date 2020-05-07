package org.janelia.colormipsearch;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CmdUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CmdUtils.class);

    static Executor createCDSExecutor(AbstractArgs args) {
        if (args.cdsConcurrency > 0) {
            return Executors.newFixedThreadPool(
                    args.cdsConcurrency,
                    new ThreadFactoryBuilder()
                            .setNameFormat("CDSRUNNER-%d")
                            .setDaemon(true)
                            .setPriority(Thread.NORM_PRIORITY + 1)
                            .build());
        } else {
            return Executors.newWorkStealingPool();
        }
    }

    static void createOutputDirs(Path... outputDirs) {
        for (Path outputDir : outputDirs) {
            if (outputDir != null) {
                try {
                    // create output directory
                    Files.createDirectories(outputDir);
                } catch (IOException e) {
                    LOG.error("Error creating output directory: {}", outputDir, e);
                    System.exit(1);
                }
            }
        }
    }

    static Results<List<ColorMIPSearchResultMetadata>> readCDSResultsFromJSONFile(File f, ObjectMapper mapper) {
        try {
            LOG.debug("Reading {}", f);
            return mapper.readValue(f, new TypeReference<Results<List<ColorMIPSearchResultMetadata>>>() {
            });
        } catch (IOException e) {
            LOG.error("Error reading CDS results from json file {}", f, e);
            throw new UncheckedIOException(e);
        }
    }

    static void sortCDSResults(List<ColorMIPSearchResultMetadata> cdsResults) {
        Comparator<ColorMIPSearchResultMetadata> csrComp = (csr1, csr2) -> {
            if (csr1.getNormalizedScore() != null && csr2.getNormalizedScore() != null) {
                return Comparator.comparingDouble(ColorMIPSearchResultMetadata::getNormalizedScore)
                        .compare(csr1, csr2)
                        ;
            } else if (csr1.getNormalizedScore() == null && csr2.getNormalizedScore() == null) {
                return Comparator.comparingInt(ColorMIPSearchResultMetadata::getMatchingPixels)
                        .compare(csr1, csr2)
                        ;
            } else if (csr1.getNormalizedScore() == null) {
                // null gap scores should be at the beginning
                return -1;
            } else {
                return 1;
            }
        };
        cdsResults.sort(csrComp.reversed());
    }

    static void writeCDSResultsToJSONFile(Results<List<ColorMIPSearchResultMetadata>> cdsResults, File f, ObjectMapper mapper) {
        try {
            if (CollectionUtils.isNotEmpty(cdsResults.results)) {
                if (f == null) {
                    mapper.writerWithDefaultPrettyPrinter().writeValue(System.out, cdsResults);
                } else {
                    LOG.info("Writing {}", f);
                    mapper.writerWithDefaultPrettyPrinter().writeValue(f, cdsResults);
                }
            }
        } catch (IOException e) {
            LOG.error("Error writing CDS results to json file {}", f, e);
            throw new UncheckedIOException(e);
        }
    }

    @Nullable
    static File getOutputFile(Path outputDir, File inputFile) {
        if (outputDir == null) {
            return null;
        } else {
            return outputDir.resolve(inputFile.getName()).toFile();
        }
    }

    static List<MIPInfo> readMIPsFromJSON(String mipsFilename, int offset, int length, Set<String> filter) {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            LOG.info("Reading {}", mipsFilename);
            List<MIPInfo> content = mapper.readValue(new File(mipsFilename), new TypeReference<List<MIPInfo>>() {
            });
            if (CollectionUtils.isEmpty(filter)) {
                int from = offset > 0 ? offset : 0;
                int to = length > 0 ? Math.min(from + length, content.size()) : content.size();
                LOG.info("Read {} mips from {} starting at {} to {}", content.size(), mipsFilename, from, to);
                return content.subList(from, to);
            } else {
                LOG.info("Read {} from {} mips", filter, content.size());
                return content.stream()
                        .filter(mip -> filter.contains(mip.publishedName.toLowerCase()) || filter.contains(StringUtils.lowerCase(mip.id)))
                        .collect(Collectors.toList());
            }
        } catch (IOException e) {
            LOG.error("Error reading {}", mipsFilename, e);
            throw new UncheckedIOException(e);
        }
    }

}
