package org.janelia.colormipsearch;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplaceURLsCommand {
    private static final Logger LOG = LoggerFactory.getLogger(ReplaceURLsCommand.class);

    @Parameters(commandDescription = "Replace image URLs from the source MIPs to the URLs from the target MIPs")
    static class ReplaceURLsArgs {
        @Parameter(names = {"--source-mips", "-src"}, required = true,
                description = "File containing the MIPS whose image URLs will change")
        private String sourceMIPsFilename;
        
        @Parameter(names = {"--target-mips", "-target"}, required = true,
                description = "File containing the MIPS with the image URLs")
        private String targetMIPsFilename;
        
        @Parameter(names = {"--input-dirs"}, variableArity = true, description = "Directory with JSON files whose image URLs have to be changed")
        List<String> inputDirs;

        @Parameter(names = {"--input-files"}, variableArity = true, description = "JSON file whose image URLs have to be changed")
        List<String> inputFiles;

        @ParametersDelegate
        final CommonArgs commonArgs;

        ReplaceURLsArgs(CommonArgs commonArgs) {
            this.commonArgs = commonArgs;
        }
        
        boolean validate() {
            return CollectionUtils.isNotEmpty(inputDirs) || CollectionUtils.isNotEmpty(inputFiles);
        }

        Path getOutputDir() {
            if (StringUtils.isNotBlank(commonArgs.outputDir)) {
                return Paths.get(commonArgs.outputDir);
            } else {
                return null;
            }
        }

    }

    private final ReplaceURLsArgs args;

    ReplaceURLsCommand(CommonArgs commonArgs) {
        args =  new ReplaceURLsArgs(commonArgs);
    }

    ReplaceURLsArgs getArgs() {
        return args;
    }

    void execute() {
        replaceMIPsURLs(args);
    }

    private void replaceMIPsURLs(ReplaceURLsArgs args) {
        Map<String, MIPInfo> indexedSourceMIPs = CmdUtils.readMIPsFromJSON(args.sourceMIPsFilename, 0, -1, Collections.emptySet())
                .stream()
                .collect(Collectors.toMap(mipInfo -> StringUtils.defaultIfBlank(mipInfo.relatedImageRefId, mipInfo.id), Function.identity()));

        Map<String, MIPInfo> indexedTargetMIPs = CmdUtils.readMIPsFromJSON(args.targetMIPsFilename, 0, -1, Collections.emptySet())
                .stream()
                .collect(Collectors.toMap(mipInfo -> StringUtils.defaultIfBlank(mipInfo.relatedImageRefId, mipInfo.id), Function.identity()));

        List<String> inputFileNames;
        if (CollectionUtils.isNotEmpty(args.inputFiles)) {
            inputFileNames = args.inputFiles;
        } else if (CollectionUtils.isNotEmpty(args.inputDirs)) {
            inputFileNames = args.inputDirs.stream()
                    .flatMap(rd -> {
                        try {
                            return Files.find(Paths.get(rd), 1, (p, fa) -> fa.isRegularFile());
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .map(p -> p.toString())
                    .collect(Collectors.toList());
        } else {
            inputFileNames = Collections.emptyList();
        }
        replaceMIPsURLs(inputFileNames, indexedSourceMIPs, indexedTargetMIPs, args.getOutputDir());
    }

    private void replaceMIPsURLs(List<String> inputFileNames, Map<String, MIPInfo> indexedSourceMIPs, Map<String, MIPInfo> indexedTargetMIPs, Path outputDir) {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        inputFileNames.stream().parallel()
                .forEach(fn -> {
                    File f = new File(fn);
                    Results<List<Map<String, Object>>> content = readJSONFile(f, mapper);
                    content.results.forEach(e -> {
                        String id = (String) e.get("id");
                        String imageURL = (String) e.get("image_path");
                        String thumbnailURL = (String) e.get("thumbnail_path");

                        MIPInfo targetMIP = indexedTargetMIPs.get(id);
                        if (targetMIP == null) {
                            LOG.warn("No target URLs found for {}", id);
                        } else if (StringUtils.isBlank(targetMIP.imageURL) || StringUtils.isBlank(targetMIP.thumbnailURL)) {
                            LOG.warn("Not all target image URLs are available for {} -> {}, {}", id, targetMIP.imageURL, targetMIP.thumbnailURL);
                        } else {
                            MIPInfo srcMIP = indexedSourceMIPs.get(id);
                            if (srcMIP == null) {
                                LOG.warn("No source URLS found for {} for validation", id);
                            } else {
                                if (!StringUtils.equals(imageURL, srcMIP.imageURL)) {
                                    LOG.info("Source image URL is different for {}: expected {} but was {}", id, srcMIP.imageURL, imageURL);
                                }
                                if (!StringUtils.equals(thumbnailURL, srcMIP.thumbnailURL)) {
                                    LOG.info("Source thumbnail URL is different for {}: expected {} but was {}", id, srcMIP.thumbnailURL, thumbnailURL);
                                }
                            }
                            e.put("image_path", targetMIP.imageURL);
                            e.put("thumbnail_path", targetMIP.thumbnailURL);
                        }
                    });
                    writeJSONFile(content, CmdUtils.getOutputFile(outputDir, f), mapper);
                });
    }

    Results<List<Map<String, Object>>> readJSONFile(File f, ObjectMapper mapper) {
        try {
            LOG.info("Reading {}", f);
            return mapper.readValue(f, new TypeReference<Results<List<Map<String, Object>>>>() {
            });
        } catch (IOException e) {
            LOG.error("Error reading json file {}", f, e);
            throw new UncheckedIOException(e);
        }
    }

    void writeJSONFile(Results<List<Map<String, Object>>> content, File f, ObjectMapper mapper) {
        try {
            if (f == null) {
                mapper.writerWithDefaultPrettyPrinter().writeValue(System.out, content);
            } else {
                LOG.info("Writing {}", f);
                mapper.writerWithDefaultPrettyPrinter().writeValue(f, content);
            }
        } catch (IOException e) {
            LOG.error("Error writing json file {}", f, e);
            throw new UncheckedIOException(e);
        }
    }

}
