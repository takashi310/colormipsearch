package org.janelia.colormipsearch.cmd;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;

import javassist.compiler.ast.Pair;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.colormipsearch.api.cdmips.AbstractMetadata;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api.cdmips.MIPsUtils;
import org.jboss.netty.channel.DownstreamChannelStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CopyColorDepthMIPVariantsCmd extends AbstractCmd {
    private static final Logger LOG = LoggerFactory.getLogger(CopyColorDepthMIPVariantsCmd.class);

    @Parameters(commandDescription = "Copy MIPs variants")
    static class CopyColorDepthMIPVariantsArgs extends AbstractCmdArgs {
        @Parameter(names = {"--input", "-i"}, required = true, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "JSON input MIPs")
        private ListArg inputMIPs;

        @Parameter(names = {"--mipsFilter"}, variableArity = true, description = "Filter for i input mips")
        Set<String> mipsFilter;

        @Parameter(names = {"--targetDirectory"}, description = "Input image file(s) start index")
        String targetFolder;

        @Parameter(names = {"-n"}, description = "Only show what the command is supposed to do")
        boolean simulateFlag;

        @DynamicParameter(names = "-variantMapping", description = "Variants mapping")
        private Map<String, String> variantMapping = new HashMap<>();

        @ParametersDelegate
        final CommonArgs commonArgs;

        CopyColorDepthMIPVariantsArgs(CommonArgs commonArgs) {
            this.commonArgs = commonArgs;
        }

        Path getOutputDir() {
            if (StringUtils.isNotBlank(targetFolder)) {
                return Paths.get(targetFolder);
            } else if (StringUtils.isNotBlank(commonArgs.outputDir)) {
                return Paths.get(commonArgs.outputDir);
            } else {
                return null;
            }
        }

    }

    private final CopyColorDepthMIPVariantsArgs args;

    CopyColorDepthMIPVariantsCmd(String commandName, CommonArgs commonArgs) {
        super(commandName);
        this.args = new CopyColorDepthMIPVariantsArgs(commonArgs);
    }

    @Override
    CopyColorDepthMIPVariantsArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        if (!args.simulateFlag) {
            CmdUtils.createOutputDirs(args.getOutputDir());
        }
        copyAllMIPsVariants(args);
    }

    private void copyAllMIPsVariants(CopyColorDepthMIPVariantsArgs args) {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        List<MIPWithVariantsMetadata> inputMips = readGenericMIPsFromJSON(
                args.inputMIPs.input,
                args.inputMIPs.offset,
                args.inputMIPs.length,
                CommonArgs.toLowerCase(args.mipsFilter),
                mapper
        );

        Map<String, List<MIPWithVariantsMetadata>> inputMIPsGroupedByID = inputMips.stream()
                .collect(Collectors.groupingBy(AbstractMetadata::getId, Collectors.toList()));
        LOG.info("Copy variants for {} mips", inputMIPsGroupedByID.size());

        Path outputPath = args.getOutputDir();
        if (outputPath == null) {
            LOG.info("No destination path has been specified");
            return;
        }
        BiConsumer<MIPMetadata,  Path> copyMIPVariantAction;
        if (args.simulateFlag) {
            copyMIPVariantAction = (variantMIP, target) -> {
                LOG.info("cp {} {}", variantMIP, target);
            };
        } else {
            copyMIPVariantAction = this::copyMIPVariant;
        }
        inputMIPsGroupedByID.entrySet().stream().parallel()
                .forEach(me -> {
                    // a singleton variant type is a variant type that basically has the same source for all segmentations
                    // an example would be a gamma variant which typically is not generated from a segmented image but from the original source
                    // because of that destination names for singleton variants will not be indexed either so first partition the variant types
                    // into singleton variant types and non-singleton
                    Map<String, Integer> mipsCountsByVariantType = me.getValue().stream()
                            .flatMap(mip -> mip.getVariantTypes().stream()
                                    .filter(vt -> args.variantMapping.get(vt) != null)
                                    .map(vt -> ImmutablePair.of(vt, mip.variantAsMIP(vt))))
                            .collect(Collectors.groupingBy(
                                    ImmutablePair::getLeft,
                                    Collectors.mapping(ImmutablePair::getRight, Collectors.collectingAndThen(Collectors.toSet(), Set::size))));

                    // handle singleton variants
                    Map<String, MIPMetadata> singletonVariants = mipsCountsByVariantType.entrySet().stream()
                            .filter(variantTypeCountEntry -> variantTypeCountEntry.getValue() == 1)
                            .map(Map.Entry::getKey)
                            .flatMap(vt -> me.getValue().stream().filter(mip -> mip.hasVariant(vt)).map(mip -> ImmutablePair.of(vt, mip.variantAsMIP(vt))))
                            .collect(Collectors.toMap(
                                    ImmutablePair::getLeft,
                                    ImmutablePair::getRight,
                                    (existingValue, newValue) -> {
                                        if (existingValue.equals(newValue)) {
                                            return existingValue;
                                        } else {
                                            throw new IllegalStateException(String.format("Duplicate key %s", newValue));
                                        }
                                    }));

                    singletonVariants.forEach((variant, variantMIP) -> {
                                String variantDestination = args.variantMapping.get(variant);
                                copyMIPVariantAction.accept(
                                        variantMIP,
                                        outputPath.resolve(variantDestination)
                                                .resolve(createMIPVariantName(variantMIP.getCdmPath(), variantMIP.getImagePath(), -1))
                                );
                            });

                    // copy non-singleton variants
                    Set<String> nonSingletonVariantTypes = mipsCountsByVariantType.entrySet().stream()
                            .filter(variantTypeCountEntry -> variantTypeCountEntry.getValue() > 1)
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toSet());
                    int mipIndex = 1;
                    for (MIPWithVariantsMetadata mip : me.getValue()) {
                        copyMIPVariants(mipIndex, mip, outputPath, args.variantMapping, nonSingletonVariantTypes, copyMIPVariantAction);
                        mipIndex++;
                    }
                });
    }

    private List<MIPWithVariantsMetadata> readGenericMIPsFromJSON(String mipsJSONFilename, int offset, int length, Set<String> filter, ObjectMapper mapper) {
        try {
            LOG.info("Reading {}", mipsJSONFilename);
            List<MIPWithVariantsMetadata> content = mapper.readValue(new File(mipsJSONFilename), new TypeReference<List<MIPWithVariantsMetadata>>() {
            });
            if (CollectionUtils.isEmpty(filter)) {
                int from = offset > 0 ? offset : 0;
                int to = length > 0 ? Math.min(from + length, content.size()) : content.size();
                LOG.info("Read {} mips from {} starting at {} to {}", content.size(), mipsJSONFilename, from, to);
                return content.subList(from, to);
            } else {
                LOG.info("Read {} from {} mips", filter, content.size());
                return content.stream()
                        .filter(mip -> filter.contains(mip.getPublishedName().toLowerCase()) || filter.contains(StringUtils.lowerCase(mip.getId())))
                        .collect(Collectors.toList());
            }
        } catch (IOException e) {
            LOG.error("Error reading {}", mipsJSONFilename, e);
            throw new UncheckedIOException(e);
        }
    }


    private void copyMIPVariants(int variantIndex,
                                 MIPWithVariantsMetadata mip,
                                 Path outputPath,
                                 Map<String, String> variantMapping,
                                 Set<String> variantTypes,
                                 BiConsumer<MIPMetadata, Path> action) {
        mip.getVariantTypes().stream()
                .filter(variantTypes::contains)
                .forEach(variant -> {
                    String variantDestination = variantMapping.get(variant);
                    MIPMetadata variantMIP = mip.variantAsMIP(variant);
                    action.accept(
                            variantMIP,
                            outputPath.resolve(variantDestination)
                                    .resolve(createMIPVariantName(mip.getCdmPath(), variantMIP.getImagePath(), variantIndex))
                    );
                });
    }

    private String createMIPVariantName(String cdmPath, String cdmImageVariantPath, int segmentIndex) {
        String cdmName = Paths.get(cdmPath).getFileName().toString();
        String cdmNameWithoutExt = RegExUtils.replacePattern(cdmName, "\\..*$", "");
        if (StringUtils.endsWith(cdmNameWithoutExt, "_CDM")) {
            String cdmSegmentName = StringUtils.removeEnd(cdmNameWithoutExt, "_CDM");
            return formatSegmentName(cdmSegmentName, segmentIndex, getImageExt(cdmImageVariantPath));
        } else {
            String cdmVariantName = Paths.get(cdmImageVariantPath).getFileName().toString();
            String cdmVariantNameWithoutExt = RegExUtils.replacePattern(cdmVariantName, "\\..*$", "");
            return formatSegmentName(cdmVariantNameWithoutExt, segmentIndex, getImageExt(cdmImageVariantPath));
        }
    }

    private String formatSegmentName(String segmentName, int segmentIndex, String imageExt) {
        if (segmentIndex > 0) {
            return String.format("%s-%02d_CDM%s", segmentName, segmentIndex, imageExt);
        } else {
            return String.format("%s_CDM%s", segmentName, imageExt);
        }
    }

    private String getImageExt(String imagePath) {
        Pattern imageExtPattern = Pattern.compile(".+(\\..*)$");
        Matcher imageExtMatcher = imageExtPattern.matcher(Paths.get(imagePath).getFileName().toString());
        if (imageExtMatcher.find()) {
            return imageExtMatcher.group(1);
        } else {
            return "";
        }
    }

    private void copyMIPVariant(MIPMetadata variantMIP, Path target) {
        try {
            LOG.debug("cp {} {}", variantMIP, target);
            CmdUtils.createOutputDirs(target.getParent());
            Files.copy(MIPsUtils.openInputStream(variantMIP), target, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            LOG.error("Error copying {} -> {}", variantMIP, target, e);
        }
    }

}
