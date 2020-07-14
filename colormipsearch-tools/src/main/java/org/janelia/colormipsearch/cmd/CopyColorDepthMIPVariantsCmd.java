package org.janelia.colormipsearch.cmd;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.api.cdmips.AbstractMetadata;
import org.janelia.colormipsearch.api.cdmips.MIPsUtils;
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
        CmdUtils.createOutputDirs(args.getOutputDir());
        copyAllMIPsVariants(args);
    }

    private void copyAllMIPsVariants(CopyColorDepthMIPVariantsArgs args) {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        List<MIPWithVariantsMetadata> inputMips = MIPsUtils.readGenericMIPsFromJSON(
                args.inputMIPs.input,
                args.inputMIPs.offset,
                args.inputMIPs.length,
                CommonArgs.toLowerCase(args.mipsFilter),
                mapper,
                MIPWithVariantsMetadata.class
        );

        Map<String, List<MIPWithVariantsMetadata>> inputMIPsGroupedByID = inputMips.stream()
                .collect(Collectors.groupingBy(AbstractMetadata::getId, Collectors.toList()));
        LOG.info("Copy variants for {} mips", inputMIPsGroupedByID);

        Path outputPath = args.getOutputDir();
        if (outputPath == null) {
            LOG.info("No destination path has been specified");
            return;
        }
        inputMIPsGroupedByID.entrySet().stream()
                .forEach(me -> {
                    int mipIndex = 1;
                    for (MIPWithVariantsMetadata mip : me.getValue()) {
                        if (copyMIPVariants(mipIndex, mip, outputPath, args.variantMapping)) {
                            mipIndex++;
                        }
                    }
                });
    }

    private boolean copyMIPVariants(int mipIndex, MIPWithVariantsMetadata mip, Path outputPath, Map<String, String> variantMapping) {
        Set<String> mipVariantTypes = mip.getVariantTypes();
        if (mipVariantTypes.isEmpty()) {
            return false;
        };
        for (String variant : mipVariantTypes) {
            String variantDestination = variantMapping.get(variant);
            if (StringUtils.isNotBlank(variantDestination)) {
                String variantSource = mip.getVariant(variant);
                copyMIPVariant(
                        variantSource,
                        outputPath.resolve(variantDestination)
                                .resolve(createMIPSegmentName(mip.getCdmPath(), getImageExt(variantSource), mipIndex))
                );
            }
        }
        return true;
    }

    private String createMIPSegmentName(String cdmPath, String imageExt, int segmentIndex) {
        String cdmName = Paths.get(cdmPath).getFileName().toString();
        String cdmSegmentName = RegExUtils.replacePattern(cdmName, "_CDM\\..*$", "");
        return String.format("%s-%2d_CDM%s", cdmSegmentName, segmentIndex, imageExt);
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

    private void copyMIPVariant(String variant, Path target) {
        System.out.println("!!!!! CP " + variant + " " + target); // FIXME
    }

}
