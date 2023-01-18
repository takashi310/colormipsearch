package org.janelia.colormipsearch.cmd;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.colormipsearch.dataio.CDMIPsReader;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.fileutils.FSUtils;
import org.janelia.colormipsearch.dataio.fs.JSONCDMIPsReader;
import org.janelia.colormipsearch.mips.NeuronMIPUtils;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.FileData;
import org.janelia.colormipsearch.model.LMNeuronEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to calculate the gradient scores.
 */
class CopyToMIPsStore extends AbstractCmd {

    private static final Logger LOG = LoggerFactory.getLogger(CopyToMIPsStore.class);
    private static final Map<String, ComputeFileType> VARIANT_FILE_TYPE_MAPPING = ImmutableMap.of (
            "cdm", ComputeFileType.SourceColorDepthImage,
            "searchable_neuron", ComputeFileType.InputColorDepthImage,
            "segmentation", ComputeFileType.InputColorDepthImage,
            "grad", ComputeFileType.GradientImage,
            "zgap", ComputeFileType.ZGapImage
    );
    private static Pattern FILENAME_EXT_PATTERN = Pattern.compile(".+(\\..*)$");

    @Parameters(commandDescription = "Create JACS variant library")
    static class CopyToMIPSStoreArgs extends AbstractCmdArgs {

        @Parameter(names = {"--input", "-i"}, required = true, converter = ListArg.ListArgConverter.class,
                   description = "JSON input MIPs")
        ListArg inputMIPsLibrary;

        @Parameter(names = {"--mipsFilter"}, variableArity = true, description = "Filter for i input mips")
        Set<String> mipsFilter;

        @Parameter(names = {"--targetDirectory"}, description = "Input image file(s) start index")
        String targetFolder;

        @Parameter(names = {"-n"}, description = "Only show what the command is supposed to do")
        boolean simulateFlag;

        // surjective variants typically have an object index in their name,
        @DynamicParameter(names = {"--surjective-variants-mapping"},
                description = "Destination mapping for surjective variants - variants that may have one or more corresponding mip images")
        Map<String, String> surjectiveVariantMapping = new HashMap<>();

        // An example of an injective variant is a gamma variant that changes the gamma of the original mip for better display
        @DynamicParameter(names = "--injective-variants-mapping",
                description = "Destination mapping for injective variants - variants that have one to more corresponding mip images")
        Map<String, String> injectiveVariantMapping = new HashMap<>();

        CopyToMIPSStoreArgs(CommonArgs commonArgs) {
            super(commonArgs);
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

    private final CopyToMIPSStoreArgs args;
    private final ObjectMapper mapper;

    CopyToMIPsStore(String commandName, CommonArgs commonArgs) {
        super(commandName);
        this.args = new CopyToMIPSStoreArgs(commonArgs);
        this.mapper = new ObjectMapper()
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        ;
    }

    @Override
    CopyToMIPSStoreArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        BiConsumer<FileData, Path> copyFileDataAction;
        if (args.simulateFlag) {
            copyFileDataAction = (fd, target) -> LOG.info("cp {} {}", fd, target);
        } else {
            copyFileDataAction = this::copyFileData;
        }
        copyMIPs(copyFileDataAction, args.getOutputDir());
    }

    private void copyMIPs(BiConsumer<FileData, Path> copyMIPVariantAction, Path targetDir) {
        CDMIPsReader cdmipsReader = getCDMipsReader();
        List<? extends AbstractNeuronEntity> mips = readMIPs(cdmipsReader);
        Map<String, List<AbstractNeuronEntity>> groupedMipsByMipID = mips.stream()
                .collect(Collectors.groupingBy(AbstractNeuronEntity::getMipId, Collectors.toList()));

        groupedMipsByMipID.entrySet().stream().parallel()
                .forEach(mipIdEntries -> {
                    Streams.zip(IntStream.rangeClosed(1, mipIdEntries.getValue().size()).boxed(),
                                    mipIdEntries.getValue().stream(),
                                    ImmutablePair::of)
                            .filter(indexedNeuronEntity -> indexedNeuronEntity.getRight().hasComputeFile(ComputeFileType.SourceColorDepthImage))
                            .forEach(indexedNeuronEntity -> {
                                Integer mipIndex = indexedNeuronEntity.getLeft();
                                AbstractNeuronEntity neuronEntity = indexedNeuronEntity.getRight();
                                String sourceCDMName = neuronEntity.getComputeFileData(ComputeFileType.SourceColorDepthImage).getNameCompOnly();
                                args.surjectiveVariantMapping.forEach((variantType, targetFolderName) -> {
                                    ComputeFileType ft = VARIANT_FILE_TYPE_MAPPING.get(variantType);
                                    if (neuronEntity.hasComputeFile(ft)) {
                                        FileData fd = neuronEntity.getComputeFileData(ft);
                                        copyMIPVariantAction.accept(
                                                fd,
                                                targetDir.resolve(targetFolderName)
                                                        .resolve(createMIPVariantName(neuronEntity, sourceCDMName, fd, mipIndex)));
                                    }
                                });
                            });
                });
    }

    private CDMIPsReader getCDMipsReader() {
        return new JSONCDMIPsReader(mapper);
    }

    private List<? extends AbstractNeuronEntity> readMIPs(CDMIPsReader mipsReader) {
        return mipsReader.readMIPs(new DataSourceParam()
                .addLibrary(args.inputMIPsLibrary.input)
                .setOffset(args.inputMIPsLibrary.offset)
                .setSize(args.inputMIPsLibrary.length)).stream()
                .filter(neuronMetadata -> CollectionUtils.isEmpty(args.mipsFilter) ||
                        args.mipsFilter.contains(neuronMetadata.getPublishedName().toLowerCase()) ||
                        args.mipsFilter.contains(neuronMetadata.getMipId()))
                .collect(Collectors.toList());
    }

    private String createMIPVariantName(AbstractNeuronEntity neuronEntity, String cdmName, FileData fd, int mipIndex) {
        if (MIPsHandlingUtils.isEmLibrary(neuronEntity.getLibraryName())) {
            return fd.getNameCompOnly();
        } else {
            LMNeuronEntity lmNeuronEntity = (LMNeuronEntity) neuronEntity;
            return createLMMIPName(lmNeuronEntity, cdmName, fd.getNameCompOnly(), mipIndex);
        }
    }

    private String createLMMIPName(LMNeuronEntity lmNeuronEntity, String cdmName, String variantName, int segmentIndex) {
        String baseCDMName = RegExUtils.replacePattern(cdmName, "(_CDM)?\\..*$", "");
        List<String> nameComponents = Splitter.on('-').splitToList(baseCDMName);
        String prefix = nameComponents.get(0); // there should always be at least one component even if there is no hyphen delim
        String alignmentSpace = lmNeuronEntity.getAlignmentSpace();
        String slideCode = lmNeuronEntity.getSlideCode();
        String objective = lmNeuronEntity.getObjective();
        String anatomicalArea = lmNeuronEntity.getAnatomicalArea();
        String sampleRef = StringUtils.removeStartIgnoreCase(lmNeuronEntity.getSourceRefId(), "Sample#");
        String channel = getComponent(nameComponents, 6, "",
                s -> StringUtils.removeStartIgnoreCase(StringUtils.removeStartIgnoreCase(s, "c"), "h")
        );
        return formatSimpleSegmentName(
                prefix + '-' +
                        slideCode + '-' +
                        objective + '-' +
                        anatomicalArea + '-' +
                        alignmentSpace + '-' +
                        sampleRef + '-' +
                        "CH" + channel,
                segmentIndex,
                getNameExt(variantName)
        );
    }

    private String getComponent(List<String> comps, int index, String defaultValue, Function<String, String> compTransform) {
        if (index < comps.size()) {
            return compTransform.apply(comps.get(index));
        } else {
            return defaultValue;
        }
    }

    private String getNameExt(String name) {
        Matcher nameExtMatcher = FILENAME_EXT_PATTERN.matcher(name);
        if (nameExtMatcher.find()) {
            return nameExtMatcher.group(1);
        } else {
            return "";
        }
    }

    private String formatSimpleSegmentName(String segmentName, int segmentIndex, String imageExt) {
        if (segmentIndex > 0) {
            return String.format("%s-%02d_CDM%s", segmentName, segmentIndex, imageExt);
        } else {
            return String.format("%s_CDM%s", segmentName, imageExt);
        }
    }

    private void copyFileData(FileData fileData, Path dest) {
        InputStream fdStream;
        try {
            FSUtils.createDirs(dest.getParent());
            fdStream = NeuronMIPUtils.openInputStream(fileData);
            if (fdStream == null) {
                LOG.warn("{} data not found", fileData);
                return;
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        try {
            LOG.debug("cp {} {}", fileData, dest);
            Files.copy(fdStream, dest, StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            LOG.error("Error copying {} -> {}", fileData, dest, e);
        } finally {
            try {
                fdStream.close();
            } catch (IOException ignore) {}
        }
    }

}
