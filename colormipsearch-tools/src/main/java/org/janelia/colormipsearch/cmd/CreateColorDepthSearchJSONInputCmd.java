package org.janelia.colormipsearch.cmd;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api.cdmips.MIPsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class CreateColorDepthSearchJSONInputCmd extends AbstractCmd {

    private static final Logger LOG = LoggerFactory.getLogger(CreateColorDepthSearchJSONInputCmd.class);

    // since right now there'sonly one EM library just use its name to figure out how to handle the color depth mips metadata
    private static final String NO_CONSENSUS = "No Consensus";
    private static final int DEFAULT_PAGE_LENGTH = 10000;

    public static class ChannelBaseValidator implements IValueValidator<Integer> {
        @Override
        public void validate(String name, Integer value) throws ParameterException {
            if (value < 0 || value > 1) {
                throw new ParameterException("Invalid channel base - supported values are {0, 1}");
            }
        }
    }

    @Parameters(commandDescription = "Groop MIPs by published name")
    static class CreateColorDepthSearchJSONInputArgs extends AbstractCmdArgs {
        @Parameter(names = {"--jacs-url", "--data-url", "--jacsURL"}, description = "JACS data service base URL")
        String dataServiceURL;

        @Parameter(names = {"--config-url"}, description = "Config URL that contains the library name mapping")
        String libraryMappingURL = "http://config.int.janelia.org/config/cdm_library";

        @Parameter(names = {"--authorization"}, description = "JACS authorization - this is the value of the authorization header")
        String authorization;

        @Parameter(names = {"--alignment-space", "-as"}, description = "Alignment space")
        String alignmentSpace = "JRC2018_Unisex_20x_HR";

        @Parameter(names = {"--libraries", "-l"},
                description = "Which libraries to extract such as {flyem_hemibrain, flylight_gen1_gal4, flylight_gen1_lexa, flylight_gen1_mcfo_case_1, flylight_splitgal4_drivers}. " +
                        "The format is <libraryName>[:<offset>[:<length>]]",
                required = true, variableArity = true, converter = ListArg.ListArgConverter.class)
        List<ListArg> libraries;

        @Parameter(names = {"--librariesVariants"},
                description = "Libraries variants descriptors. " +
                        "A library variant contains library name, variant type, location and suffix separated by colon , e.g., " +
                        "flylight_gen1_mcfo_published:segmentation:/libDir/mcfo/segmentation:_CDM",
                converter = MIPVariantArg.MIPVariantArgConverter.class,
                validateValueWith = MIPVariantArg.ListMIPVariantArgValidator.class,
                variableArity = true)
        List<MIPVariantArg> libraryVariants;

        @Parameter(names = "--included-libraries", variableArity = true, description = "If set, MIPs should also be in all these libraries")
        Set<String> includedLibraries;

        @Parameter(names = "--excluded-libraries", variableArity = true, description = "If set, MIPs should not be part of any of these libraries")
        Set<String> excludedLibraries;

        @Parameter(names = {"--datasets"}, description = "Which datasets to extract", variableArity = true)
        List<String> datasets;

        @Parameter(names = {"--color-depth-mips-variant"},
                description = "The entry name in the variants dictionary for color depth mips")
        String cdmVariantName;

        @Parameter(names = {"--segmented-mips-variant"},
                description = "The entry name in the variants dictionary for segmented images")
        String segmentationVariantName;

        @Parameter(names = "--include-mips-without-publishing-name", description = "Bitmap flag for include MIPs without publishing name: " +
                "0x0 - if either the publishing name is missing or the publishedToStaging is not set the image is not included; " +
                "0x1 - the mip is included even if publishing name is not set; " +
                "0x2 - the mip is included even if publishedToStaging is not set")
        int includeMIPsWithoutPublisingName = 0;

        @Parameter(names = "--segmented-image-handling", description = "Bit field that specifies how to handle segmented images - " +
                "0 - lookup segmented images but if none is found include the original, " +
                "0x1 - include the original MIP but only if a segmented image exists, " +
                "0x2 - include only segmented image if it exists, " +
                "0x4 - include both the original MIP and all its segmentations")
        int segmentedImageHandling = 0;

        @Parameter(names = "--segmentation-channel-base", description = "Segmentation channel base (0 or 1)", validateValueWith = ChannelBaseValidator.class)
        int segmentedImageChannelBase = 1;

        @Parameter(names = {"--excluded-mips"}, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "Comma-delimited list of JSON configs containing mips to be excluded from the requested list")
        List<ListArg> excludedMIPs;

        @Parameter(names = {"--excluded-names"}, description = "Published names excluded from JSON", variableArity = true)
        List<String> excludedNames;

        @Parameter(names = {"--default-gender"}, description = "Default gender")
        String defaultGender = "f";

        @Parameter(names = {"--keep-dups"}, description = "Keep duplicates", arity = 0)
        boolean keepDuplicates;

        @Parameter(names = {"--urls-relative-to"}, description = "URLs are relative to the specified component")
        int urlsRelativeTo = -1;

        @Parameter(names = {"--output-filename"}, description = "Output file name")
        String outputFileName;

        @Parameter(names = {"--append-output"}, description = "Append output if it exists", arity = 0)
        boolean appendOutput;

        @ParametersDelegate
        final CommonArgs commonArgs;

        CreateColorDepthSearchJSONInputArgs(CommonArgs commonArgs) {
            this.commonArgs = commonArgs;
        }

        @Override
        List<String> validate() {
            return Collections.emptyList();
        }
    }

    static class LibraryPathsArgs {
        ListArg library;
        List<MIPVariantArg> libraryVariants;

        List<MIPVariantArg> listLibraryVariants() {
            return CollectionUtils.isNotEmpty(libraryVariants) ? libraryVariants : Collections.emptyList();
        }

        Optional<MIPVariantArg> getLibraryVariant(String variantType) {
            return listLibraryVariants().stream()
                    .filter(lv -> lv.variantType.equals(variantType))
                    .findFirst();
        }

        String getLibraryVariantNameSuffix(String variantType) {
            return getLibraryVariant(variantType)
                    .map(lv -> lv.variantNameSuffix)
                    .orElse(null);
        }

        String getLibraryVariantPath(String variantType) {
            return getLibraryVariant(variantType)
                    .map(lv -> lv.variantPath)
                    .orElse(null);
        }

        String getLibraryName() {
            return library != null ? library.input : null;
        }
    }

    private final CreateColorDepthSearchJSONInputArgs args;
    private final ObjectMapper mapper;

    CreateColorDepthSearchJSONInputCmd(String commandName, CommonArgs commonArgs) {
        super(commandName);
        args = new CreateColorDepthSearchJSONInputArgs(commonArgs);
        this.mapper = new ObjectMapper()
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    CreateColorDepthSearchJSONInputArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        Set<MIPMetadata> excludedMips;
        if (args.excludedMIPs != null) {
            LOG.info("Read mips to be excluded from the output");
            excludedMips = args.excludedMIPs.stream()
                    .flatMap(mipsInput -> MIPsUtils.readMIPsFromJSON(
                            mipsInput.input,
                            mipsInput.offset,
                            mipsInput.length,
                            Collections.emptySet(),
                            mapper).stream())
                    .collect(Collectors.toSet());
            LOG.info("Found {} mips to be excluded from the output", excludedMips.size());
        } else {
            excludedMips = Collections.emptySet();
        }

        Function<String, String> imageURLMapper = aUrl -> {
            if (StringUtils.isBlank(aUrl)) {
                return "";
            } else if (StringUtils.startsWithIgnoreCase(aUrl, "https://") ||
                    StringUtils.startsWithIgnoreCase(aUrl, "http://")) {
                if (args.urlsRelativeTo >= 0) {
                    URI uri = URI.create(aUrl);
                    Path uriPath = Paths.get(uri.getPath());
                    return uriPath.subpath(args.urlsRelativeTo,  uriPath.getNameCount()).toString();
                } else {
                    return aUrl;
                }
            } else {
                return aUrl;
            }
        };
        Map<String, List<MIPVariantArg>> libraryVariants;
        if (CollectionUtils.isEmpty(args.libraryVariants)) {
            libraryVariants = Collections.emptyMap();
        } else {
            libraryVariants = args.libraryVariants.stream().collect(Collectors.groupingBy(lv -> lv.libraryName, Collectors.toList()));
        }
        args.libraries.stream()
                .map(library -> {
                    LibraryPathsArgs lpaths = new LibraryPathsArgs();
                    lpaths.library = library;
                    lpaths.libraryVariants = libraryVariants.get(library.input);
                    return lpaths;
                })
                .forEach(lpaths -> {
                    if (StringUtils.isBlank(args.dataServiceURL)) {
                        createColorDepthSearchJSONInputMIPsInOfflineMode(
                                lpaths,
                                args.cdmVariantName,
                                args.segmentationVariantName,
                                CollectionUtils.isEmpty(args.excludedNames)
                                        ? Collections.emptySet()
                                        : ImmutableSet.copyOf(args.excludedNames),
                                Paths.get(args.commonArgs.outputDir),
                                args.outputFileName);

                    } else {
                        WebTarget serverEndpoint = createHttpClient().target(args.dataServiceURL);
                        Map<String, String> libraryNameMapping = MIPsHandlingUtils.retrieveLibraryNameMapping(createHttpClient(), args.libraryMappingURL);

                        createColorDepthSearchJSONInputMIPs(
                                serverEndpoint,
                                lpaths,
                                args.segmentationVariantName,
                                excludedMips,
                                libraryNameMapping,
                                imageURLMapper,
                                Paths.get(args.commonArgs.outputDir),
                                args.outputFileName
                        );
                    }
                });
    }

    private JsonGenerator createJsonGenerator(Path outputPath,
                                              String outputFileName,
                                              int libraryFromIndex,
                                              int libraryToIndex) {
        String outputName;
        if (libraryFromIndex > 0) {
            outputName = outputFileName + "-" + libraryFromIndex + "-" + libraryToIndex + ".json";
        } else {
            outputName = outputFileName + ".json";
        }
        Path outputFilePath = outputPath.resolve(outputName);
        LOG.info("Write color depth MIPs to {}", outputFilePath);
        if (Files.exists(outputFilePath) && args.appendOutput) {
            return openOutputForAppend(outputFilePath.toFile());
        } else {
            return openOutput(outputFilePath.toFile());
        }
    }

    private void createColorDepthSearchJSONInputMIPsInOfflineMode(LibraryPathsArgs libraryPaths,
                                                                  String cdmVariantType,
                                                                  String segmentationVariantType,
                                                                  Set<String> excludedNames,
                                                                  Path outputPath,
                                                                  String outputFileName) {
        LOG.warn("Generate JSON mips in offline mode");

        try {
            Files.createDirectories(outputPath);
        } catch (IOException e) {
            LOG.error("Error creating the output directory for {}", outputPath, e);
        }

        JsonGenerator gen;
        try {
            gen = createJsonGenerator(
                    outputPath,
                    StringUtils.defaultIfBlank(outputFileName, libraryPaths.getLibraryName()),
                    0, // no range based naming
                    0);
        } catch (Exception e) {
            LOG.error("Error opening the outputfile {}", outputPath, e);
            return;
        }

        String cdmLibraryPath = libraryPaths.getLibraryVariantPath(cdmVariantType);
        Pair<MIPsHandlingUtils.MIPLibraryEntryType, Map<String, List<String>>> cdmImages =
                MIPsHandlingUtils.getLibraryImageFiles(libraryPaths.library.input, cdmLibraryPath, libraryPaths.getLibraryVariantNameSuffix(cdmVariantType));

        String librarySegmentationPath = libraryPaths.getLibraryVariantPath(segmentationVariantType);
        Pair<MIPsHandlingUtils.MIPLibraryEntryType, Map<String, List<String>>> segmentedImages =
                MIPsHandlingUtils.getLibraryImageFiles(libraryPaths.library.input, librarySegmentationPath, libraryPaths.getLibraryVariantNameSuffix(segmentationVariantType));
        LOG.info("Found {} segmented slide codes in {}", segmentedImages.getRight().size(), librarySegmentationPath);

        cdmImages.getRight().entrySet().stream()
                .flatMap(cdmEntry -> cdmEntry.getValue().stream())
                .map(cdmipImage -> MIPsHandlingUtils.isEmLibrary(libraryPaths.getLibraryName())
                        ? asEMBodyMetadataFromName(
                                libraryPaths.getLibraryName(),
                                cdmLibraryPath,
                                cdmImages.getLeft(),
                                cdmipImage)
                        : asLMLineMetadataFromName(
                                libraryPaths.getLibraryName(),
                                cdmLibraryPath,
                                cdmImages.getLeft(),
                                cdmipImage))
                .filter(cdmip -> cdmip != null)
                .filter(cdmip -> !excludedNames.contains(cdmip.getPublishedName()))
                .flatMap(cdmip -> MIPsHandlingUtils.findSegmentedMIPs(cdmip, librarySegmentationPath, segmentedImages, args.segmentedImageHandling, args.segmentedImageChannelBase).stream())
                .map(ColorDepthMetadata::asMIPWithVariants)
                .peek(cdmip -> {
                    libraryPaths.getLibraryVariant(segmentationVariantType)
                            .ifPresent(librarySegmentationVariant -> {
                                // add the image itself as a variant
                                addVariant(
                                        cdmip,
                                        segmentationVariantType,
                                        MIPsHandlingUtils.MIPLibraryEntryType.fromStringValue(cdmip.getImageType()),
                                        cdmip.getImageArchivePath(),
                                        cdmip.getImageName());
                            });
                })
                .peek(cdmip -> populateAllVariantsExceptSegmentation(
                        cdmip,
                        libraryPaths.listLibraryVariants(),
                        libraryPaths.getLibraryVariant(segmentationVariantType).orElse(null)))
                .forEach(cdmip -> {
                    try {
                        gen.writeObject(cdmip);
                    } catch (IOException e) {
                        LOG.error("Error writing entry for {}", cdmip, e);
                    }
                });
        try {
            gen.writeEndArray();
            gen.flush();
        } catch (IOException e) {
            LOG.error("Error closing array in {}", outputPath, e);
        }
    }

    private void createColorDepthSearchJSONInputMIPs(WebTarget serverEndpoint,
                                                     LibraryPathsArgs libraryPaths,
                                                     String segmentationVariantType,
                                                     Set<MIPMetadata> excludedMIPs,
                                                     Map<String, String> libraryNamesMapping,
                                                     Function<String, String> imageURLMapper,
                                                     Path outputPath,
                                                     String outputFileName) {
        int cdmsCount = countColorDepthMips(
                serverEndpoint,
                args.authorization,
                args.alignmentSpace,
                libraryPaths.library.input,
                args.datasets);
        LOG.info("Found {} entities in library {} with alignment space {}{}",
                cdmsCount, libraryPaths.getLibraryName(), args.alignmentSpace, CollectionUtils.isNotEmpty(args.datasets) ? " for datasets " + args.datasets : "");
        int to = libraryPaths.library.length > 0 ? Math.min(libraryPaths.library.offset + libraryPaths.library.length, cdmsCount) : cdmsCount;

        Function<ColorDepthMIP, String> libraryNameExtractor = cmip -> {
            String internalLibraryName = cmip.findLibrary(libraryPaths.getLibraryName());
            if (StringUtils.isBlank(internalLibraryName)) {
                return null;
            } else {
                String displayLibraryName = libraryNamesMapping.get(internalLibraryName);
                if (StringUtils.isBlank(displayLibraryName)) {
                    return internalLibraryName;
                } else {
                    return displayLibraryName;
                }
            }
        };

        try {
            Files.createDirectories(outputPath);
        } catch (IOException e) {
            LOG.error("Error creating the output directory for {}", outputPath, e);
        }

        JsonGenerator gen;
        try {
            gen = createJsonGenerator(
                    outputPath,
                    StringUtils.defaultIfBlank(outputFileName, libraryPaths.getLibraryName()),
                    libraryPaths.library.offset,
                    to);
        } catch (Exception e) {
            LOG.error("Error opening the outputfile {}", outputPath, e);
            return;
        }
        try {
            Map<String, List<String>> cdmNameToMIPIdForDupCheck = new HashMap<>();
            if (CollectionUtils.isNotEmpty(excludedMIPs)) {
                // build the initial map for duplicate check as well
                excludedMIPs.forEach(cdmip -> {
                    String cdmName = cdmip.getCdmName();
                    if (StringUtils.isNotBlank(cdmName)) {
                        List<String> mipIds = cdmNameToMIPIdForDupCheck.get(cdmName);
                        if (mipIds == null) {
                            cdmNameToMIPIdForDupCheck.put(cdmName, ImmutableList.of(cdmip.getId()));
                        } else {
                            if (!mipIds.contains(cdmip.getId())) {
                                cdmNameToMIPIdForDupCheck.put(
                                        cdmName,
                                        ImmutableList.<String>builder().addAll(mipIds).add(cdmip.getId()).build());
                            }
                        }
                    }
                });
            }
            String librarySegmentationPath = libraryPaths.getLibraryVariantPath(segmentationVariantType);
            Pair<MIPsHandlingUtils.MIPLibraryEntryType, Map<String, List<String>>> segmentedImages =
                    MIPsHandlingUtils.getLibraryImageFiles(libraryPaths.library.input, librarySegmentationPath, libraryPaths.getLibraryVariantNameSuffix(segmentationVariantType));
            LOG.info("Found {} segmented slide codes in {}", segmentedImages.getRight().size(), librarySegmentationPath);
            for (int pageOffset = libraryPaths.library.offset; pageOffset < to; pageOffset += DEFAULT_PAGE_LENGTH) {
                int pageSize = Math.min(DEFAULT_PAGE_LENGTH, to - pageOffset);
                List<ColorDepthMIP> cdmipsPage = retrieveColorDepthMipsWithSamples(
                        serverEndpoint,
                        args.authorization,
                        args.alignmentSpace,
                        libraryPaths.library,
                        args.datasets,
                        pageOffset,
                        pageSize);
                LOG.info("Process {} entries from {} to {} out of {}", cdmipsPage.size(), pageOffset, pageOffset + pageSize, cdmsCount);
                cdmipsPage.stream()
                        .peek(cdmip -> {
                            if (Files.notExists(Paths.get(cdmip.filepath))) {
                                LOG.warn("No filepath {} found for {} (sample: {}, publishedName: {})", cdmip.filepath, cdmip, cdmip.sampleRef, !hasSample(cdmip) ? "no sample": cdmip.sample.publishingName);
                            }
                        })
                        .filter(cdmip -> checkMIPLibraries(cdmip, args.includedLibraries, args.excludedLibraries))
                        .filter(cdmip -> MIPsHandlingUtils.isEmLibrary(libraryPaths.getLibraryName()) || (hasSample(cdmip) && hasConsensusLine(cdmip) && hasPublishedName(args.includeMIPsWithoutPublisingName, cdmip)))
                        .map(cdmip -> MIPsHandlingUtils.isEmLibrary(libraryPaths.getLibraryName())
                                ? asEMBodyMetadata(cdmip, args.defaultGender, libraryNameExtractor, imageURLMapper)
                                : asLMLineMetadata(cdmip, libraryNameExtractor, imageURLMapper))
                        .filter(cdmip -> cdmip != null)
                        .flatMap(cdmip -> MIPsHandlingUtils.findSegmentedMIPs(cdmip, librarySegmentationPath, segmentedImages, args.segmentedImageHandling, args.segmentedImageChannelBase).stream())
                        .map(ColorDepthMetadata::asMIPWithVariants)
                        .filter(cdmip -> CollectionUtils.isEmpty(excludedMIPs) || !excludedMIPs.contains(cdmip))
                        .peek(cdmip -> {
                            libraryPaths.getLibraryVariant(segmentationVariantType)
                                    .ifPresent(librarySegmentationVariant -> {
                                        // add the image itself as a variant
                                        addVariant(
                                                cdmip,
                                                segmentationVariantType,
                                                MIPsHandlingUtils.MIPLibraryEntryType.fromStringValue(cdmip.getImageType()),
                                                cdmip.getImageArchivePath(),
                                                cdmip.getImageName());
                                    });
                        })
                        .peek(cdmip -> {
                            String cdmName = cdmip.getCdmName();
                            if (StringUtils.isNotBlank(cdmName)) {
                                List<String> mipIds = cdmNameToMIPIdForDupCheck.get(cdmName);
                                if (mipIds == null) {
                                    cdmNameToMIPIdForDupCheck.put(cdmName, ImmutableList.of(cdmip.getId()));
                                } else {
                                    if (!mipIds.contains(cdmip.getId())) {
                                        cdmNameToMIPIdForDupCheck.put(
                                                cdmName,
                                                ImmutableList.<String>builder().addAll(mipIds).add(cdmip.getId()).build());
                                    }
                                }
                            }
                        })
                        .filter(cdmip -> {
                            String cdmName = cdmip.getCdmName();
                            if (args.keepDuplicates || StringUtils.isBlank(cdmName)) {
                                return true;
                            } else {
                                cdmNameToMIPIdForDupCheck.get(cdmName);
                                List<String> mipIds = cdmNameToMIPIdForDupCheck.get(cdmName);
                                int mipIndex = mipIds.indexOf(cdmip.getId());
                                if (mipIndex == 0) {
                                    return true;
                                } else {
                                    LOG.info("Not keeping {} because it is a duplicate of {}", cdmip.getId(), mipIds.get(0));
                                    return false;
                                }
                            }
                        })
                        .peek(cdmip -> populateAllVariantsExceptSegmentation(
                                cdmip,
                                libraryPaths.listLibraryVariants(),
                                libraryPaths.getLibraryVariant(segmentationVariantType).orElse(null)))
                        .peek(this::setImageURLs)
                        .peek(cdmip -> setPublishedImageURLs(cdmip, serverEndpoint, args.authorization))
                        .forEach(cdmip -> {
                            try {
                                gen.writeObject(cdmip);
                            } catch (IOException e) {
                                LOG.error("Error writing entry for {}", cdmip, e);
                            }
                        });
            }
            gen.writeEndArray();
            gen.flush();
        } catch (IOException e) {
            LOG.error("Error writing json args for library {}", libraryPaths.library, e);
        } finally {
            try {
                gen.close();
            } catch (IOException ignore) {
            }
        }
    }

    private void populateAllVariantsExceptSegmentation(MIPMetadata cdmip,
                                                       List<MIPVariantArg> libraryVariantArgs,
                                                       MIPVariantArg segmentationVariantArg) {
        String librarySegmentationSuffix = segmentationVariantArg == null ? null : segmentationVariantArg.variantTypeSuffix;
        for (MIPVariantArg mipVariantArg : libraryVariantArgs) {
            if (mipVariantArg == segmentationVariantArg) { // here it's safe to use '==' because I am looking for identity
                continue; // skip segmentation variant because it was already handled
            }
            MIPMetadata variantMIP = MIPsUtils.getMIPVariantInfo(
                    cdmip,
                    mipVariantArg.variantType,
                    Collections.singletonList(mipVariantArg.variantPath),
                    nc -> {
                        String suffix = StringUtils.defaultIfBlank(mipVariantArg.variantTypeSuffix, "");
                        if (StringUtils.isNotBlank(librarySegmentationSuffix)) {
                            return StringUtils.replaceIgnoreCase(nc, librarySegmentationSuffix, "") + suffix;
                        } else {
                            return nc + suffix;
                        }
                    },
                    mipVariantArg.variantNameSuffix);
            if (variantMIP !=  null) {
                addVariant(
                        cdmip,
                        mipVariantArg.variantType,
                        MIPsHandlingUtils.MIPLibraryEntryType.fromStringValue(variantMIP.getImageType()),
                        variantMIP.getImageArchivePath(),
                        variantMIP.getImageName());
            }
        }
    }

    private void addVariant(MIPMetadata cdmip, String variant, MIPsHandlingUtils.MIPLibraryEntryType variantType, String variantArchivePath, String variantName) {
        if (variantType == MIPsHandlingUtils.MIPLibraryEntryType.zipEntry) {
            cdmip.addVariant(variant, variantName);
            cdmip.addVariant(variant + "EntryType", MIPsHandlingUtils.MIPLibraryEntryType.zipEntry.name());
            cdmip.addVariant(variant + "ArchivePath", variantArchivePath);
        } else {
            if (StringUtils.isNotBlank(variantArchivePath)) {
                Path variantPath  = Paths.get(variantArchivePath, variantName);
                cdmip.addVariant(variant, variantPath.toString());
            } else {
                cdmip.addVariant(variant, variantName);
            }
        }
    }

    /**
     * The method sets image URLs to relative URLs if they are not set as well as the name of the searched image.
     * @param cdmip
     */
    private void setImageURLs(MIPMetadata cdmip) {
        if (!cdmip.getImageName().isEmpty() && !cdmip.getImageURL().isEmpty()) {
            // searchableName filename must be expressed in terms of publishedName (not internal name),
            //  and must include the integer suffix that identifies exactly which image it is (for LM),
            //  when there are multiple images for a given combination of parameters
            // in practical terms, we take the filename from imageURL, which has
            //  the publishedName in it, and graft on the required integer from imageName (for LM), which
            //  has the internal name; we have to similarly grab _FL from EM names

            // remove directories and extension (which we know is ".png") from imageURL:
            Path imagePath = Paths.get(cdmip.getImageURL());
            String matchingImageName = createMatchingSegmentationName(
                    Paths.get(cdmip.getCdmName()).getFileName().toString(),
                    Paths.get(cdmip.getImageName()).getFileName().toString(),
                    imagePath.getFileName().toString());
            cdmip.setSearchablePNG(matchingImageName);
        }
        if (StringUtils.isBlank(cdmip.getImageURL())) {
            // set relative image URLs
            String imageRelativeURL;
            if (MIPsHandlingUtils.isEmLibrary(cdmip.getLibraryName())) {
                imageRelativeURL = createEMImageRelativeURL(cdmip);
            } else {
                imageRelativeURL = createLMImageRelativeURL(cdmip);
            }
            cdmip.setImageURL(imageRelativeURL);
            cdmip.setThumbnailURL(imageRelativeURL);
        }
    }

    /**
     * this method sets the S3 URL(s) we get via publishedImage collection
     */
    private void setPublishedImageURLs(MIPMetadata cdmip, WebTarget serverEndpoint, String credentials){
        WebTarget endpoint = serverEndpoint.path("/publishedImage/image/" + cdmip.getAlignmentSpace() + "/" + cdmip.getSlideCode() +
            "/" + cdmip.getObjective());

        LOG.info("setPublishedImageURLs: URI = {}", endpoint.getUri());
        Response response = createRequestWithCredentials(endpoint.request(MediaType.APPLICATION_JSON), credentials).get();
        PublishedImage image;
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new IllegalStateException("Invalid response from " + endpoint.getUri() + " -> " + response);
        } else {
            List<PublishedImage> images = response.readEntity(new GenericType<>(new TypeReference<List<PublishedImage>>() {
            }.getType()));
            // api guarantees exactly one element in list:
            image = images.get(0);
        }

        // for now, there is only one URL
        // we don't have jacs-model, so not sure how to get this filetype?  can I hard-code what
        //  the name of the element of the enum is?
        cdmip.setImageStack(image.files.get("VisuallyLosslessStack"));
    }

    /**
     * Create the published name for the segmented image - the one that will actually be "color depth searched".
     * @param mipFileName
     * @param imageFileName
     * @param displayFileName
     * @return
     */
    private String createMatchingSegmentationName(String mipFileName, String imageFileName, String displayFileName) {
        String mipName = RegExUtils.replacePattern(mipFileName, "(_)?(CDM)?\\..*$", ""); // clear  _CDM.<ext> suffix
        String imageName = RegExUtils.replacePattern(imageFileName, "(_)?(CDM)?\\..*$", ""); // clear  _CDM.<ext> suffix
        String imageSuffix = RegExUtils.replacePattern(
                StringUtils.removeStart(imageName, mipName), // typically the segmentation name shares the same prefix with the original mip name
                "^[-_]",
                ""
        ); // remove the hyphen or underscore prefix
        String displayName = RegExUtils.replacePattern(displayFileName, "\\..*$", ""); // clear  .<ext> suffix
        return StringUtils.isBlank(imageSuffix)
                ? displayName + ".png"
                : displayName + "-" + imageSuffix + ".png";
    }

    private String createEMImageRelativeURL(MIPMetadata cdmip) {
        String imageName = StringUtils.isNotBlank(cdmip.getImageName())
                ? Paths.get(cdmip.getImageName()).getFileName().toString()
                : cdmip.getCdmName();
        Pattern imageNamePattern = Pattern.compile("((?<segmentation>\\d\\d)_CDM)?(?<ext>\\..*$)");
        Matcher imageNameMatcher = imageNamePattern.matcher(imageName);
        String seg;
        String ext;
        if (imageNameMatcher.find()) {
            seg = imageNameMatcher.group("segmentation");
            ext = imageNameMatcher.group("ext");
        } else {
            seg = "";
            ext = ".png";
        }
        return cdmip.getAlignmentSpace() + '/' +
                cdmip.getLibraryName() + '/' +
                cdmip.getPublishedName() + '-' +
                cdmip.getAlignmentSpace() + '-' +
                "CDM" +
                (StringUtils.isNotBlank(seg) ? "_" + seg : "") +
                ext;
    }

    private String createLMImageRelativeURL(MIPMetadata cdmip) {
        String imageName = StringUtils.isNotBlank(cdmip.getImageName())
                ? Paths.get(cdmip.getImageName()).getFileName().toString()
                : cdmip.getCdmName();
        Pattern imageNamePattern = Pattern.compile("((?<segmentation>\\d\\d)(_CDM)?)?(?<ext>\\..*$)");
        Matcher imageNameMatcher = imageNamePattern.matcher(imageName);
        String seg;
        String ext;
        if (imageNameMatcher.find()) {
            seg = imageNameMatcher.group("segmentation");
            ext = imageNameMatcher.group("ext");
        } else {
            seg = "";
            ext = ".png";
        }
        // I think we should get rid of this as well
        String driverName;
        if (StringUtils.isBlank(cdmip.getDriver())) {
            driverName = "";
        } else {
            int driverSeparator = cdmip.getDriver().indexOf('_');
            if (driverSeparator == -1) {
                driverName = cdmip.getDriver();
            } else {
                driverName = cdmip.getDriver().substring(0, driverSeparator);
            }
        }

        return cdmip.getAlignmentSpace() + '/' +
                StringUtils.replace(cdmip.getLibraryName(), " ", "_") + '/' +
                cdmip.getPublishedName() + '-' +
                cdmip.getSlideCode() + '-' +
                (StringUtils.isBlank(driverName) ? "" : driverName + '-') +
                cdmip.getGender() + '-' +
                cdmip.getObjective() + '-' +
                StringUtils.lowerCase(cdmip.getAnatomicalArea()) + '-' +
                cdmip.getAlignmentSpace() + '-' +
                "CDM" +
                (StringUtils.isNotBlank(seg) ? "_" + seg : "") +
                ext;
    }

    private JsonGenerator openOutput(File of)  {
        try {
            JsonGenerator gen = mapper.getFactory().createGenerator(new FileOutputStream(of), JsonEncoding.UTF8);
            gen.useDefaultPrettyPrinter();
            gen.writeStartArray();
            return gen;
        } catch (IOException e) {
            LOG.error("Error creating the output stream for {}", of, e);
            throw new UncheckedIOException(e);
        }
    }

    private JsonGenerator openOutputForAppend(File of) {
        try {
            LOG.debug("Append to {}", of);
            RandomAccessFile rf = new RandomAccessFile(of, "rw");
            long rfLength = rf.length();
            // position FP after the end of the last item
            // this may not work on Windows because of the new line separator
            // - so on windows it may need to rollback more than 4 chars
            rf.seek(rfLength - 2);
            OutputStream outputStream = Channels.newOutputStream(rf.getChannel());
            outputStream.write(',');
            long pos = rf.getFilePointer();
            JsonGenerator gen = mapper.getFactory().createGenerator(outputStream, JsonEncoding.UTF8);
            gen.useDefaultPrettyPrinter();
            gen.writeStartArray();
            gen.flush();
            rf.seek(pos);
            return gen;
        } catch (IOException e) {
            LOG.error("Error creating the output stream to be appended for {}", of, e);
            throw new UncheckedIOException(e);
        }
    }

    private boolean checkMIPLibraries(ColorDepthMIP cdmip, Set<String> includedLibraries, Set<String> excludedLibraries) {
        if (CollectionUtils.isNotEmpty(includedLibraries)) {
            if (!cdmip.libraries.containsAll(includedLibraries)) {
                return false;
            }
        }
        if (CollectionUtils.isNotEmpty(excludedLibraries)) {
            return cdmip.libraries.stream()
                    .filter(l -> excludedLibraries.contains(l))
                    .count() == 0;
        }
        return true;
    }

    private boolean hasSample(ColorDepthMIP cdmip) {
        return cdmip.sample != null;
    }

    private boolean hasConsensusLine(ColorDepthMIP cdmip) {
        if (StringUtils.equalsIgnoreCase(NO_CONSENSUS, cdmip.sample.line)) {
            LOG.warn ("MIP {} has no consensus line", cdmip);
            return false;
        } else {
            return true;
        }
    }

    private boolean hasPublishedName(int missingPublishingHandling,  ColorDepthMIP cdmip) {
        String publishingName = cdmip.sample.publishingName;
        if (cdmip.sample.publishedToStaging && StringUtils.isNotBlank(publishingName) && !StringUtils.equalsIgnoreCase(NO_CONSENSUS, publishingName)) {
            return true;
        } else {
            if (missingPublishingHandling == 0) {
                return false;
            } else {
                // missingPublishingHandling values:
                //      0x1 - ignore missing name
                //      0x2 - ignore missing published flag
                if ((StringUtils.isNotBlank(publishingName) &&
                        !StringUtils.equalsIgnoreCase(NO_CONSENSUS, publishingName) || (missingPublishingHandling & 0x1) != 0) &&
                        (cdmip.sample.publishedToStaging || (missingPublishingHandling & 0x2) != 0)) {
                    return true;
                } else {
                    LOG.warn("MIP {} has no consensus publishing name", cdmip);
                    return false;
                }

            }
        }
    }

    private ColorDepthMetadata asLMLineMetadata(ColorDepthMIP cdmip,
                                                Function<ColorDepthMIP, String> libraryNameExtractor,
                                                Function<String, String> imageURLMapper) {
        String libraryName = libraryNameExtractor.apply(cdmip);
        ColorDepthMetadata cdMetadata = new ColorDepthMetadata();
        cdMetadata.setId(cdmip.id);
        cdMetadata.setAlignmentSpace(cdmip.alignmentSpace);
        cdMetadata.setLibraryName(libraryName);
        cdMetadata.filepath = cdmip.filepath;
        cdMetadata.setImageURL(imageURLMapper.apply(cdmip.publicImageUrl));
        cdMetadata.setThumbnailURL(imageURLMapper.apply(cdmip.publicThumbnailUrl));
        cdMetadata.sourceImageRef = cdmip.sourceImageRef;
        cdMetadata.setSampleRef(cdmip.sampleRef);
        if (cdmip.sample != null) {
            cdMetadata.setPublishedToStaging(cdmip.sample.publishedToStaging);
            cdMetadata.setLMLinePublishedName(cdmip.sample.publishingName);
            cdMetadata.setSlideCode(cdmip.sample.slideCode);
            cdMetadata.setGender(cdmip.sample.gender);
            cdMetadata.setDriver(cdmip.sample.driver);
            cdMetadata.setMountingProtocol(cdmip.sample.mountingProtocol);
        } else {
            populateCDMetadataFromCDMIPName(cdmip.name, cdMetadata);
        }
        cdMetadata.setAnatomicalArea(cdmip.anatomicalArea);
        cdMetadata.setAlignmentSpace(cdmip.alignmentSpace);
        cdMetadata.setObjective(cdmip.objective);
        cdMetadata.setChannel(cdmip.channelNumber);
        return cdMetadata;
    }

    private void populateCDMetadataFromCDMIPName(String mipName, ColorDepthMetadata cdMetadata) {
        String[] mipNameComponents = StringUtils.split(mipName, '-');
        String line = mipNameComponents.length > 0 ? mipNameComponents[0] : mipName;
        // attempt to remove the PI initials
        int piSeparator = StringUtils.indexOf(line, '_');
        String lineID;
        if (piSeparator == -1) {
            lineID = line;
        } else {
            lineID = line.substring(piSeparator + 1);
        }
        cdMetadata.setLMLinePublishedName(StringUtils.defaultIfBlank(lineID, "Unknown"));
        String slideCode = mipNameComponents.length > 1 ? mipNameComponents[1] : null;
        cdMetadata.setSlideCode(slideCode);
        int colorChannel = MIPsHandlingUtils.extractColorChannelFromImageName(mipName, 0);
        if (colorChannel != -1) {
            cdMetadata.setChannel(String.valueOf(colorChannel));
        }
        cdMetadata.setObjective(MIPsHandlingUtils.extractObjectiveFromImageName(mipName));
        String gender = MIPsHandlingUtils.extractGenderFromImageName(mipName);
        if (gender != null) {
            cdMetadata.setGender(gender);
        }
    }

    private ColorDepthMetadata asEMBodyMetadata(ColorDepthMIP cdmip,
                                                String defaultGender,
                                                Function<ColorDepthMIP, String> libraryNameExtractor,
                                                Function<String, String> imageURLMapper) {
        if (cdmip.bodyId == null || cdmip.bodyId == 0L) {
            LOG.warn("Invalid body ID - Ignore {}", cdmip);
            return null;
        }
        try {
            String libraryName = libraryNameExtractor.apply(cdmip);
            ColorDepthMetadata cdMetadata = new ColorDepthMetadata();
            cdMetadata.setId(cdmip.id);
            cdMetadata.setNeuronType(cdmip.neuronType);
            cdMetadata.setNeuronInstance(cdmip.neuronInstance);
            cdMetadata.setAlignmentSpace(cdmip.alignmentSpace);
            cdMetadata.setLibraryName(libraryName);
            cdMetadata.filepath = cdmip.filepath;
            cdMetadata.setImageURL(imageURLMapper.apply(cdmip.publicImageUrl));
            cdMetadata.setThumbnailURL(imageURLMapper.apply(cdmip.publicThumbnailUrl));
            cdMetadata.setEMSkeletonPublishedName(Long.toString(cdmip.bodyId));
            cdMetadata.setGender(defaultGender);
            return cdMetadata;
        } catch (Exception e) {
            LOG.error("Error getting metadata for {}", cdmip, e);
            return null;
        }
    }

    private ColorDepthMetadata asLMLineMetadataFromName(String libraryName,
                                                        String libraryPath,
                                                        MIPsHandlingUtils.MIPLibraryEntryType mipEntryType,
                                                        String mipImageName) {
        ColorDepthMetadata cdMetadata = createMetadataFromName(libraryName, libraryPath, mipEntryType, mipImageName);
        populateCDMetadataFromCDMIPName(new File(mipImageName).getName(), cdMetadata);
        cdMetadata.setId(cdMetadata.getPublishedName());
        return cdMetadata;
    }

    private ColorDepthMetadata asEMBodyMetadataFromName(String libraryName,
                                                        String libraryPath,
                                                        MIPsHandlingUtils.MIPLibraryEntryType mipEntryType,
                                                        String mipImageName) {
        ColorDepthMetadata cdMetadata = createMetadataFromName(libraryName, libraryPath, mipEntryType, mipImageName);
        cdMetadata.setEMSkeletonPublishedName(MIPsHandlingUtils.extractEMBodyIdFromName(new File(mipImageName).getName()));
        cdMetadata.setId(cdMetadata.getPublishedName());
        return cdMetadata;
    }

    private ColorDepthMetadata createMetadataFromName(String libraryName,
                                                      String libraryPath,
                                                      MIPsHandlingUtils.MIPLibraryEntryType mipEntryType,
                                                      String mipImageName) {
        ColorDepthMetadata cdMetadata = new ColorDepthMetadata();
        cdMetadata.setAlignmentSpace(args.alignmentSpace);
        cdMetadata.setLibraryName(libraryName);
        cdMetadata.setImageName(mipImageName);
        if (mipEntryType == MIPsHandlingUtils.MIPLibraryEntryType.zipEntry) {
            cdMetadata.setImageType(mipEntryType.name());
            cdMetadata.setImageArchivePath(libraryPath);
        }
        cdMetadata.filepath = mipImageName; // this is not quite right for zip entries but it's the best approximation
        return cdMetadata;
    }

    private int countColorDepthMips(WebTarget serverEndpoint, String credentials, String alignmentSpace, String library, List<String> datasets) {
        WebTarget target = serverEndpoint.path("/data/colorDepthMIPsCount")
                .queryParam("libraryName", library)
                .queryParam("alignmentSpace", alignmentSpace)
                .queryParam("dataset", datasets != null ? datasets.stream().filter(StringUtils::isNotBlank).reduce((s1, s2) -> s1 + "," + s2).orElse(null) : null);
        Response response = createRequestWithCredentials(target.request(MediaType.TEXT_PLAIN), credentials).get();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new IllegalStateException("Invalid response from " + target + " -> " + response);
        } else {
            return response.readEntity(Integer.class);
        }
    }

    private List<ColorDepthMIP> retrieveColorDepthMipsWithSamples(WebTarget serverEndpoint,
                                                                  String credentials,
                                                                  String alignmentSpace,
                                                                  ListArg libraryArg,
                                                                  List<String> datasets,
                                                                  int offset,
                                                                  int pageLength) {
        return retrieveColorDepthMips(serverEndpoint.path("/data/colorDepthMIPsWithSamples")
                .queryParam("libraryName", libraryArg.input)
                .queryParam("alignmentSpace", alignmentSpace)
                .queryParam("dataset", datasets != null ? datasets.stream().filter(StringUtils::isNotBlank).reduce((s1, s2) -> s1 + "," + s2).orElse(null) : null)
                .queryParam("offset", offset)
                .queryParam("length", pageLength),
                credentials)
                ;
    }

    private List<ColorDepthMIP> retrieveColorDepthMips(WebTarget endpoint, String credentials) {
        Response response = createRequestWithCredentials(endpoint.request(MediaType.APPLICATION_JSON), credentials).get();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new IllegalStateException("Invalid response from " + endpoint.getUri() + " -> " + response);
        } else {
            return response.readEntity(new GenericType<>(new TypeReference<List<ColorDepthMIP>>() {
            }.getType()));
        }
    }

    private Invocation.Builder createRequestWithCredentials(Invocation.Builder requestBuilder, String credentials) {
        if (StringUtils.isNotBlank(credentials)) {
            return requestBuilder.header("Authorization", credentials);
        } else {
            return requestBuilder;
        }
    }

    private Client createHttpClient() {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLSv1");
            TrustManager[] trustManagers = {
                    new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(X509Certificate[] x509Certificates, String authType) {
                            // Everyone is trusted
                        }

                        @Override
                        public void checkServerTrusted(X509Certificate[] x509Certificates, String authType) {
                            // Everyone is trusted
                        }

                        @Override
                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }
                    }
            };
            sslContext.init(null, trustManagers, new SecureRandom());

            JacksonJsonProvider jsonProvider = new JacksonJaxbJsonProvider()
                    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                    ;

            return ClientBuilder.newBuilder()
                    .connectTimeout(30, TimeUnit.SECONDS)
                    .readTimeout(0, TimeUnit.SECONDS)
                    .sslContext(sslContext)
                    .hostnameVerifier((s, sslSession) -> true)
                    .register(jsonProvider)
                    .build();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}
