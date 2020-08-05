package org.janelia.colormipsearch.cmd;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
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
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

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
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
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
    private static final int MAX_SEGMENTED_DATA_DEPTH = 5;

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
        @Parameter(names = {"--jacs-url", "--data-url", "--jacsURL"},
                description = "JACS data service base URL", required = true)
        String dataServiceURL;

        @Parameter(names = {"--config-url"}, description = "Config URL that contains the library name mapping")
        String libraryMappingURL = "http://config.int.janelia.org/config/cdm_libraries";

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

        @Parameter(names = {"--segmented-mips-variant"},
                description = "The entry name in the variants dictionary for segmented images")
        String segmentationVariantName;

        @Parameter(names = "--include-mips-with-missing-urls", description = "Include MIPs that do not have a valid URL", arity = 0)
        boolean includeMIPsWithNoPublishedURL;

        @Parameter(names = "--include-mips-without-publishing-name", description = "Bitmap flag for include MIPs without publishing name: " +
                "0x0 - if either the publishing name is missing or the publishedToStaging is not set the image is not included; " +
                "0x1 - the mip is included even if publishing name is not set; " +
                "0x2 - the mip is included even if publishedToStaging is not set")
        int includeMIPsWithoutPublisingName;

        @Parameter(names = "--segmented-image-handling", description = "Bit field that specifies how to handle segmented images - " +
                "0 - lookup segmented images but if none is found include the original, " +
                "0x1 - include the original MIP but only if a segmented image exists, " +
                "0x2 - include only segmented image if it exists, " +
                "0x4 - include both the original MIP and all its segmentations")
        int segmentedImageHandling;

        @Parameter(names = "--segmentation-channel-base", description = "Segmentation channel base (0 or 1)", validateValueWith = ChannelBaseValidator.class)
        int segmentedImageChannelBase = 0;

        @Parameter(names = {"--excluded-mips"}, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "Comma-delimited list of JSON configs containing mips to be excluded from the requested list")
        List<ListArg> excludedMIPs;

        @Parameter(names = {"--default-gender"}, description = "Default gender")
        String defaultGender = "f";

        @Parameter(names = {"--keep-dups"}, description = "Keep duplicates", arity = 0)
        boolean keepDuplicates;

        @Parameter(names = {"--output-filename"}, description = "Output file name")
        String outputFileName;

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

        Optional<MIPVariantArg> getLibrarySegmentationVariant(String segmentationVariantType) {
            return listLibraryVariants().stream()
                    .filter(lv -> lv.variantType.equals(segmentationVariantType))
                    .findFirst();
        }

        String getLibrarySegmentationPath(String segmentationVariantType) {
            return getLibrarySegmentationVariant(segmentationVariantType)
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
        WebTarget serverEndpoint = createHttpClient().target(args.dataServiceURL);

        Map<String, String> libraryNameMapping = retrieveLibraryNameMapping(args.libraryMappingURL);

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

        Map<String, List<MIPVariantArg>> libraryVariants;
        if (CollectionUtils.isEmpty(args.libraryVariants)) {
            libraryVariants = Collections.emptyMap();
        } else {
            libraryVariants = args.libraryVariants.stream().collect(Collectors.groupingBy(lv -> lv.libraryName, Collectors.toList()));
        }
        Streams.zip(
                IntStream.range(0, args.libraries.size()).boxed(),
                args.libraries.stream(),
                (lindex, library) -> {
                    LibraryPathsArgs lpaths = new LibraryPathsArgs();
                    lpaths.library = library;
                    lpaths.libraryVariants = libraryVariants.get(library.input);
                    return lpaths;
                }
        ).forEach(lpaths -> {
            createColorDepthSearchJSONInputMIPs(
                    serverEndpoint,
                    lpaths,
                    args.segmentationVariantName,
                    excludedMips,
                    libraryNameMapping,
                    Paths.get(args.commonArgs.outputDir),
                    args.outputFileName
            );

        });
    }

    private Map<String, String> retrieveLibraryNameMapping(String configURL) {
        Response response = createHttpClient().target(configURL).request(MediaType.APPLICATION_JSON).get();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new IllegalStateException("Invalid response from " + configURL + " -> " + response);
        }
        Map<String, Object> configJSON = response.readEntity(new GenericType<>(new TypeReference<Map<String, Object>>() {}.getType()));
        Object configEntry = configJSON.get("config");
        if (!(configEntry instanceof Map)) {
            LOG.error("Config entry from {} is null or it's not a map", configJSON);
            throw new IllegalStateException("Config entry not found");
        }
        Map<String, String> cdmLibraryNamesMapping = new HashMap<>();
        cdmLibraryNamesMapping.putAll((Map<String, String>)configEntry);
        LOG.info("Using {} for mapping library names", cdmLibraryNamesMapping);
        return cdmLibraryNamesMapping;
    }

    private void createColorDepthSearchJSONInputMIPs(WebTarget serverEndpoint,
                                                     LibraryPathsArgs libraryPaths,
                                                     String segmentationVariantType,
                                                     Set<MIPMetadata> excludedMIPs,
                                                     Map<String, String> libraryNamesMapping,
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
                    LOG.warn("No name mapping found {}", internalLibraryName);
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

        FileOutputStream outputStream;
        try {
            String outputName;
            String outputBasename = StringUtils.defaultIfBlank(outputFileName, libraryPaths.getLibraryName());
            if (libraryPaths.library.offset > 0 || libraryPaths.library.length > 0) {
                outputName = outputBasename + "-" + libraryPaths.library.offset + "-" + to + ".json";
            } else {
                outputName = outputBasename + ".json";
            }
            Path outputFilePath = outputPath.resolve(outputName);
            LOG.info("Write color depth MIPs to {}", outputFilePath);
            outputStream = new FileOutputStream(outputFilePath.toFile());
        } catch (FileNotFoundException e) {
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
            Pair<String, Map<String, List<String>>> segmentedImages;
            String librarySegmentationPath = libraryPaths.getLibrarySegmentationPath(segmentationVariantType);
            if (isEmLibrary(libraryPaths.library.input)) {
                Pattern skeletonRegExPattern = Pattern.compile("([0-9]{7,})[_-].*");
                segmentedImages = getSegmentedImages(skeletonRegExPattern, librarySegmentationPath);
            } else {
                Pattern slideCodeRegExPattern = Pattern.compile("[-_](\\d\\d\\d\\d\\d\\d\\d\\d_[a-zA-Z0-9]+_[a-zA-Z0-9]+)([-_][mf])?[-_](.+[_-])ch?(\\d+)[_-]", Pattern.CASE_INSENSITIVE);
                segmentedImages = getSegmentedImages(slideCodeRegExPattern, librarySegmentationPath);
            }
            LOG.info("Found {} segmented slide codes in {}", segmentedImages.getRight().size(), librarySegmentationPath);
            JsonGenerator gen = mapper.getFactory().createGenerator(outputStream, JsonEncoding.UTF8);
            gen.useDefaultPrettyPrinter();
            gen.writeStartArray();
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
                        .filter(cdmip -> args.includeMIPsWithNoPublishedURL || hasPublishedImageURL(cdmip))
                        .filter(cdmip -> checkMIPLibraries(cdmip, args.includedLibraries, args.excludedLibraries))
                        .filter(cdmip -> isEmLibrary(libraryPaths.getLibraryName()) || (hasSample(cdmip) && hasConsensusLine(cdmip) && hasPublishedName(args.includeMIPsWithoutPublisingName, cdmip)))
                        .map(cdmip -> isEmLibrary(libraryPaths.getLibraryName()) ? asEMBodyMetadata(cdmip, args.defaultGender, libraryNameExtractor) : asLMLineMetadata(cdmip, libraryNameExtractor))
                        .flatMap(cdmip -> findSegmentedMIPs(cdmip, librarySegmentationPath, segmentedImages, args.segmentedImageHandling, args.segmentedImageChannelBase).stream())
                        .map(ColorDepthMetadata::asMIPWithVariants)
                        .filter(cdmip -> CollectionUtils.isEmpty(excludedMIPs) || !excludedMIPs.contains(cdmip))
                        .peek(cdmip -> {
                            libraryPaths.getLibrarySegmentationVariant(segmentationVariantType)
                                    .ifPresent(librarySegmentationVariant -> {
                                        // add the image itself as a variant
                                        cdmip.addVariant(segmentationVariantType,
                                                cdmip.getImageArchivePath(),
                                                cdmip.getImageName(),
                                                cdmip.getImageType());
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
                        .peek(cdmip -> {
                            MIPVariantArg segmentationVariant = libraryPaths.getLibrarySegmentationVariant(segmentationVariantType).orElse(null);
                            String librarySegmentationSuffix = segmentationVariant == null ? null : segmentationVariant.variantSuffix;
                            for (MIPVariantArg mipVariantArg : libraryPaths.listLibraryVariants()) {
                                if (mipVariantArg.variantType.equals(segmentationVariantType)) {
                                    continue; // skip segmentation variant because it was already handled
                                }
                                MIPMetadata variantMIP = MIPsUtils.getAncillaryMIPInfo(
                                        cdmip,
                                        Collections.singletonList(mipVariantArg.variantPath),
                                        nc -> {
                                            String suffix = StringUtils.defaultIfBlank(mipVariantArg.variantSuffix, "");
                                            if (StringUtils.isNotBlank(librarySegmentationSuffix)) {
                                                return StringUtils.replaceIgnoreCase(nc, librarySegmentationSuffix, "") + suffix;
                                            } else {
                                                return nc + suffix;
                                            }
                                        });
                                if (variantMIP !=  null) {
                                    cdmip.addVariant(mipVariantArg.variantType, variantMIP.getImageArchivePath(), variantMIP.getImageName(), variantMIP.getImageType());
                                }
                            }
                        })
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
                outputStream.close();
            } catch (IOException ignore) {
            }
        }
    }

    private List<ColorDepthMetadata> findSegmentedMIPs(ColorDepthMetadata cdmipMetadata,
                                                       String segmentedImagesBasePath,
                                                       Pair<String, Map<String, List<String>>> segmentedImages,
                                                       int segmentedImageHandling,
                                                       int segmentedImageChannelBase) {
        if (StringUtils.isBlank(segmentedImagesBasePath)) {
            return Collections.singletonList(cdmipMetadata);
        } else {
            List<ColorDepthMetadata> segmentedCDMIPs = lookupSegmentedImages(cdmipMetadata, segmentedImagesBasePath, segmentedImages.getLeft(), segmentedImages.getRight(), segmentedImageChannelBase);
            if (segmentedImageHandling == 0x1) {
                return segmentedCDMIPs.isEmpty() ? Collections.emptyList() : Collections.singletonList(cdmipMetadata);
            } else if (segmentedImageHandling == 0x2) {
                return segmentedCDMIPs;
            } else if (segmentedImageHandling == 0x4) {
                return Stream.concat(Stream.of(cdmipMetadata), segmentedCDMIPs.stream()).collect(Collectors.toList());
            } else {
                return segmentedCDMIPs.isEmpty() ? Collections.singletonList(cdmipMetadata) : segmentedCDMIPs;
            }
        }
    }

    private List<ColorDepthMetadata> lookupSegmentedImages(ColorDepthMetadata cdmipMetadata, String segmentedDataBasePath, String type, Map<String, List<String>> segmentedImages, int segmentedImageChannelBase) {
        String indexingField;
        Predicate<String> segmentedImageMatcher;
        if (isEmLibrary(cdmipMetadata.getLibraryName())) {
            indexingField = cdmipMetadata.getPublishedName();
            Pattern emNeuronStateRegExPattern = Pattern.compile("[0-9]+[_-]([0-9A-Z]*)_.*", Pattern.CASE_INSENSITIVE);
            segmentedImageMatcher = p -> {
                String fn = RegExUtils.replacePattern(Paths.get(p).getFileName().toString(), "\\.\\D*$", "");
                Preconditions.checkArgument(fn.contains(indexingField));
                String cmFN = RegExUtils.replacePattern(Paths.get(cdmipMetadata.filepath).getFileName().toString(), "\\.\\D*$", "");
                String fnState = extractEMNeuronStateFromName(fn, emNeuronStateRegExPattern);
                String cmFNState = extractEMNeuronStateFromName(cmFN, emNeuronStateRegExPattern);
                return StringUtils.isBlank(fnState) && StringUtils.isBlank(cmFNState) ||
                        StringUtils.isNotBlank(cmFNState) && fnState.startsWith(cmFNState); // fnState may be LV or TC which is actually the same as L or T respectivelly so for now this check should work
            };
        } else {
            indexingField = cdmipMetadata.getSlideCode();
            segmentedImageMatcher = p -> {
                String fn = Paths.get(p).getFileName().toString();
                Preconditions.checkArgument(fn.contains(indexingField));
                int channelFromMip = getColorChannel(cdmipMetadata);
                int channelFromFN = extractColorChannelFromSegmentedImageName(fn.replace(indexingField, ""), segmentedImageChannelBase);
                LOG.debug("Compare channel from {} ({}) with channel from {} ({})", cdmipMetadata.filepath, channelFromMip, fn, channelFromFN);
                String objectiveFromMip = cdmipMetadata.getObjective();
                String objectiveFromFN = extractObjectiveFromSegmentedImageName(fn.replace(indexingField, ""));
                return matchMIPChannelWithSegmentedImageChannel(channelFromMip, channelFromFN) &&
                        matchMIPObjectiveWithSegmentedImageObjective(objectiveFromMip, objectiveFromFN);
            };
        }
        if (segmentedImages.get(indexingField) == null) {
            return Collections.emptyList();
        } else {
            return segmentedImages.get(indexingField).stream()
                    .filter(segmentedImageMatcher)
                    .map(p -> {
                        String sifn = Paths.get(p).getFileName().toString();
                        int scIndex = sifn.indexOf(indexingField);
                        Preconditions.checkArgument(scIndex != -1);
                        ColorDepthMetadata segmentMIPMetadata = new ColorDepthMetadata();
                        cdmipMetadata.copyTo(segmentMIPMetadata);
                        segmentMIPMetadata.segmentedDataBasePath = segmentedDataBasePath;
                        segmentMIPMetadata.setImageType(type);
                        segmentMIPMetadata.segmentFilepath = p;
                        return segmentMIPMetadata;
                    })
                    .collect(Collectors.toList());
        }
    }

    private int getColorChannel(ColorDepthMetadata cdMetadata) {
        String channel = cdMetadata.getChannel();
        if (StringUtils.isNotBlank(channel)) {
            return Integer.parseInt(channel) - 1; // mip channels are 1 based so make it 0 based
        } else {
            return -1;
        }
    }

    private int extractColorChannelFromSegmentedImageName(String imageName, int channelBase) {
        Pattern regExPattern = Pattern.compile("[_-]ch?(\\d+)[_-]", Pattern.CASE_INSENSITIVE);
        Matcher chMatcher = regExPattern.matcher(imageName);
        if (chMatcher.find()) {
            String channel = chMatcher.group(1);
            return Integer.parseInt(channel) - channelBase;
        } else {
            return -1;
        }
    }

    private String extractObjectiveFromSegmentedImageName(String imageName) {
        Pattern regExPattern = Pattern.compile("[_-]([0-9]+x)[_-]", Pattern.CASE_INSENSITIVE);
        Matcher objectiveMatcher = regExPattern.matcher(imageName);
        if (objectiveMatcher.find()) {
            return objectiveMatcher.group(1);
        } else {
            return null;
        }
    }

    private boolean matchMIPChannelWithSegmentedImageChannel(int mipChannel, int segmentImageChannel) {
        if (mipChannel == -1 && segmentImageChannel == -1) {
            return true;
        } else if (mipChannel == -1)  {
            LOG.warn("No channel info found in the mip");
            return true;
        } else if (segmentImageChannel == -1) {
            LOG.warn("No channel info found in the segmented image");
            return true;
        } else {
            return mipChannel == segmentImageChannel;
        }
    }

    private boolean matchMIPObjectiveWithSegmentedImageObjective(String mipObjective, String segmentImageObjective) {
        if (StringUtils.isBlank(mipObjective) && StringUtils.isBlank(segmentImageObjective)) {
            return true;
        } else if (StringUtils.isBlank(mipObjective) )  {
            LOG.warn("No objective found in the mip");
            return false;
        } else if (StringUtils.isBlank(segmentImageObjective)) {
            // if the segmented image does not have objective match it against every image
            return true;
        } else {
            return StringUtils.equalsIgnoreCase(mipObjective, segmentImageObjective);
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

    private boolean isEmLibrary(String lname) {
        return lname != null && StringUtils.containsIgnoreCase(lname, "flyem") && StringUtils.containsIgnoreCase(lname, "hemibrain");
    }

    private boolean hasSample(ColorDepthMIP cdmip) {
        return cdmip.sample != null;
    }

    private boolean hasConsensusLine(ColorDepthMIP cdmip) {
        return !StringUtils.equalsIgnoreCase(NO_CONSENSUS, cdmip.sample.line);
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
                return (StringUtils.isNotBlank(publishingName) &&
                        !StringUtils.equalsIgnoreCase(NO_CONSENSUS, publishingName) || (missingPublishingHandling & 0x1) != 0) &&
                        (cdmip.sample.publishedToStaging || (missingPublishingHandling & 0x2) != 0);

            }
        }
    }

    private boolean hasPublishedImageURL(ColorDepthMIP cdmip) {
        return StringUtils.isNotBlank(cdmip.publicImageUrl);
    }

    private ColorDepthMetadata asLMLineMetadata(ColorDepthMIP cdmip, Function<ColorDepthMIP, String> libraryNameExtractor) {
        String libraryName = libraryNameExtractor.apply(cdmip);
        ColorDepthMetadata cdMetadata = new ColorDepthMetadata();
        cdMetadata.setId(cdmip.id);
        cdMetadata.setAlignmentSpace(cdmip.alignmentSpace);
        cdMetadata.setLibraryName(libraryName);
        cdMetadata.filepath = cdmip.filepath;
        cdMetadata.setImageURL(cdmip.publicImageUrl);
        cdMetadata.setThumbnailURL(cdmip.publicThumbnailUrl);
        cdMetadata.sourceImageRef = cdmip.sourceImageRef;
        cdMetadata.sampleRef = cdmip.sampleRef;
        if (cdmip.sample != null) {
            cdMetadata.setPublishedToStaging(cdmip.sample.publishedToStaging);
            cdMetadata.setLMLinePublishedName(cdmip.sample.publishingName);
            cdMetadata.setSlideCode(cdmip.sample.slideCode);
            cdMetadata.setGender(cdmip.sample.gender);
            cdMetadata.setMountingProtocol(cdmip.sample.mountingProtocol);
        } else {
            populateCDMetadataFromCDMIPName(cdmip, cdMetadata);
        }
        cdMetadata.setAnatomicalArea(cdmip.anatomicalArea);
        cdMetadata.setAlignmentSpace(cdmip.alignmentSpace);
        cdMetadata.setObjective(cdmip.objective);
        cdMetadata.setChannel(cdmip.channelNumber);
        return cdMetadata;
    }

    private void populateCDMetadataFromCDMIPName(ColorDepthMIP cdmip, ColorDepthMetadata cdMetadata) {
        List<String> mipNameComponents = Splitter.on('-').splitToList(cdmip.name);
        String line = mipNameComponents.size() > 0 ? mipNameComponents.get(0) : cdmip.name;
        // attempt to remove the PI initials
        int piSeparator = StringUtils.indexOf(line, '_');
        String lineID;
        if (piSeparator == -1) {
            lineID = line;
        } else {
            lineID = line.substring(piSeparator + 1);
        }
        cdMetadata.setLMLinePublishedName(StringUtils.defaultIfBlank(lineID, "Unknown"));
        String slideCode = mipNameComponents.size() > 1 ? mipNameComponents.get(1) : null;
        cdMetadata.setSlideCode(slideCode);
    }

    private ColorDepthMetadata asEMBodyMetadata(ColorDepthMIP cdmip, String defaultGender, Function<ColorDepthMIP, String> libraryNameExtractor) {
        String libraryName = libraryNameExtractor.apply(cdmip);
        ColorDepthMetadata cdMetadata = new ColorDepthMetadata();
        cdMetadata.setId(cdmip.id);
        cdMetadata.setAlignmentSpace(cdmip.alignmentSpace);
        cdMetadata.setLibraryName(libraryName);
        cdMetadata.filepath = cdmip.filepath;
        cdMetadata.setImageURL(cdmip.publicImageUrl);
        cdMetadata.setThumbnailURL(cdmip.publicThumbnailUrl);
        cdMetadata.setEMSkeletonPublishedName(extractEMSkeletonIdFromName(cdmip.name));
        cdMetadata.setGender(defaultGender);
        return cdMetadata;
    }

    private String extractEMSkeletonIdFromName(String name) {
        String[] mipNameComponents = StringUtils.split(name, "-_");
        if (mipNameComponents != null && mipNameComponents.length  > 0) {
            return mipNameComponents[0];
        } else {
            return null;
        }
    }

    private String extractEMNeuronStateFromName(String name, Pattern emNeuronStatePattern) {
        Matcher matcher = emNeuronStatePattern.matcher(name);
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return "";
        }
    }

    private Pair<String, Map<String, List<String>>> getSegmentedImages(Pattern indexingFieldRegExPattern, String segmentedMIPsBaseDir) {
        if (StringUtils.isBlank(segmentedMIPsBaseDir)) {
            return ImmutablePair.of("", Collections.emptyMap());
        } else {
            Path segmentdMIPsBasePath = Paths.get(segmentedMIPsBaseDir);
            Function<String, String> indexingFieldFromName = n -> {
                Matcher m = indexingFieldRegExPattern.matcher(n);
                if (m.find()) {
                    return m.group(1);
                } else {
                    LOG.warn("Indexing field could not be extracted from {} - no match found using {}", n, indexingFieldRegExPattern);
                    return null;
                }
            };

            if (Files.isDirectory(segmentdMIPsBasePath)) {
                return ImmutablePair.of("file", getSegmentedImagesFromDir(indexingFieldFromName, segmentdMIPsBasePath));
            } else if (Files.isRegularFile(segmentdMIPsBasePath)) {
                return ImmutablePair.of("zipEntry", getSegmentedImagesFromZip(indexingFieldFromName, segmentdMIPsBasePath.toFile()));
            } else {
                return ImmutablePair.of("file", Collections.emptyMap());
            }
        }
    }

    private Map<String, List<String>> getSegmentedImagesFromDir(Function<String, String> indexingFieldFromName, Path segmentedMIPsBasePath) {
        try {
            return Files.find(segmentedMIPsBasePath, MAX_SEGMENTED_DATA_DEPTH,
                    (p, fa) -> fa.isRegularFile())
                    .map(p -> p.getFileName().toString())
                    .filter(entryName -> StringUtils.isNotBlank(indexingFieldFromName.apply(entryName)))
                    .collect(Collectors.groupingBy(indexingFieldFromName));
        } catch (IOException e) {
            LOG.warn("Error scanning {} for segmented images", segmentedMIPsBasePath, e);
            return Collections.emptyMap();
        }
    }

    private Map<String, List<String>> getSegmentedImagesFromZip(Function<String, String> indexingFieldFromName, File segmentedMIPsFile) {
        ZipFile segmentedMIPsZipFile;
        try {
            segmentedMIPsZipFile = new ZipFile(segmentedMIPsFile);
        } catch (Exception e) {
            LOG.warn("Error opening segmented mips archive {}", segmentedMIPsFile, e);
            return Collections.emptyMap();
        }
        try {
            return segmentedMIPsZipFile.stream()
                    .filter(ze -> !ze.isDirectory())
                    .map(ZipEntry::getName)
                    .filter(entryName -> StringUtils.isNotBlank(indexingFieldFromName.apply(entryName)))
                    .collect(Collectors.groupingBy(indexingFieldFromName));
        } finally {
            try {
                segmentedMIPsZipFile.close();
            } catch (IOException ignore) {
            }
        }
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
