package org.janelia.colormipsearch.cmd;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.api.cds.FileDataUtils;
import org.janelia.colormipsearch.api.cds.NeuronMIPUtils;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.EMNeuronMetadata;
import org.janelia.colormipsearch.model.FileData;
import org.janelia.colormipsearch.model.FileType;
import org.janelia.colormipsearch.model.Gender;
import org.janelia.colormipsearch.model.LMNeuronMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prepare the data input for Color Depth Search. These data input could potentially be a Mongo database or a JSON file
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class CreateColorDepthSearchDataInputCmd extends AbstractCmd {

    private static final Logger LOG = LoggerFactory.getLogger(CreateColorDepthSearchDataInputCmd.class);

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

    @Parameters(commandDescription = "Create input data for CDS")
    static class CreateColorDepthSearchDataInputArgs extends AbstractCmdArgs {
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

        @Parameter(names = {"--releases", "-r"},
                description = "Which specific releases to be included.",
                required = false, variableArity = true)
        List<String> releases;

        @Parameter(names = {"--librariesVariants", "--libraryVariants"},
                description = "Libraries variants descriptors. " +
                        "A library variant contains library name, variant type, location and suffix separated by colon , e.g., " +
                        "flylight_gen1_mcfo_published:segmentation:/libDir/mcfo/segmentation:_CDM",
                converter = LibraryVariantArg.LibraryVariantArgConverter.class,
                validateValueWith = LibraryVariantArg.ListLibraryVariantArgValidator.class,
                variableArity = true)
        List<LibraryVariantArg> libraryVariants;

        @DynamicParameter(names = "--variant-filetype-mapping", description = "Dynamic variant to file type mapping")
        Map<String, String> variantFileTypeMapping = new HashMap<String, String>() {{
            put("cdm", FileType.ColorDepthMipInput.name());
            put("searchable_neurons", FileType.ColorDepthMipInput.name());
            put("segmentation", FileType.ColorDepthMipInput.name());
            put("grad", FileType.GradientImage.name());
            put("gradient", FileType.GradientImage.name());
            put("zgap", FileType.RGBZGapImage.name());
            put("rgb", FileType.RGBZGapImage.name());
        }};

        @Parameter(names = "--included-libraries", variableArity = true, description = "If set, MIPs should also be in all these libraries")
        Set<String> includedLibraries;

        @Parameter(names = "--excluded-libraries", variableArity = true, description = "If set, MIPs should not be part of any of these libraries")
        Set<String> excludedLibraries;

        @Parameter(names = {"--datasets"}, description = "Which datasets to extract", variableArity = true)
        List<String> datasets;

        @Parameter(names = "--include-original-mip-in-segmentation",
                   description = "If set include original mip in the segmentation",
                   arity = 0)
        boolean includeOriginalWithSegmentation = false;

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

        CreateColorDepthSearchDataInputArgs(CommonArgs commonArgs) {
            this.commonArgs = commonArgs;
        }

        @Override
        List<String> validate() {
            return Collections.emptyList();
        }
    }

    static class LibraryPathsArgs {
        ListArg library;
        List<LibraryVariantArg> libraryVariants;

        List<LibraryVariantArg> listLibraryVariants() {
            return CollectionUtils.isNotEmpty(libraryVariants) ? libraryVariants : Collections.emptyList();
        }

        Optional<LibraryVariantArg> getLibraryVariant(String variantType) {
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

    private final CreateColorDepthSearchDataInputArgs args;
    private final ObjectMapper mapper;

    CreateColorDepthSearchDataInputCmd(String commandName, org.janelia.colormipsearch.cmd.CommonArgs commonArgs) {
        super(commandName);
        args = new CreateColorDepthSearchDataInputArgs(commonArgs);
        this.mapper = new ObjectMapper()
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                ;
    }

    @Override
    CreateColorDepthSearchDataInputArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        Map<String, List<LibraryVariantArg>> libraryVariants = getLibraryVariants();
        Set<? extends AbstractNeuronMetadata> excludedNeurons = getExcludedNeurons();

        args.libraries.stream()
                .map(larg -> {
                    LibraryPathsArgs lpaths = new LibraryPathsArgs();
                    lpaths.library = larg;
                    lpaths.libraryVariants = libraryVariants.get(larg.input);
                    return lpaths;
                })
                .forEach(lpaths -> {
                    if (StringUtils.isNotBlank(args.dataServiceURL)) {
                        Client httpClient = createHttpClient();
                        Map<String, String> libraryNameMapping = retrieveLibraryNameMapping(httpClient, args.libraryMappingURL);
                        WebTarget serverEndpoint = httpClient.target(args.dataServiceURL);
                        createColorDepthSearchInputData(
                                serverEndpoint,
                                lpaths,
                                getVariantForFileType(FileType.ColorDepthMipInput),
                                excludedNeurons,
                                libraryNameMapping::get,
                                getNeuronFileURLMapper(),
                                Paths.get(args.commonArgs.outputDir),
                                args.outputFileName
                        );
                    } else {
                        // generate the input in offline mode
                    }
                });
    }

    private Map<String, List<LibraryVariantArg>> getLibraryVariants() {
        Map<String, List<LibraryVariantArg>> libraryVariants;
        if (CollectionUtils.isEmpty(args.libraryVariants)) {
            libraryVariants = Collections.emptyMap();
        } else {
            libraryVariants = args.libraryVariants.stream().collect(Collectors.groupingBy(lv -> lv.libraryName, Collectors.toList()));
        }
        return libraryVariants;
    }

    private <N extends AbstractNeuronMetadata> Set<N> getExcludedNeurons() {
        if (CollectionUtils.isEmpty(args.excludedMIPs)) {
            return Collections.emptySet();
        } else {
            return args.excludedMIPs.stream()
                    .flatMap(mipsInput -> {
                        List<N> neurons = NeuronMIPUtils.loadNeuronMetadataFromJSON(
                                mipsInput.input,
                                mipsInput.offset,
                                mipsInput.length,
                                Collections.emptySet(),
                                mapper);
                        return neurons.stream();
                    })
                    .collect(Collectors.toSet());
        }
    }

    private Function<String, String> getNeuronFileURLMapper() {
        return aUrl -> {
            if (StringUtils.isBlank(aUrl)) {
                return "";
            } else if (StringUtils.startsWithIgnoreCase(aUrl, "https://") ||
                    StringUtils.startsWithIgnoreCase(aUrl, "http://")) {
                if (args.urlsRelativeTo >= 0) {
                    URI uri = URI.create(aUrl);
                    Path uriPath = Paths.get(uri.getPath());
                    return uriPath.subpath(args.urlsRelativeTo, uriPath.getNameCount()).toString();
                } else {
                    return aUrl;
                }
            } else {
                return aUrl;
            }
        };
    }

    private void createColorDepthSearchInputData(WebTarget serverEndpoint,
                                                 LibraryPathsArgs libraryPaths,
                                                 String segmentationVariantType,
                                                 Set<? extends AbstractNeuronMetadata> excludedNeurons,
                                                 Function<String, String> libraryNamesMapping,
                                                 Function<String, String> neuronFileURLMapping,
                                                 Path outputPath,
                                                 String outputFilename) {

        int cdmsCount = countColorDepthMips(
                serverEndpoint,
                args.authorization,
                args.alignmentSpace,
                libraryPaths.library.input,
                args.datasets,
                args.releases);
        LOG.info("Found {} entities in library {} with alignment space {}{}",
                cdmsCount, libraryPaths.getLibraryName(), args.alignmentSpace, CollectionUtils.isNotEmpty(args.datasets) ? " for datasets " + args.datasets : "");
        int to = libraryPaths.library.length > 0 ? Math.min(libraryPaths.library.offset + libraryPaths.library.length, cdmsCount) : cdmsCount;

        Function<ColorDepthMIP, String> libraryNameExtractor = cmip -> {
            String internalLibraryName = cmip.findLibrary(libraryPaths.getLibraryName());
            if (StringUtils.isBlank(internalLibraryName)) {
                return null;
            } else {
                String displayLibraryName = libraryNamesMapping.apply(internalLibraryName);
                if (StringUtils.isBlank(displayLibraryName)) {
                    return internalLibraryName;
                } else {
                    return displayLibraryName;
                }
            }
        };

        CDSDataInputGenerator gen = new JSONCDSDataInputGenerator(
                outputPath,
                StringUtils.defaultIfBlank(outputFilename, libraryPaths.getLibraryName()),
                libraryPaths.library.offset,
                to,
                args.appendOutput,
                mapper)
                .prepare();

        String librarySegmentationPath = libraryPaths.getLibraryVariantPath(segmentationVariantType);
        Pair<FileData.FileDataType, Map<String, List<String>>> segmentedImages =
                MIPsHandlingUtils.getLibraryImageFiles(libraryPaths.library.input, librarySegmentationPath, libraryPaths.getLibraryVariantNameSuffix(segmentationVariantType));

        for (int pageOffset = libraryPaths.library.offset; pageOffset < to; pageOffset += DEFAULT_PAGE_LENGTH) {
            int currentPageSize = Math.min(DEFAULT_PAGE_LENGTH, to - pageOffset);
            List<ColorDepthMIP> cdmipsPage = retrieveColorDepthMipsWithSamples(
                    serverEndpoint,
                    args.authorization,
                    args.alignmentSpace,
                    libraryPaths.library,
                    args.datasets,
                    args.releases,
                    pageOffset,
                    currentPageSize);
            LOG.info("Process {} entries from {} to {} out of {}", cdmipsPage.size(), pageOffset, pageOffset + currentPageSize, cdmsCount);
            cdmipsPage.stream()
                    .filter(cdmip -> checkLibraries(cdmip, args.includedLibraries, args.excludedLibraries))
                    .map(cdmip -> MIPsHandlingUtils.isEmLibrary(libraryPaths.getLibraryName())
                            ? asEMNeuron(cdmip, libraryNameExtractor, neuronFileURLMapping, Gender.fromVal(args.defaultGender))
                            : asLMNeuron(cdmip, libraryNameExtractor, neuronFileURLMapping))
                    .flatMap(cdmip -> MIPsHandlingUtils.findSegmentedMIPs(cdmip, librarySegmentationPath,  segmentedImages, args.includeOriginalWithSegmentation, args.segmentedImageChannelBase).stream())
                    .peek(cdmip -> populateOtherVariantsBasedOnSegmentation(
                            cdmip,
                            EnumSet.of(FileType.GradientImage, FileType.RGBZGapImage),
                            libraryPaths.libraryVariants,
                            libraryPaths.getLibraryVariant(segmentationVariantType).orElse(null)))
                    .forEach(cdmip -> {
                        gen.write(cdmip);
                    });
        }
        gen.done();
    }

    private void populateOtherVariantsBasedOnSegmentation(AbstractNeuronMetadata cdmip,
                                                          Set<FileType> fileTypes,
                                                          List<LibraryVariantArg> libraryVariants,
                                                          LibraryVariantArg segmentationVariant) {
        String librarySegmentationSuffix = segmentationVariant == null ? null : segmentationVariant.variantTypeSuffix;
        libraryVariants.stream()
                .filter(variant -> fileTypes.contains(FileType.fromName(args.variantFileTypeMapping.get(variant.variantType))))
                .forEach(variant -> {
                    FileType variantFileType = FileType.fromName(args.variantFileTypeMapping.get(variant.variantType));
                    FileData variantFileData = FileDataUtils.lookupVariantFileData(
                            cdmip.getNeuronFileName(FileType.ColorDepthMipInput),
                            cdmip.getSourceFilepath(),
                            Collections.singletonList(variant.variantPath),
                            variant.variantNameSuffix,
                            nc -> {
                                String suffix = StringUtils.defaultIfBlank(variant.variantTypeSuffix, "");
                                if (StringUtils.isNotBlank(librarySegmentationSuffix)) {
                                    return StringUtils.replaceIgnoreCase(nc, librarySegmentationSuffix, "") + suffix;
                                } else {
                                    return nc + suffix;
                                }
                            }
                    );
                    cdmip.setNeuronFileData(variantFileType, variantFileData);
                });

    }

    private boolean checkLibraries(ColorDepthMIP cdmip, Set<String> includedLibraries, Set<String> excludedLibraries) {
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

    private EMNeuronMetadata asEMNeuron(ColorDepthMIP cdmip,
                                        Function<ColorDepthMIP, String> libraryNameExtractor,
                                        Function<String, String> urlMapping,
                                        Gender defaultGenderForEMNeuron) {
        String libraryName = libraryNameExtractor.apply(cdmip);
        EMNeuronMetadata neuronMetadata = new EMNeuronMetadata();
        neuronMetadata.setId(cdmip.id);
        neuronMetadata.setSourceFilepath(cdmip.filepath);
        neuronMetadata.setAlignmentSpace(cdmip.alignmentSpace);
        neuronMetadata.setLibraryName(libraryName);
        neuronMetadata.setNeuronType(cdmip.neuronType);
        neuronMetadata.setNeuronInstance(cdmip.neuronInstance);
        neuronMetadata.setNeuronFileData(FileType.ColorDepthMip, FileData.fromString(urlMapping.apply(cdmip.publicImageUrl)));
        neuronMetadata.setNeuronFileData(FileType.ColorDepthMipThumbnail, FileData.fromString(urlMapping.apply(cdmip.publicThumbnailUrl)));
        neuronMetadata.setPublishedName(cdmip.bodyId != null ? String.valueOf(cdmip.bodyId) : null);
        neuronMetadata.setGender(defaultGenderForEMNeuron);
        if (cdmip.emBody != null && cdmip.emBody.files != null) {
            neuronMetadata.setNeuronFileData(FileType.AlignedBodySWC, FileData.fromString(cdmip.emBody.files.get("SkeletonSWC")));
        }
        return neuronMetadata;
    }

    private LMNeuronMetadata asLMNeuron(ColorDepthMIP cdmip,
                                        Function<ColorDepthMIP, String> libraryNameExtractor,
                                        Function<String, String> urlMapping) {
        String libraryName = libraryNameExtractor.apply(cdmip);
        LMNeuronMetadata neuronMetadata = new LMNeuronMetadata();
        neuronMetadata.setId(cdmip.id);
        neuronMetadata.setSourceFilepath(cdmip.filepath);
        neuronMetadata.setAlignmentSpace(cdmip.alignmentSpace);
        neuronMetadata.setLibraryName(libraryName);
        neuronMetadata.setSampleRef(cdmip.sampleRef);
        neuronMetadata.setAnatomicalArea(cdmip.anatomicalArea);
        neuronMetadata.setObjective(cdmip.objective);
        neuronMetadata.setChannel(Integer.valueOf(cdmip.channelNumber));
        neuronMetadata.setNeuronFileData(FileType.ColorDepthMip, FileData.fromString(urlMapping.apply(cdmip.publicImageUrl)));
        neuronMetadata.setNeuronFileData(FileType.ColorDepthMipThumbnail, FileData.fromString(urlMapping.apply(cdmip.publicThumbnailUrl)));
        neuronMetadata.setNeuronFileData(FileType.VisuallyLosslessStack, FileData.fromString(cdmip.sample3DImageStack));
        neuronMetadata.setNeuronFileData(FileType.SignalMipExpression, FileData.fromString(cdmip.sampleGen1Gal4ExpressionImage));
        if (cdmip.sample != null) {
            neuronMetadata.setPublishedName(cdmip.sample.publishingName);
            neuronMetadata.setSlideCode(cdmip.sample.slideCode);
            neuronMetadata.setGender(Gender.fromVal(cdmip.sample.gender));
            neuronMetadata.setMountingProtocol(cdmip.sample.mountingProtocol);
        } else {
            populateNeuronDataFromCDMIPName(cdmip.name, neuronMetadata);
        }
        return neuronMetadata;
    }

    private String getVariantForFileType(FileType fileType) {
        return args.variantFileTypeMapping.entrySet().stream()
                .filter(e -> e.getValue().equalsIgnoreCase(fileType.name()))
                .map(e -> e.getKey())
                .findFirst()
                .orElse(null);
    }

    private void populateNeuronDataFromCDMIPName(String mipName, LMNeuronMetadata neuronMetadata) {
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
        neuronMetadata.setPublishedName(StringUtils.defaultIfBlank(lineID, "Unknown"));
        String slideCode = mipNameComponents.length > 1 ? mipNameComponents[1] : null;
        neuronMetadata.setSlideCode(slideCode);
        int colorChannel = MIPsHandlingUtils.extractColorChannelFromMIPName(mipName, 0);
        if (colorChannel != -1) {
            neuronMetadata.setChannel(colorChannel);
        }
        neuronMetadata.setObjective(MIPsHandlingUtils.extractObjectiveFromMIPName(mipName));
        String gender = MIPsHandlingUtils.extractGenderFromMIPName(mipName);
        if (gender != null) {
            neuronMetadata.setGender(Gender.fromVal(gender));
        }
    }

    private int countColorDepthMips(WebTarget serverEndpoint, String credentials, String alignmentSpace, String library, List<String> datasets, List<String> releases) {
        WebTarget target = serverEndpoint.path("/data/colorDepthMIPsCount")
                .queryParam("libraryName", library)
                .queryParam("alignmentSpace", alignmentSpace)
                .queryParam("dataset", datasets != null ? datasets.stream().filter(StringUtils::isNotBlank).reduce((s1, s2) -> s1 + "," + s2).orElse(null) : null)
                .queryParam("release", releases != null ? releases.stream().filter(StringUtils::isNotBlank).reduce((s1, s2) -> s1 + "," + s2).orElse(null) : null);
        LOG.info("Count color depth mips using {}, l={}, as={}, ds={}, rs={}", target, library, alignmentSpace, datasets, releases);
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
                                                                  List<String> releases,
                                                                  int offset,
                                                                  int pageLength) {
        return retrieveColorDepthMips(serverEndpoint.path("/data/colorDepthMIPsWithSamples")
                        .queryParam("libraryName", libraryArg.input)
                        .queryParam("alignmentSpace", alignmentSpace)
                        .queryParam("dataset", datasets != null ? datasets.stream().filter(StringUtils::isNotBlank).reduce((s1, s2) -> s1 + "," + s2).orElse(null) : null)
                        .queryParam("release", releases != null ? releases.stream().filter(StringUtils::isNotBlank).reduce((s1, s2) -> s1 + "," + s2).orElse(null) : null)
                        .queryParam("offset", offset)
                        .queryParam("length", pageLength),
                credentials)
                .stream()
                .map(colorDepthMIP -> retrieve3DImageStack(colorDepthMIP, serverEndpoint, credentials))
                .collect(Collectors.toList())
                ;
    }

    private List<ColorDepthMIP> retrieveColorDepthMips(WebTarget endpoint, String credentials) {
        LOG.info("Get mips from {}", endpoint);
        Response response = createRequestWithCredentials(endpoint.request(MediaType.APPLICATION_JSON), credentials).get();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new IllegalStateException("Invalid response from " + endpoint.getUri() + " -> " + response);
        } else {
            return response.readEntity(new GenericType<>(new TypeReference<List<ColorDepthMIP>>() {
            }.getType()));
        }
    }

    private ColorDepthMIP retrieve3DImageStack(ColorDepthMIP cdmip, WebTarget endpoint, String credentials) {
        if (cdmip.sample != null) {
            // only for LM images
            WebTarget refImageEndpoint = endpoint.path("/publishedImage/imageWithGen1Image")
                    .path(cdmip.alignmentSpace)
                    .path(cdmip.sample.slideCode)
                    .path(cdmip.objective);
            Response response = createRequestWithCredentials(refImageEndpoint.request(MediaType.APPLICATION_JSON), credentials).get();
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                Map<String, SamplePublishedData> publishedImages = response.readEntity(new GenericType<>(new TypeReference<Map<String, SamplePublishedData>>() {}.getType()));
                SamplePublishedData sample3DImage = publishedImages.get("VisuallyLosslessStack");
                SamplePublishedData gen1Gal4ExpressionImage = publishedImages.get("SignalMipExpression");
                cdmip.sample3DImageStack = sample3DImage != null ? sample3DImage.files.get("VisuallyLosslessStack") : null;
                cdmip.sampleGen1Gal4ExpressionImage = gen1Gal4ExpressionImage != null ? gen1Gal4ExpressionImage.files.get("ColorDepthMip1") : null;
            }
        }
        return cdmip;
    }

    private Client createHttpClient() {
        try {
            SSLContext sslContext = createSSLContext();

            JacksonJsonProvider jsonProvider = new JacksonJaxbJsonProvider()
                    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

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

    private SSLContext createSSLContext() {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
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
            return sslContext;
        } catch (Exception e) {
            throw new IllegalStateException("Error initilizing SSL context", e);
        }
    }

    private Invocation.Builder createRequestWithCredentials(Invocation.Builder requestBuilder, String credentials) {
        if (StringUtils.isNotBlank(credentials)) {
            return requestBuilder.header("Authorization", credentials);
        } else {
            return requestBuilder;
        }
    }

    static Map<String, String> retrieveLibraryNameMapping(Client httpClient, String configURL) {
        Response response = httpClient.target(configURL).request(MediaType.APPLICATION_JSON).get();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new IllegalStateException("Invalid response from " + configURL + " -> " + response);
        }
        Map<String, Object> configJSON = response.readEntity(new GenericType<>(new TypeReference<Map<String, Object>>() {
        }.getType()));
        Object configEntry = configJSON.get("config");
        if (!(configEntry instanceof Map)) {
            LOG.error("Config entry from {} is null or it's not a map", configJSON);
            throw new IllegalStateException("Config entry not found");
        }
        Map<String, String> cdmLibraryNamesMapping = new HashMap<>();
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> configEntryMap = (Map<String, Map<String, Object>>) configEntry;
        configEntryMap.forEach((lid, ldata) -> {
            String lname = (String) ldata.get("name");
            cdmLibraryNamesMapping.put(lid, lname);
        });
        LOG.info("Using {} for mapping library names", cdmLibraryNamesMapping);
        return cdmLibraryNamesMapping;
    }

}
