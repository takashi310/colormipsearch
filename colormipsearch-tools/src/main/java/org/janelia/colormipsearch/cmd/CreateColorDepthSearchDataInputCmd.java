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
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.api.cds.FileDataUtils;
import org.janelia.colormipsearch.api.cds.NeuronMIPUtils;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.ComputeFileType;
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
            put("cdm", ComputeFileType.InputColorDepthImage.name());
            put("searchable_neurons", ComputeFileType.InputColorDepthImage.name());
            put("segmentation", ComputeFileType.InputColorDepthImage.name());
            put("fl", ComputeFileType.InputColorDepthImage.name());
            put("grad", ComputeFileType.GradientImage.name());
            put("gradient", ComputeFileType.GradientImage.name());
            put("zgap", ComputeFileType.ZGapImage.name());
            put("rgb", ComputeFileType.ZGapImage.name());
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

        CreateColorDepthSearchDataInputArgs(CommonArgs commonArgs) {
            super(commonArgs);
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

        String getLibraryName() {
            return library != null ? library.input : null;
        }
    }

    private final CreateColorDepthSearchDataInputArgs args;
    private final ObjectMapper mapper;

    CreateColorDepthSearchDataInputCmd(String commandName, CommonArgs commonArgs) {
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
                                getAllVariantsForComputeFileType(ComputeFileType.InputColorDepthImage),
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
                                                 Set<String> computationInputVariantTypes,
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

        Optional<LibraryVariantArg> inputLibraryVariantChoice = computationInputVariantTypes.stream()
                .map(variantType -> libraryPaths.getLibraryVariant(variantType).orElse(null))
                .filter(lv -> lv != null)
                .findFirst()
                ;
        Pair<FileData.FileDataType, Map<String, List<String>>> inputImages =
                MIPsHandlingUtils.getLibraryImageFiles(
                        libraryPaths.library.input,
                        inputLibraryVariantChoice.map(lv -> lv.variantPath).orElse(null),
                        inputLibraryVariantChoice.map(lv -> lv.variantNameSuffix).orElse(null));
        LOG.info("Found {} input codes for {}",
                inputImages.getRight().size(),
                inputLibraryVariantChoice);

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
                    .flatMap(cdmip -> MIPsHandlingUtils.findNeuronMIPs(cdmip,
                            inputLibraryVariantChoice.map(lv -> lv.variantPath).orElse(null),
                            inputImages,
                            args.includeOriginalWithSegmentation,
                            args.segmentedImageChannelBase).stream())
                    .filter(cdmip -> CollectionUtils.isEmpty(excludedNeurons) || !CollectionUtils.containsAny(excludedNeurons, cdmip))
                    .peek(cdmip -> populateOtherComputeFilesFromInput(
                            cdmip,
                            EnumSet.of(ComputeFileType.GradientImage, ComputeFileType.ZGapImage),
                            libraryPaths.listLibraryVariants(),
                            inputLibraryVariantChoice.orElse(null)))
                    .peek(cdmip -> updateNeuronFiles(cdmip))
                    .forEach(gen::write);
        }
        gen.done();
    }

    private void populateOtherComputeFilesFromInput(AbstractNeuronMetadata cdmip,
                                                    Set<ComputeFileType> computeFileTypes,
                                                    List<LibraryVariantArg> libraryVariants,
                                                    LibraryVariantArg computeInputVariant) {
        String computeInputVarianSuffix = computeInputVariant == null ? null : computeInputVariant.variantTypeSuffix;
        libraryVariants.stream()
                .filter(variant -> computeFileTypes.contains(ComputeFileType.fromName(args.variantFileTypeMapping.get(variant.variantType))))
                .forEach(variant -> {
                    ComputeFileType variantFileType = ComputeFileType.fromName(args.variantFileTypeMapping.get(variant.variantType));
                    FileData variantFileData = FileDataUtils.lookupVariantFileData(
                            cdmip.getComputeFileName(ComputeFileType.InputColorDepthImage),
                            cdmip.getComputeFileName(ComputeFileType.SourceColorDepthImage),
                            Collections.singletonList(variant.variantPath),
                            variant.variantNameSuffix,
                            nc -> {
                                String suffix = StringUtils.defaultIfBlank(variant.variantTypeSuffix, "");
                                if (StringUtils.isNotBlank(computeInputVarianSuffix)) {
                                    return StringUtils.replaceIgnoreCase(nc, computeInputVarianSuffix, "") + suffix;
                                } else {
                                    return nc + suffix;
                                }
                            }
                    );
                    cdmip.setComputeFileData(variantFileType, variantFileData);
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
        neuronMetadata.setAlignmentSpace(cdmip.alignmentSpace);
        neuronMetadata.setLibraryName(libraryName);
        neuronMetadata.setNeuronType(cdmip.neuronType);
        neuronMetadata.setNeuronInstance(cdmip.neuronInstance);
        neuronMetadata.setComputeFileData(ComputeFileType.SourceColorDepthImage, FileData.fromString(cdmip.filepath));
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
        neuronMetadata.setAlignmentSpace(cdmip.alignmentSpace);
        neuronMetadata.setLibraryName(libraryName);
        neuronMetadata.setSampleRef(cdmip.sampleRef);
        neuronMetadata.setAnatomicalArea(cdmip.anatomicalArea);
        neuronMetadata.setObjective(cdmip.objective);
        neuronMetadata.setChannel(Integer.valueOf(cdmip.channelNumber));
        neuronMetadata.setComputeFileData(ComputeFileType.SourceColorDepthImage, FileData.fromString(cdmip.filepath));
        neuronMetadata.setNeuronFileData(FileType.ColorDepthMip, FileData.fromString(urlMapping.apply(cdmip.publicImageUrl)));
        neuronMetadata.setNeuronFileData(FileType.ColorDepthMipThumbnail, FileData.fromString(urlMapping.apply(cdmip.publicThumbnailUrl)));
        neuronMetadata.setNeuronFileData(FileType.VisuallyLosslessStack, FileData.fromString(cdmip.sample3DImageStack));
        neuronMetadata.setNeuronFileData(FileType.SignalMipExpression, FileData.fromString(cdmip.sampleGen1Gal4ExpressionImage));
        if (cdmip.sample != null) {
            neuronMetadata.setPublishedName(cdmip.sample.publishingName);
            neuronMetadata.setSlideCode(cdmip.sample.slideCode);
            neuronMetadata.setGender(Gender.fromVal(cdmip.sample.gender));
            neuronMetadata.setMountingProtocol(cdmip.sample.mountingProtocol);
            neuronMetadata.setDriver(cdmip.sample.driver);
        } else {
            populateNeuronDataFromCDMIPName(cdmip.name, neuronMetadata);
        }
        return neuronMetadata;
    }

    private Set<String> getAllVariantsForComputeFileType(ComputeFileType computeFileType) {
        return args.variantFileTypeMapping.entrySet().stream()
                .filter(e -> e.getValue().equalsIgnoreCase(computeFileType.name()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
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

    private <N extends AbstractNeuronMetadata> void updateNeuronFiles(N cdmip) {
        if (cdmip.hasComputeFile(ComputeFileType.InputColorDepthImage) && cdmip.hasNeuronFile(FileType.ColorDepthMip)) {
            // ColorDepthInput filename must be expressed in terms of publishedName (not internal name),
            //  and must include the integer suffix that identifies exactly which image it is (for LM),
            //  when there are multiple images for a given combination of parameters
            // in practical terms, we take the filename from imageURL, which has
            //  the publishedName in it, and graft on the required integer from imageName (for LM), which
            //  has the internal name; we have to similarly grab _FL from EM names

            // remove directories and extension (which we know is ".png") from imageURL:
            Path imagePath = Paths.get(cdmip.getNeuronFileName(FileType.ColorDepthMip));
            String colorDepthInputName = createColorDepthInputName(
                    Paths.get(cdmip.getComputeFileName(ComputeFileType.SourceColorDepthImage)).getFileName().toString(),
                    Paths.get(cdmip.getComputeFileName(ComputeFileType.InputColorDepthImage)).getFileName().toString(),
                    imagePath.getFileName().toString());
            cdmip.setNeuronFileData(FileType.ColorDepthMipInput, FileData.fromString(colorDepthInputName));
        }
        if (!cdmip.hasNeuronFile(FileType.ColorDepthMip)) {
            // set relative image URLs
            String imageRelativeURL;
            if (MIPsHandlingUtils.isEmLibrary(cdmip.getLibraryName())) {
                imageRelativeURL = createEMImageRelativeURL((EMNeuronMetadata) cdmip);
            } else {
                imageRelativeURL = createLMImageRelativeURL((LMNeuronMetadata) cdmip);
            }
            cdmip.setNeuronFileData(FileType.ColorDepthMip, FileData.fromString(imageRelativeURL));
            cdmip.setNeuronFileData(FileType.ColorDepthMipThumbnail, FileData.fromString(imageRelativeURL));
        }
    }

    /**
     * Create the published name for the input image - the one that will actually be "color depth searched".
     * @param mipFileName
     * @param imageFileName
     * @param displayFileName
     * @return
     */
    private String createColorDepthInputName(String mipFileName, String imageFileName, String displayFileName) {
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

    private String createEMImageRelativeURL(EMNeuronMetadata cdmip) {
        String imageName = cdmip.hasComputeFile(ComputeFileType.InputColorDepthImage)
                ? Paths.get(cdmip.getComputeFileName(ComputeFileType.InputColorDepthImage)).getFileName().toString()
                : Paths.get(cdmip.getComputeFileName(ComputeFileType.SourceColorDepthImage)).getFileName().toString();
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

    private String createLMImageRelativeURL(LMNeuronMetadata cdmip) {
        String imageName = cdmip.hasComputeFile(ComputeFileType.InputColorDepthImage)
                ? Paths.get(cdmip.getComputeFileName(ComputeFileType.InputColorDepthImage)).getFileName().toString()
                : Paths.get(cdmip.getComputeFileName(ComputeFileType.SourceColorDepthImage)).getFileName().toString();
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
            return response.readEntity(new GenericType<>(new TypeReference<List<ColorDepthMIP>>() {}.getType()));
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
