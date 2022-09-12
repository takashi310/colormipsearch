package org.janelia.colormipsearch.cmd;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.cmd.jacsdata.ColorDepthMIP;
import org.janelia.colormipsearch.dataio.CDMIPsWriter;
import org.janelia.colormipsearch.dataio.db.DBCDMIPsWriter;
import org.janelia.colormipsearch.dataio.fs.JSONCDMIPsWriter;
import org.janelia.colormipsearch.mips.FileDataUtils;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.EMNeuronEntity;
import org.janelia.colormipsearch.model.FileData;
import org.janelia.colormipsearch.model.Gender;
import org.janelia.colormipsearch.model.LMNeuronEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prepare the data input for Color Depth Search. These data input could potentially be a Mongo database or a JSON file
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
class CreateCDSDataInputCmd extends AbstractCmd {

    private static final Logger LOG = LoggerFactory.getLogger(CreateCDSDataInputCmd.class);

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

        @Parameter(names = {"--authorization"}, description = "JACS authorization - this is the value of the authorization header")
        String authorization;

        @Parameter(names = {"--alignment-space", "-as"}, description = "Alignment space: {JRC2018_Unisex_20x_HR, JRC2018_VNC_Unisex_40x_DS} ", required = true)
        String alignmentSpace;

        @Parameter(names = {"--library", "-l"},
                description = "Which library to extract such as {flyem_hemibrain, flylight_gen1_gal4, flylight_gen1_lexa, flylight_gen1_mcfo_case_1, flylight_splitgal4_drivers}. " +
                        "The format is <libraryName>[:<offset>[:<length>]]",
                required = true, converter = ListArg.ListArgConverter.class)
        ListArg library;

        @Parameter(names = {"--releases", "-r"},
                description = "Which specific releases to be included.",
                variableArity = true)
        List<String> releases;

        @Parameter(names = {"--mips"},
                description = "If set only create inputs for these specific mips",
                variableArity = true)
        List<String> includedMIPs;

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

        @Parameter(names = {"--tag"}, description = "Tag to assign to the imported mips")
        String tag;

        @Parameter(names = "--include-original-mip-in-segmentation",
                description = "If set include original mip in the segmentation",
                arity = 0)
        boolean includeOriginalWithSegmentation = false;

        @Parameter(names = "--segmentation-channel-base", description = "Segmentation channel base (0 or 1)", validateValueWith = ChannelBaseValidator.class)
        int segmentedImageChannelBase = 1;

        @Parameter(names = {"--excluded-neurons"}, variableArity = true,
                description = "Comma-delimited list of LM slide codes or EM body ids to be excluded from the requested list")
        List<String> excludedNeurons;

        @Parameter(names = {"--output-filename"}, description = "Output file name")
        String outputFileName;

        @Parameter(names = {"--append-output"}, description = "Append output if it exists", arity = 0)
        boolean appendOutput;

        CreateColorDepthSearchDataInputArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }

        String getOutputFileName() {
            return StringUtils.defaultIfBlank(outputFileName, library.input);
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

    CreateCDSDataInputCmd(String commandName, CommonArgs commonArgs) {
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
        Set<String> excludedNeurons = getExcludedNeurons();

        LibraryPathsArgs lpaths = new LibraryPathsArgs();
        lpaths.library = args.library;
        lpaths.libraryVariants = libraryVariants.get(lpaths.getLibraryName());

        if (StringUtils.isNotBlank(args.dataServiceURL)) {
            Client httpClient = HttpHelper.createClient();
            WebTarget serverEndpoint = httpClient.target(args.dataServiceURL);
            createColorDepthSearchInputData(
                    serverEndpoint,
                    lpaths,
                    getAllVariantsForColorDepthInput(),
                    excludedNeurons
            );
        } else {
            // offline mode is not supported yet in this version
            LOG.error("No datasservice URL has been provided");
        }
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

    private Set<String> getExcludedNeurons() {
        if (CollectionUtils.isEmpty(args.excludedNeurons)) {
            return Collections.emptySet();
        } else {
            return new HashSet<>(args.excludedNeurons);
        }
    }

    private void createColorDepthSearchInputData(WebTarget serverEndpoint,
                                                 LibraryPathsArgs libraryPaths,
                                                 Set<String> computationInputVariantTypes,
                                                 Set<String> excludedNeurons) {

        int cdmsCount = countColorDepthMips(
                serverEndpoint,
                args.authorization,
                args.alignmentSpace,
                libraryPaths.library.input,
                args.datasets,
                args.releases,
                args.includedMIPs);
        LOG.info("Found {} entities in library {} with alignment space {}{}",
                cdmsCount, libraryPaths.getLibraryName(), args.alignmentSpace, CollectionUtils.isNotEmpty(args.datasets) ? " for datasets " + args.datasets : "");
        int to = libraryPaths.library.length > 0 ? Math.min(libraryPaths.library.offset + libraryPaths.library.length, cdmsCount) : cdmsCount;

        Function<ColorDepthMIP, String> libraryNameExtractor = cmip -> cmip.findLibrary(libraryPaths.getLibraryName());

        CDMIPsWriter gen = getCDSInputWriter();

        Optional<LibraryVariantArg> inputLibraryVariantChoice = computationInputVariantTypes.stream()
                .map(variantType -> libraryPaths.getLibraryVariant(variantType).orElse(null))
                .filter(Objects::nonNull)
                .findFirst();
        Pair<FileData.FileDataType, Map<String, List<String>>> inputImages =
                MIPsHandlingUtils.getLibraryImageFiles(
                        libraryPaths.library.input,
                        inputLibraryVariantChoice.map(lv -> lv.variantPath).orElse(null),
                        inputLibraryVariantChoice.map(lv -> lv.variantNameSuffix).orElse(null));
        LOG.info("Found {} input codes for {}",
                inputImages.getRight().size(),
                inputLibraryVariantChoice);

        gen.open();
        for (int pageOffset = libraryPaths.library.offset; pageOffset < to; pageOffset += DEFAULT_PAGE_LENGTH) {
            int currentPageSize = Math.min(DEFAULT_PAGE_LENGTH, to - pageOffset);
            List<ColorDepthMIP> cdmipsPage = retrieveColorDepthMipsWithSamples(
                    serverEndpoint,
                    args.authorization,
                    args.alignmentSpace,
                    libraryPaths.library,
                    args.datasets,
                    args.releases,
                    args.includedMIPs,
                    pageOffset,
                    currentPageSize);
            LOG.info("Process {} entries from {} to {} out of {}", cdmipsPage.size(), pageOffset, pageOffset + currentPageSize, cdmsCount);
            List<AbstractNeuronEntity> cdNeurons = cdmipsPage.stream()
                    .filter(cdmip -> checkLibraries(cdmip, args.includedLibraries, args.excludedLibraries))
                    .map(cdmip -> MIPsHandlingUtils.isEmLibrary(libraryPaths.getLibraryName())
                            ? asEMNeuron(cdmip, libraryNameExtractor)
                            : asLMNeuron(cdmip, libraryNameExtractor))
                    .flatMap(cdmip -> MIPsHandlingUtils.findNeuronMIPs(
                                    cdmip.getSourceMIP(),
                                    cdmip.getNeuronMetadata(),
                                    inputLibraryVariantChoice.map(lv -> lv.variantPath).orElse(null),
                                    inputImages,
                                    args.includeOriginalWithSegmentation,
                                    args.segmentedImageChannelBase)
                            .stream()
                            .map(n -> new InputCDMipNeuron<>(cdmip.getSourceMIP(), n))
                    )
                    .filter(cdmip -> CollectionUtils.isEmpty(excludedNeurons) || !CollectionUtils.containsAny(excludedNeurons, cdmip.getNeuronMetadata().getNeuronId()))
                    .peek(cdmip -> populateOtherComputeFilesFromInput(
                            cdmip,
                            EnumSet.of(ComputeFileType.GradientImage, ComputeFileType.ZGapImage),
                            libraryPaths.listLibraryVariants(),
                            inputLibraryVariantChoice.orElse(null)))
                    .peek(this::updateTag)
                    .map(InputCDMipNeuron::getNeuronMetadata)
                    .collect(Collectors.toList());
            gen.write(cdNeurons);
        }
        gen.close();
    }

    private void populateOtherComputeFilesFromInput(InputCDMipNeuron<? extends AbstractNeuronEntity> cdmip,
                                                    Set<ComputeFileType> computeFileTypes,
                                                    List<LibraryVariantArg> libraryVariants,
                                                    LibraryVariantArg computeInputVariant) {
        String computeInputVarianSuffix = computeInputVariant == null ? null : computeInputVariant.variantTypeSuffix;
        libraryVariants.stream()
                .filter(variant -> computeFileTypes.contains(ComputeFileType.fromName(args.variantFileTypeMapping.get(variant.variantType))))
                .forEach(variant -> {
                    ComputeFileType variantFileType = ComputeFileType.fromName(args.variantFileTypeMapping.get(variant.variantType));
                    FileData variantFileData = FileDataUtils.lookupVariantFileData(
                            cdmip.getNeuronMetadata().getComputeFileName(ComputeFileType.InputColorDepthImage),
                            cdmip.getNeuronMetadata().getComputeFileName(ComputeFileType.SourceColorDepthImage),
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
                    cdmip.getNeuronMetadata().setComputeFileData(variantFileType, variantFileData);
                });
    }

    private CDMIPsWriter getCDSInputWriter() {
        if (args.commonArgs.resultsStorage == StorageType.DB) {
            return new DBCDMIPsWriter(getDaosProvider().getNeuronMetadataDao());
        } else {
            return new JSONCDMIPsWriter(args.getOutputDir(),
                    args.getOutputFileName(),
                    args.library.offset,
                    args.library.length,
                    args.appendOutput,
                    mapper);
        }
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

    private InputCDMipNeuron<EMNeuronEntity> asEMNeuron(ColorDepthMIP cdmip, Function<ColorDepthMIP, String> libraryNameExtractor) {
        String libraryName = libraryNameExtractor.apply(cdmip);
        EMNeuronEntity neuronMetadata = new EMNeuronEntity();
        neuronMetadata.setMipId(cdmip.id);
        neuronMetadata.setAlignmentSpace(cdmip.alignmentSpace);
        neuronMetadata.setLibraryName(libraryName);
        neuronMetadata.setSourceRefId(cdmip.emBodyRef);
        neuronMetadata.setPublishedName(cdmip.emBodyId());
        // set source color depth image
        neuronMetadata.setComputeFileData(ComputeFileType.SourceColorDepthImage, FileData.fromString(cdmip.filepath));
        return new InputCDMipNeuron<>(cdmip, neuronMetadata);
    }

    private InputCDMipNeuron<LMNeuronEntity> asLMNeuron(ColorDepthMIP cdmip, Function<ColorDepthMIP, String> libraryNameExtractor) {
        String libraryName = libraryNameExtractor.apply(cdmip);
        LMNeuronEntity neuronMetadata = new LMNeuronEntity();
        neuronMetadata.setMipId(cdmip.id);
        neuronMetadata.setAlignmentSpace(cdmip.alignmentSpace);
        neuronMetadata.setLibraryName(libraryName);
        neuronMetadata.setSourceRefId(cdmip.sampleRef);
        neuronMetadata.setPublishedName(cdmip.lmLineName());
        neuronMetadata.setSlideCode(cdmip.lmSlideCode());
        neuronMetadata.setAnatomicalArea(cdmip.anatomicalArea);
        neuronMetadata.setGender(Gender.fromVal(cdmip.gender()));
        neuronMetadata.setObjective(cdmip.objective);
        // set source color depth image
        neuronMetadata.setComputeFileData(ComputeFileType.SourceColorDepthImage, FileData.fromString(cdmip.filepath));
        return new InputCDMipNeuron<>(cdmip, neuronMetadata);
    }

    private Set<String> getAllVariantsForColorDepthInput() {
        return args.variantFileTypeMapping.entrySet().stream()
                .filter(e -> e.getValue().equalsIgnoreCase(ComputeFileType.InputColorDepthImage.name()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    private void updateTag(InputCDMipNeuron<? extends AbstractNeuronEntity> cdmip) {
        cdmip.getNeuronMetadata().addTag(args.tag);
    }

    private int countColorDepthMips(WebTarget serverEndpoint, String credentials, String alignmentSpace, String library,
                                    List<String> datasets, List<String> releases, List<String> mips) {
        WebTarget target = serverEndpoint.path("/data/colorDepthMIPsCount")
                .queryParam("libraryName", library)
                .queryParam("alignmentSpace", alignmentSpace)
                .queryParam("dataset", datasets != null ? datasets.stream().filter(StringUtils::isNotBlank).reduce((s1, s2) -> s1 + "," + s2).orElse(null) : null)
                .queryParam("release", releases != null ? releases.stream().filter(StringUtils::isNotBlank).reduce((s1, s2) -> s1 + "," + s2).orElse(null) : null)
                .queryParam("id", mips != null ? mips.stream().filter(StringUtils::isNotBlank).reduce((s1, s2) -> s1 + "," + s2).orElse(null) : null);
        LOG.info("Count color depth mips using {}, l={}, as={}, ds={}, rs={}", target, library, alignmentSpace, datasets, releases);
        Response response = HttpHelper.createRequestWithCredentials(target.request(MediaType.TEXT_PLAIN), credentials).get();
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
                                                                  List<String> mips,
                                                                  int offset,
                                                                  int pageLength) {
        return retrieveColorDepthMips(
                serverEndpoint.path("/data/colorDepthMIPsWithSamples")
                        .queryParam("libraryName", libraryArg.input)
                        .queryParam("alignmentSpace", alignmentSpace)
                        .queryParam("dataset", datasets != null ? datasets.stream().filter(StringUtils::isNotBlank).reduce((s1, s2) -> s1 + "," + s2).orElse(null) : null)
                        .queryParam("release", releases != null ? releases.stream().filter(StringUtils::isNotBlank).reduce((s1, s2) -> s1 + "," + s2).orElse(null) : null)
                        .queryParam("id", mips != null ? mips.stream().filter(StringUtils::isNotBlank).reduce((s1, s2) -> s1 + "," + s2).orElse(null) : null)
                        .queryParam("offset", offset)
                        .queryParam("length", pageLength),
                credentials)
                ;
    }

    private List<ColorDepthMIP> retrieveColorDepthMips(WebTarget endpoint, String credentials) {
        LOG.info("Get mips from {}", endpoint);
        Response response = HttpHelper.createRequestWithCredentials(endpoint.request(MediaType.APPLICATION_JSON), credentials).get();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new IllegalStateException("Invalid response from " + endpoint.getUri() + " -> " + response);
        } else {
            return response.readEntity(new GenericType<>(new TypeReference<List<ColorDepthMIP>>() {
            }.getType()));
        }
    }

}
