package org.janelia.colormipsearch.cmd;

import java.net.URI;
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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.api.cds.NeuronMIPUtils;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
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

    static class ChannelBaseValidator implements IValueValidator<Integer> {
        @Override
        public void validate(String name, Integer value) throws ParameterException {
            if (value < 0 || value > 1) {
                throw new ParameterException("Invalid channel base - supported values are {0, 1}");
            }
        }
    }

    @Parameters(commandDescription = "Groop MIPs by published name")
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

        @Parameter(names = {"--librariesVariants"},
                description = "Libraries variants descriptors. " +
                        "A library variant contains library name, variant type, location and suffix separated by colon , e.g., " +
                        "flylight_gen1_mcfo_published:segmentation:/libDir/mcfo/segmentation:_CDM",
                converter = LibraryVariantArg.LibraryVariantArgConverter.class,
                validateValueWith = LibraryVariantArg.ListLibraryVariantArgValidator.class,
                variableArity = true)
        List<LibraryVariantArg> libraryVariants;

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
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
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
                                excludedNeurons,
                                libraryNameMapping::get,
                                getNeuronFileURLMapper(),
                                Paths.get(args.commonArgs.outputDir),
                                args.outputFileName
                        );
                    }
                });
        // FIXME
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
                    return uriPath.subpath(args.urlsRelativeTo,  uriPath.getNameCount()).toString();
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
                                                 Set<? extends AbstractNeuronMetadata> excludedNeurons,
                                                 Function<String, String> libraryNamesMapping,
                                                 Function<String, String> neuronFileURLMapping,
                                                 Path outputPath,
                                                 String outputFilename) {

    }

    private Client createHttpClient() {
        try {
            SSLContext sslContext = createSSLContext();

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
        Map<String, Object> configJSON = response.readEntity(new GenericType<>(new TypeReference<Map<String, Object>>() {}.getType()));
        Object configEntry = configJSON.get("config");
        if (!(configEntry instanceof Map)) {
            LOG.error("Config entry from {} is null or it's not a map", configJSON);
            throw new IllegalStateException("Config entry not found");
        }
        Map<String, String> cdmLibraryNamesMapping = new HashMap<>();
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> configEntryMap = (Map<String, Map<String, Object>>)configEntry;
        configEntryMap.forEach((lid, ldata) -> {
            String lname = (String) ldata.get("name");
            cdmLibraryNamesMapping.put(lid, lname);
        });
        LOG.info("Using {} for mapping library names", cdmLibraryNamesMapping);
        return cdmLibraryNamesMapping;
    }

}
