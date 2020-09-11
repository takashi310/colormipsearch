package org.janelia.colormipsearch.cmd;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import com.beust.jcommander.Parameter;
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
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.api.cdmips.AbstractMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class GroupMIPsByPublishedNameCmd extends AbstractCmd {

    private static final Logger LOG = LoggerFactory.getLogger(GroupMIPsByPublishedNameCmd.class);

    // since right now there'sonly one EM library just use its name to figure out how to handle the color depth mips metadata
    private static final String NO_CONSENSUS = "No Consensus";
    private static final int DEFAULT_PAGE_LENGTH = 10000;

    @Parameters(commandDescription = "Grooup MIPs by published name")
    private static class GroupMIPsByPublishedNameArgs extends AbstractCmdArgs {
        @Parameter(names = {"--jacs-url", "--data-url", "--jacsURL"},
                description = "JACS data service base URL", required = true)
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

        @Parameter(names = "--included-libraries", variableArity = true, description = "If set, MIPs should also be in all these libraries")
        Set<String> includedLibraries;

        @Parameter(names = "--excluded-libraries", variableArity = true, description = "If set, MIPs should not be part of any of these libraries")
        Set<String> excludedLibraries;

        @Parameter(names = {"--datasets"}, description = "Which datasets to extract", variableArity = true)
        List<String> datasets;

        @Parameter(names = {"--skeletons-directory", "-emdir"}, description = "Em skeletons sub-directory")
        String skeletonsOutput = "by_body";

        @Parameter(names = {"--lines-directory", "-lmdir"}, description = "LM lines sub-directory")
        String linesOutput = "by_line";

        @Parameter(names = "--include-mips-with-missing-urls", description = "Include MIPs that do not have a valid URL", arity = 0)
        boolean includeMIPsWithNoPublishedURL;

        @Parameter(names = "--include-mips-without-publishing-name", description = "Bitmap flag for include MIPs without publishing name: " +
                "0x0 - if either the publishing name is missing or the publishedToStaging is not set the image is not included; " +
                "0x1 - the mip is included even if publishing name is not set; " +
                "0x2 - the mip is included even if publishedToStaging is not set")
        int includeMIPsWithoutPublisingName;

        @Parameter(names = {"--default-gender"}, description = "Default gender")
        String defaultGender = "f";

        @Parameter(names = {"--keep-dups"}, description = "Keep duplicates", arity = 0)
        boolean keepDuplicates;

        @Parameter(names = {"--urls-relative-to"}, description = "URLs are relative to the specified component")
        int urlsRelativeTo = -1;

        @ParametersDelegate
        final CommonArgs commonArgs;

        GroupMIPsByPublishedNameArgs(CommonArgs commonArgs) {
            this.commonArgs = commonArgs;
        }
    }

    private final GroupMIPsByPublishedNameArgs args;
    private final ObjectMapper mapper;

    GroupMIPsByPublishedNameCmd(String commandName, CommonArgs commonArgs) {
        super(commandName);
        args = new GroupMIPsByPublishedNameArgs(commonArgs);
        this.mapper = new ObjectMapper()
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    GroupMIPsByPublishedNameArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        WebTarget serverEndpoint = createHttpClient().target(args.dataServiceURL);

        Map<String, String> libraryNameMapping = retrieveLibraryNameMapping(args.libraryMappingURL);

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

        args.libraries.forEach(library -> {
            Path outputPath;
            if (isEmLibrary(library.input)) {
                outputPath = Paths.get(args.commonArgs.outputDir, args.skeletonsOutput);
            } else {
                outputPath = Paths.get(args.commonArgs.outputDir, args.linesOutput);
            }
            groupMIPsByPublishedName(
                    serverEndpoint,
                    library,
                    libraryNameMapping,
                    imageURLMapper,
                    outputPath
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

    private void groupMIPsByPublishedName(WebTarget serverEndpoint,
                                          ListArg libraryArg,
                                          Map<String, String> libraryNamesMapping,
                                          Function<String, String> imageURLMapper,
                                          Path outputPath) {
        // get color depth mips from JACS for the specified alignmentSpace and library
        int cdmsCount = countColorDepthMips(
                serverEndpoint,
                args.authorization,
                args.alignmentSpace,
                libraryArg.input,
                args.datasets);
        LOG.info("Found {} entities in library {} with alignment space {}{}",
                cdmsCount, libraryArg.input, args.alignmentSpace, CollectionUtils.isNotEmpty(args.datasets) ? " for datasets " + args.datasets : "");
        int to = libraryArg.length > 0 ? Math.min(libraryArg.offset + libraryArg.length, cdmsCount) : cdmsCount;

        Function<ColorDepthMIP, String> libraryNameExtractor = cmip -> {
            String internalLibraryName = cmip.findLibrary(libraryArg.input);
            if (StringUtils.isBlank(internalLibraryName)) {
                return null;
            } else {
                String displayLibraryName = libraryNamesMapping.get(internalLibraryName);
                if (StringUtils.isBlank(displayLibraryName)) {
                    LOG.warn("No name mapping found {}", internalLibraryName);
                    return null;
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
        Map<String, List<String>> cdmNameToMIPIdForDupCheck = new HashMap<>();
        for (int pageOffset = libraryArg.offset; pageOffset < to; pageOffset += DEFAULT_PAGE_LENGTH) {
            int pageSize = Math.min(DEFAULT_PAGE_LENGTH, to - pageOffset);
            List<ColorDepthMIP> cdmipsPage = retrieveColorDepthMipsWithSamples(
                    serverEndpoint,
                    args.authorization,
                    args.alignmentSpace,
                    libraryArg,
                    args.datasets,
                    pageOffset,
                    pageSize);
            LOG.info("Process {} entries from {} to {} out of {}", cdmipsPage.size(), pageOffset, pageOffset + pageSize, cdmsCount);
            Map<String, List<ColorDepthMetadata>> resultsByLineOrSkeleton = cdmipsPage.stream()
                    .filter(cdmip -> args.includeMIPsWithNoPublishedURL || hasPublishedImageURL(cdmip))
                    .filter(cdmip -> checkMIPLibraries(cdmip, args.includedLibraries, args.excludedLibraries))
                    .filter(cdmip -> isEmLibrary(libraryArg.input) || (hasSample(cdmip) && hasConsensusLine(cdmip) && hasPublishedName(args.includeMIPsWithoutPublisingName, cdmip)))
                    .map(cdmip -> isEmLibrary(libraryArg.input)
                            ? asEMBodyMetadata(cdmip, args.defaultGender, libraryNameExtractor, imageURLMapper)
                            : asLMLineMetadata(cdmip, libraryNameExtractor, imageURLMapper))
                    .filter(cdmip -> StringUtils.isNotBlank(cdmip.getPublishedName()))
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
                    .collect(Collectors.groupingBy(
                            AbstractMetadata::getPublishedName,
                            Collectors.toList()));
            // write the results to the output
            resultsByLineOrSkeleton
                    .forEach((lineOrSkeletonName, results) -> writeColorDepthMetadata(outputPath.resolve(lineOrSkeletonName + ".json"), results))
            ;
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

    private ColorDepthMetadata asLMLineMetadata(ColorDepthMIP cdmip,
                                                Function<ColorDepthMIP, String> libraryNameExtractor,
                                                Function<String, String> imageURLMapper) {
        String libraryName = libraryNameExtractor.apply(cdmip);
        ColorDepthMetadata cdMetadata = new ColorDepthMetadata();
        cdMetadata.setId(cdmip.id);
        cdMetadata.setLibraryName(libraryName);
        cdMetadata.filepath = cdmip.filepath;
        cdMetadata.setImageURL(imageURLMapper.apply(cdmip.publicImageUrl));
        cdMetadata.setThumbnailURL(imageURLMapper.apply(cdmip.publicThumbnailUrl));
        cdMetadata.sourceImageRef  = cdmip.sourceImageRef;
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

    private ColorDepthMetadata asEMBodyMetadata(ColorDepthMIP cdmip,
                                                String defaultGender,
                                                Function<ColorDepthMIP, String> libraryNameExtractor,
                                                Function<String, String> imageURLMapper) {
        String libraryName = libraryNameExtractor.apply(cdmip);
        ColorDepthMetadata cdMetadata = new ColorDepthMetadata();
        cdMetadata.setId(cdmip.id);
        cdMetadata.setLibraryName(libraryName);
        cdMetadata.filepath = cdmip.filepath;
        cdMetadata.setImageURL(imageURLMapper.apply(cdmip.publicImageUrl));
        cdMetadata.setThumbnailURL(imageURLMapper.apply(cdmip.publicThumbnailUrl));
        cdMetadata.setEMSkeletonPublishedName(extractEMSkeletonIdFromName(cdmip.name));
        cdMetadata.setGender(defaultGender);
        return cdMetadata;
    }

    private String extractEMSkeletonIdFromName(String name) {
        List<String> mipNameComponents = Splitter.on('_').splitToList(name);
        return mipNameComponents.size() > 0 ? mipNameComponents.get(0) : null;
    }

    /**
     * Preconditions results.isNotEmpty
     *
     * @param outputPath
     * @param results
     */
    private void writeColorDepthMetadata(Path outputPath, List<ColorDepthMetadata> results) {
        if (Files.exists(outputPath)) {
            RandomAccessFile rf;
            OutputStream outputStream;
            try {
                LOG.debug("Append to {}", outputPath);
                rf = new RandomAccessFile(outputPath.toFile(), "rw");
                long rfLength = rf.length();
                // position FP after the end of the last item
                // this may not work on Windows because of the new line separator
                // - so on windows it may need to rollback more than 4 chars
                rf.seek(rfLength - 4);
                outputStream = Channels.newOutputStream(rf.getChannel());
            } catch (IOException e) {
                LOG.error("Error creating the output stream to be appended for {}", outputPath, e);
                return;
            }
            try {
                // FP is positioned at the end of the last element
                long endOfLastItemPos = rf.getFilePointer();
                outputStream.write(", ".getBytes()); // write the separator for the next array element
                // append the new elements to the existing results
                JsonGenerator gen = mapper.getFactory().createGenerator(outputStream, JsonEncoding.UTF8);
                gen.useDefaultPrettyPrinter();
                gen.writeStartObject(); // just to tell the generator that this is inside of an object which has an array
                gen.writeArrayFieldStart("results");
                gen.writeObject(results.get(0)); // write the first element - it can be any element or dummy object
                // just to fool the generator that there is already an element in the array
                gen.flush();
                // reset the position
                rf.seek(endOfLastItemPos);
                // and now start writing the actual elements
                writeColorDepthArrayValues(gen, results);
                gen.writeEndArray();
                gen.writeEndObject();
                gen.flush();
                long currentPos = rf.getFilePointer();
                rf.setLength(currentPos); // truncate
            } catch (IOException e) {
                LOG.error("Error writing json output for {} outputfile {}", results, outputPath, e);
            } finally {
                try {
                    outputStream.close();
                } catch (IOException ignore) {
                }
            }
        } else {
            FileOutputStream outputStream;
            try {
                outputStream = new FileOutputStream(outputPath.toFile());
            } catch (FileNotFoundException e) {
                LOG.error("Error opening the outputfile {}", outputPath, e);
                return;
            }
            try {
                JsonGenerator gen = mapper.getFactory().createGenerator(outputStream, JsonEncoding.UTF8);
                gen.useDefaultPrettyPrinter();
                gen.writeStartObject();
                gen.writeArrayFieldStart("results");
                writeColorDepthArrayValues(gen, results);
                gen.writeEndArray();
                gen.writeEndObject();
                gen.flush();
            } catch (IOException e) {
                LOG.error("Error writing json output for {} outputfile {}", results, outputPath, e);
            } finally {
                try {
                    outputStream.close();
                } catch (IOException ignore) {
                }
            }
        }
    }

    private void writeColorDepthArrayValues(JsonGenerator gen, List<ColorDepthMetadata> results) {
        for (ColorDepthMetadata item : results) {
            try {
                gen.writeObject(item);
            } catch (IOException e) {
                LOG.error("Error writing {}", item, e);
            }
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
