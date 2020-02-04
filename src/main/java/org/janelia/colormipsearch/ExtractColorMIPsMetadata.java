package org.janelia.colormipsearch;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Splitter;

import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class ExtractColorMIPsMetadata {

    private static final Logger LOG = LoggerFactory.getLogger(ExtractColorMIPsMetadata.class);
    // since right now there'sonly one EM library just use its name to figure out how to handle the color depth mips metadata
    private static final String EM_LIBRARY = "flyem_hemibrain";
    private static final String NO_CONSENSUS_LINE = "No Consensus";
    private static final int DEFAULT_PAGE_LENGTH = 10000;

    private static class Args {
        @Parameter(names = {"--jacsURL", "--dataServiceURL"}, description = "JACS data service base URL", required = true)
        private String dataServiceURL;

        @Parameter(names = {"--authorization"}, description = "JACS authorization - this is the value of the authorization header")
        private String authorization;

        @Parameter(names = {"--alignmentSpace", "-as"}, description = "Alignment space")
        private String alignmentSpace = "JRC2018_Unisex_20x_HR";

        @Parameter(names = {"--libraries", "-l"}, description = "Which libraries to extract", required = true, variableArity = true)
        private List<String> libraries;

        @Parameter(names = {"--output-directory", "-od"}, description = "Output directory", required = true)
        private String outputDir;

        @Parameter(names = {"--maxResults"}, description = "Maximum number of results to process")
        private int maxResults = 0;

        @Parameter(names = "-h", description = "Display the help message", help = true, arity = 0)
        private boolean displayHelpMessage = false;
    }

    private final Client httpClient;
    private final WebTarget serverEndpoint;
    private final String credentials;
    private final ObjectMapper mapper;

    private ExtractColorMIPsMetadata(String serverURL, String credentials) {
        this.httpClient = createHttpClient();
        this.serverEndpoint = httpClient.target(serverURL);
        this.credentials = credentials;
        this.mapper = new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    private void writeColorDepthMetadata(String alignmentSpace, String library, String outputDir, int maxResults) {
        // get color depth mips from JACS for the specified alignmentSpace and library
        int cdmsCount = countColorDepthMips(alignmentSpace, library);
        LOG.info("Found {} entities in library {} with alignment space {}", cdmsCount, library, alignmentSpace);
        int maxCdms = maxResults > 0 ? Math.min(cdmsCount, maxResults) : cdmsCount;

        Predicate<String> isEmLibrary = aLibraryName -> StringUtils.equalsIgnoreCase(EM_LIBRARY, aLibraryName);
        Predicate<ColorDepthMIP> isEmSkeleton = cdmip -> isEmLibrary.test(cdmip.findLibrary());
        Predicate<ColorDepthMIP> hasSample = cdmip -> cdmip.sample != null;
        Predicate<ColorDepthMIP> hasConsensusLine = cdmip -> !StringUtils.equalsIgnoreCase(NO_CONSENSUS_LINE, cdmip.sample.line);

        Path outputPath;
        if (isEmLibrary.test(library)) {
            outputPath = Paths.get(outputDir, "by_body");
        } else {
            outputPath = Paths.get(outputDir, "by_line");
        }
        try {
            Files.createDirectories(outputPath);
        } catch (IOException e) {
            LOG.error("Error creating the output directory for {}", outputPath, e);
        }

        for (int pageOffset = 0; pageOffset < maxCdms; pageOffset += DEFAULT_PAGE_LENGTH) {
            List<ColorDepthMIP> cdmipsPage = retrieveColorDepthMips(alignmentSpace, library, pageOffset, DEFAULT_PAGE_LENGTH);
            LOG.info("Process {} entries from {} out of {}", cdmipsPage.size(), pageOffset, maxCdms);
            Map<String, List<ColorDepthMetadata>> resultsByLineOrSkeleton = cdmipsPage.stream()
                    .filter(isEmSkeleton.or(hasSample.and(hasConsensusLine))) // here we may have to filter if it has published name
                    .map(cdmip -> isEmSkeleton.test(cdmip) ? asEMBodyMetadata(cdmip) : asLMLineMetadata(cdmip))
                    .collect(Collectors.groupingBy(cdmip -> cdmip.publishedLineOrSkeleton, Collectors.toList()))
                    ;
            // write the results to the output
            resultsByLineOrSkeleton
                    .forEach((lineOrSkeletonName, results) -> writeColorDepthMetadata(outputPath.resolve(lineOrSkeletonName + ".json"), results))
                    ;
            if (cdmipsPage.size() < DEFAULT_PAGE_LENGTH) {
                break;
            }
        }
    }

    private ColorDepthMetadata asLMLineMetadata(ColorDepthMIP cdmip) {
        ColorDepthMetadata cdMetadata = new ColorDepthMetadata();
        cdMetadata.id = cdmip.id;
        cdMetadata.internalName = cdmip.name;
        cdMetadata.sampleRef = cdmip.sampleRef;
        cdMetadata.libraryName = cdmip.findLibrary();
        cdMetadata.imageUrl = StringUtils.defaultIfBlank(cdmip.publicImageUrl, TestData.aRandomURL());
        cdMetadata.thumbnailUrl = cdmip.publicThumbnailUrl;
        if (cdmip.sample != null) {
            cdMetadata.publishedLineOrSkeleton = cdmip.sample.line; // This will have to change to the published line
            cdMetadata.addAttr("Line", cdmip.sample.line);
            cdMetadata.addAttr("Slide Code", cdmip.sample.slideCode);
            cdMetadata.addAttr("Published Name", cdmip.sample.publishingName);
            cdMetadata.addAttr("Gender", cdmip.sample.gender);
            cdMetadata.addAttr("Genotype", cdmip.sample.genotype);
            cdMetadata.addAttr("Mounting Protocol", cdmip.sample.mountingProtocol);
        } else {
            populateCDMetadataFromCDMIPName(cdmip, cdMetadata);
        }
        cdMetadata.addAttr("Anatomical Area", cdmip.anatomicalArea);
        cdMetadata.addAttr("Alignment Space", cdmip.alignmentSpace);
        cdMetadata.addAttr("Objective", cdmip.objective);
        cdMetadata.addAttr("Library", cdmip.findLibrary());
        cdMetadata.addAttr("Channel", cdmip.channelNumber);
        return cdMetadata;
    }

    private void populateCDMetadataFromCDMIPName(ColorDepthMIP cdmip, ColorDepthMetadata cdMetadata) {
        List<String> mipNameComponents = Splitter.on('-').splitToList(cdmip.name);
        String line = mipNameComponents.size() > 0 ? mipNameComponents.get(0) : null;
        // attempt to remove the PI initials
        int piSeparator = StringUtils.indexOf(line, '_');
        String lineID;
        if (piSeparator == -1) {
            lineID = line;
        } else {
            lineID = line.substring(piSeparator + 1);
        }
        cdMetadata.publishedLineOrSkeleton = StringUtils.defaultIfBlank(lineID, "Unknown");
        cdMetadata.addAttr("Line", line);
        String slideCode = mipNameComponents.size() > 1 ? mipNameComponents.get(1) : null;
        cdMetadata.addAttr("Slide Code", slideCode);
    }

    private ColorDepthMetadata asEMBodyMetadata(ColorDepthMIP cdmip) {
        ColorDepthMetadata cdMetadata = new ColorDepthMetadata();
        cdMetadata.id = cdmip.id;
        cdMetadata.internalName = cdmip.name;
        cdMetadata.sampleRef = cdmip.sampleRef;
        cdMetadata.libraryName = cdmip.findLibrary();
        cdMetadata.imageUrl = cdmip.publicImageUrl;
        cdMetadata.thumbnailUrl = cdmip.publicThumbnailUrl;
        cdMetadata.publishedLineOrSkeleton = extractEMSkeletonIdFromName(cdmip.name);
        cdMetadata.addAttr("Body Id", extractEMSkeletonIdFromName(cdmip.name));
        cdMetadata.addAttr("Library", cdmip.findLibrary());
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
                // and now start writing theactual elements
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

    private int countColorDepthMips(String alignmentSpace, String library) {
        WebTarget target = serverEndpoint.path("/data/colorDepthMIPsCount")
                .queryParam("libraryName", library)
                .queryParam("alignmentSpace", alignmentSpace)
                ;
        Response response = createRequestWithCredentials(target.request(MediaType.TEXT_PLAIN)).get();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new IllegalStateException("Invalid response from " + target + " -> " + response);
        } else {
            return response.readEntity(Integer.class);
        }
    }

    private List<ColorDepthMIP> retrieveColorDepthMips(String alignmentSpace, String library, int offset, int pageLength) {
        WebTarget target = serverEndpoint.path("/data/colorDepthMIPsWithSamples")
                .queryParam("libraryName", library)
                .queryParam("alignmentSpace", alignmentSpace)
                .queryParam("offset", offset)
                .queryParam("length", pageLength)
                ;
        Response response = createRequestWithCredentials(target.request(MediaType.APPLICATION_JSON)).get();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new IllegalStateException("Invalid response from " + target + " -> " + response);
        } else {
            return response.readEntity(new GenericType<>(new TypeReference<List<ColorDepthMIP>>(){}.getType()));
        }
    }

    private Invocation.Builder createRequestWithCredentials(Invocation.Builder requestBuilder) {
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
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

            return ClientBuilder.newBuilder()
                    .connectTimeout(30, TimeUnit.SECONDS)
                    .readTimeout(0, TimeUnit.SECONDS)
                    .sslContext(sslContext)
                    .hostnameVerifier((s, sslSession) -> true)
                    .register(JacksonFeature.class)
                    .register(jsonProvider)
                    .build();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static void main(String[] argv) {
        Args args = new Args();
        JCommander cmdline = JCommander.newBuilder()
                .addObject(args)
                .build();

        cmdline.parse(argv);

        if (args.displayHelpMessage) {
            cmdline.usage();
            System.exit(0);
        }

        ExtractColorMIPsMetadata cdmipMetadataExtractor = new ExtractColorMIPsMetadata(args.dataServiceURL, args.authorization);
        args.libraries.forEach(library -> {
            cdmipMetadataExtractor.writeColorDepthMetadata(args.alignmentSpace, library, args.outputDir, args.maxResults);
        });
    }

}
