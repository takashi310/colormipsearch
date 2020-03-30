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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Splitter;

import org.apache.commons.collections4.CollectionUtils;
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
    private static final int MAX_SEGMENTED_DATA_DEPTH = 5;

    // since right now there'sonly one EM library just use its name to figure out how to handle the color depth mips metadata
    private static final String EM_LIBRARY = "flyem_hemibrain";
    private static final String NO_CONSENSUS = "No Consensus";
    private static final int DEFAULT_PAGE_LENGTH = 10000;

    private static class MainArgs {
        @Parameter(names = "-h", description = "Display the help message", help = true, arity = 0)
        private boolean displayHelpMessage = false;
    }

    private static class Args {
        @Parameter(names = {"--jacsURL", "--dataServiceURL"}, description = "JACS data service base URL", required = true)
        private String dataServiceURL;

        @Parameter(names = {"--authorization"}, description = "JACS authorization - this is the value of the authorization header")
        private String authorization;

        @Parameter(names = {"--alignmentSpace", "-as"}, description = "Alignment space")
        private String alignmentSpace = "JRC2018_Unisex_20x_HR";

        @Parameter(names = {"--libraries", "-l"},
                description = "Which libraries to extract such as {flyem_hemibrain, flylight_gen1_gal4, flylight_gen1_lexa, flylight_gen1_mcfo_case_1, flylight_splitgal4_drivers}. " +
                        "The format is <libraryName>[:<offset>[:<length>]]",
                required = true, variableArity = true, converter = ListArg.ListArgConverter.class)
        private List<ListArg> libraries;

        @Parameter(names = {"--datasets"}, description = "Which datasets to extract", variableArity = true)
        private List<String> datasets;

        @Parameter(names = {"--segmented-mips-base-dir"}, description = "The base directory for segmented MIPS")
        private String segmentedMIPsBaseDir;

        @Parameter(names = {"--output-directory", "-od"}, description = "Output directory", required = true)
        private String outputDir;

        @Parameter(names = "-h", description = "Display the help message", help = true, arity = 0)
        private boolean displayHelpMessage = false;
    }

    @Parameters(commandDescription = "Group color depth mips")
    private static class GroupCDMIPArgs {
        @ParametersDelegate
        private final Args argsDelegate;

        GroupCDMIPArgs(Args argsDelegate) {
            this.argsDelegate = argsDelegate;
        }
    }

    @Parameters(commandDescription = "Prepare the input arguments for color depth search")
    private static class PrepareColorDepthSearchArgs {
        @ParametersDelegate
        private final Args argsDelegate;

        PrepareColorDepthSearchArgs(Args argsDelegate) {
            this.argsDelegate = argsDelegate;
        }
    }

    private final WebTarget serverEndpoint;
    private final String credentials;
    private final ObjectMapper mapper;

    private ExtractColorMIPsMetadata(String serverURL, String credentials) {
        this.serverEndpoint = createHttpClient().target(serverURL);
        this.credentials = credentials;
        this.mapper = new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    private void writeColorDepthMetadata(String alignmentSpace, ListArg libraryArg, List<String> datasets, String outputDir) {
        // get color depth mips from JACS for the specified alignmentSpace and library
        int cdmsCount = countColorDepthMips(alignmentSpace, libraryArg.input, datasets);
        LOG.info("Found {} entities in library {} with alignment space {}{}", cdmsCount, libraryArg.input, alignmentSpace, CollectionUtils.isNotEmpty(datasets) ? " for datasets " + datasets : "");
        int to = libraryArg.length > 0 ? Math.min(libraryArg.offset + libraryArg.length, cdmsCount) : cdmsCount;

        Path outputPath;
        if (isEmLibrary(libraryArg.input)) {
            outputPath = Paths.get(outputDir, "by_body");
        } else {
            outputPath = Paths.get(outputDir, "by_line");
        }
        try {
            Files.createDirectories(outputPath);
        } catch (IOException e) {
            LOG.error("Error creating the output directory for {}", outputPath, e);
        }

        for (int pageOffset = libraryArg.offset; pageOffset < to; pageOffset += DEFAULT_PAGE_LENGTH) {
            int pageSize = Math.min(DEFAULT_PAGE_LENGTH, to - pageOffset);
            List<ColorDepthMIP> cdmipsPage = retrieveColorDepthMipsWithSamples(alignmentSpace, libraryArg, datasets, pageOffset, pageSize);
            LOG.info("Process {} entries from {} to {} out of {}", cdmipsPage.size(), pageOffset, pageOffset + pageSize, cdmsCount);
            Map<String, List<ColorDepthMetadata>> resultsByLineOrSkeleton = cdmipsPage.stream()
                    .filter(cdmip -> isEmSkeleton(cdmip) || (hasSample(cdmip) && hasConsensusLine(cdmip) && hasPublishedName(cdmip)))
                    .map(cdmip -> isEmSkeleton(cdmip) ? asEMBodyMetadata(cdmip) : asLMLineMetadata(cdmip))
                    .filter(cdmip -> StringUtils.isNotBlank(cdmip.publishedName))
                    .collect(Collectors.groupingBy(cdmip -> cdmip.publishedName, Collectors.toList()));
            // write the results to the output
            resultsByLineOrSkeleton
                    .forEach((lineOrSkeletonName, results) -> writeColorDepthMetadata(outputPath.resolve(lineOrSkeletonName + ".json"), results))
            ;
        }
    }

    private ColorDepthMetadata asLMLineMetadata(ColorDepthMIP cdmip) {
        ColorDepthMetadata cdMetadata = new ColorDepthMetadata();
        cdMetadata.id = cdmip.id;
        cdMetadata.internalName = cdmip.name;
        cdMetadata.sampleRef = cdmip.sampleRef;
        cdMetadata.libraryName = cdmip.findLibrary();
        cdMetadata.filepath = cdmip.filepath;
        cdMetadata.imageUrl = cdmip.publicImageUrl;
        cdMetadata.thumbnailUrl = cdmip.publicThumbnailUrl;
        if (cdmip.sample != null) {
            cdMetadata.publishedName = cdmip.sample.publishingName;
            cdMetadata.line = cdmip.sample.line;
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

    private int getChannel(ColorDepthMetadata cdMetadata) {
        String channel = cdMetadata.getAttr("Channel");
        if (StringUtils.isNotBlank(channel)) {
            return Integer.parseInt(channel) - 1; // mip channels are 1 based so make it 0 based
        } else {
            return -1;
        }
    }

    private int extractChannelFromSegmentedImageName(String imageName) {
        Pattern regExPattern = Pattern.compile("_c(\\d+)_", Pattern.CASE_INSENSITIVE);
        Matcher chMatcher = regExPattern.matcher(imageName);
        if (chMatcher.find()) {
            String channel = chMatcher.group(1);
            return Integer.parseInt(channel);
        } else {
            return -1;
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

    private boolean isEmLibrary(String lname) {
        return StringUtils.equalsIgnoreCase(EM_LIBRARY, lname);
    }

    private boolean isEmSkeleton(ColorDepthMIP cdmip) {
        return isEmLibrary(cdmip.findLibrary());
    }

    private boolean hasSample(ColorDepthMIP cdmip) {
        return cdmip.sample != null;
    }

    private boolean hasConsensusLine(ColorDepthMIP cdmip) {
        return !StringUtils.equalsIgnoreCase(NO_CONSENSUS, cdmip.sample.line);
    }

    private boolean hasPublishedName(ColorDepthMIP cdmip) {
        String publishingName = cdmip.sample.publishingName;
        return StringUtils.isNotBlank(publishingName) && !StringUtils.equalsIgnoreCase(NO_CONSENSUS, publishingName);
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
        cdMetadata.publishedName = StringUtils.defaultIfBlank(lineID, "Unknown");
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
        cdMetadata.filepath = cdmip.filepath;
        cdMetadata.imageUrl = cdmip.publicImageUrl;
        cdMetadata.thumbnailUrl = cdmip.publicThumbnailUrl;
        cdMetadata.publishedName = extractEMSkeletonIdFromName(cdmip.name);
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

    private void prepareColorDepthSearchArgs(String alignmentSpace, ListArg libraryArg, List<String> datasets, String segmentedMIPsBaseDir, String outputDir) {
        int cdmsCount = countColorDepthMips(alignmentSpace, libraryArg.input, datasets);
        LOG.info("Found {} entities in library {} with alignment space {}{}", cdmsCount, libraryArg.input, alignmentSpace, CollectionUtils.isNotEmpty(datasets) ? " for datasets " + datasets : "");
        int to = libraryArg.length > 0 ? Math.min(libraryArg.offset + libraryArg.length, cdmsCount) : cdmsCount;

        Path outputPath = Paths.get(outputDir);
        try {
            Files.createDirectories(outputPath);
        } catch (IOException e) {
            LOG.error("Error creating the output directory for {}", outputPath, e);
        }

        FileOutputStream outputStream;
        try {
            String outputName;
            if (libraryArg.offset > 0 || libraryArg.length > 0) {
                outputName = libraryArg.input + "-" + libraryArg.offset + "-" + to + ".json";
            } else {
                outputName = libraryArg.input + ".json";
            }
            outputStream = new FileOutputStream(outputPath.resolve(outputName).toFile());
        } catch (FileNotFoundException e) {
            LOG.error("Error opening the outputfile {}", outputPath, e);
            return;
        }
        try {
            JsonGenerator gen = mapper.getFactory().createGenerator(outputStream, JsonEncoding.UTF8);
            gen.useDefaultPrettyPrinter();
            gen.writeStartArray();
            for (int pageOffset = libraryArg.offset; pageOffset < to; pageOffset += DEFAULT_PAGE_LENGTH) {
                int pageSize = Math.min(DEFAULT_PAGE_LENGTH, to - pageOffset);
                List<ColorDepthMIP> cdmipsPage = retrieveColorDepthMipsWithSamples(alignmentSpace, libraryArg, datasets, pageOffset, pageSize);
                LOG.info("Process {} entries from {} to {} out of {}", cdmipsPage.size(), pageOffset, pageOffset + pageSize, cdmsCount);
                cdmipsPage.stream()
                        .peek(cdmip -> {
                            if (Files.notExists(Paths.get(cdmip.filepath))) {
                                LOG.warn("No filepath {} found for {} (sample: {}, publishedName: {})", cdmip.filepath, cdmip, cdmip.sampleRef, !hasSample(cdmip) ? "no sample": cdmip.sample.publishingName);
                            }
                        })
                        .filter(cdmip -> isEmSkeleton(cdmip) || (hasSample(cdmip) && hasConsensusLine(cdmip) && hasPublishedName(cdmip)))
                        .map(cdmip -> isEmSkeleton(cdmip) ? asEMBodyMetadata(cdmip) : asLMLineMetadata(cdmip))
                        .flatMap(cdmip -> findSegmentedMIPs(cdmip, segmentedMIPsBaseDir).stream())
                        .forEach(cdmip -> {
                            try {
                                Path imageFilepath = Paths.get(cdmip.segmentFilepath != null ? cdmip.segmentFilepath : cdmip.filepath);
                                gen.writeStartObject();
                                gen.writeStringField("id", cdmip.id);
                                gen.writeStringField("libraryName", cdmip.libraryName);
                                gen.writeStringField("publishedName", cdmip.publishedName);
                                gen.writeStringField("imagePath", imageFilepath.toString());
                                gen.writeStringField("cdmPath", cdmip.filepath);
                                gen.writeStringField("imageURL", cdmip.imageUrl);
                                gen.writeStringField("thumbnailURL", cdmip.thumbnailUrl);
                                gen.writeEndObject();
                            } catch (IOException e) {
                                LOG.error("Error writing entry for {}", cdmip, e);
                            }
                        });
            }
            gen.writeEndArray();
            gen.flush();
        } catch (IOException e) {
            LOG.error("Error writing json args for library {}", libraryArg, e);
        } finally {
            try {
                outputStream.close();
            } catch (IOException ignore) {
            }
        }
    }

    private List<ColorDepthMetadata> findSegmentedMIPs(ColorDepthMetadata cdmipMetadata, String segmentedMIPsBaseDir) {
        if (StringUtils.isBlank(segmentedMIPsBaseDir)) {
            return Collections.singletonList(cdmipMetadata);
        } else {
            try {
                List<ColorDepthMetadata> segmentedCDMIPs = Files.find(Paths.get(segmentedMIPsBaseDir), MAX_SEGMENTED_DATA_DEPTH,
                        (p, fa) -> {
                            if (fa.isRegularFile()) {
                                String fn = p.getFileName().toString();
                                String line = cdmipMetadata.line;
                                String publishingName = cdmipMetadata.getAttr("Published Name");
                                String slideCode = cdmipMetadata.getAttr("Slide Code");
                                if ((StringUtils.contains(fn, publishingName) || StringUtils.contains(fn, line)) && StringUtils.contains(fn, slideCode)) {
                                    int channelFromMip = getChannel(cdmipMetadata);
                                    int channelFromFN = extractChannelFromSegmentedImageName(fn);
                                    LOG.debug("Compare channel from {} ({}) with channel from {} ({})", cdmipMetadata.filepath, channelFromMip, fn, channelFromFN);
                                    return matchMIPChannelWithSegmentedImageChannel(channelFromMip, channelFromFN);
                                } else {
                                    return false;
                                }
                            } else {
                                return true;
                            }
                        })
                        .filter(p -> Files.isRegularFile(p))
                        .map(p -> {
                            ColorDepthMetadata segmentMIPMetadata = new ColorDepthMetadata();
                            cdmipMetadata.copyTo(segmentMIPMetadata);
                            segmentMIPMetadata.segmentedDataBasePath = segmentedMIPsBaseDir;
                            segmentMIPMetadata.segmentFilepath = p.toString();
                            return segmentMIPMetadata;
                        }).collect(Collectors.toList());
                return segmentedCDMIPs.isEmpty() ? Collections.singletonList(cdmipMetadata) : segmentedCDMIPs;
            } catch (IOException e) {
                LOG.warn("Error finding the segmented mips for {}", cdmipMetadata, e);
                return Collections.singletonList(cdmipMetadata);
            }
        }
    }

    private int countColorDepthMips(String alignmentSpace, String library, List<String> datasets) {
        WebTarget target = serverEndpoint.path("/data/colorDepthMIPsCount")
                .queryParam("libraryName", library)
                .queryParam("alignmentSpace", alignmentSpace)
                .queryParam("dataset", datasets != null ? datasets.stream().filter(StringUtils::isNotBlank).reduce((s1, s2) -> s1 + "," + s2).orElse(null) : null);
        Response response = createRequestWithCredentials(target.request(MediaType.TEXT_PLAIN)).get();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new IllegalStateException("Invalid response from " + target + " -> " + response);
        } else {
            return response.readEntity(Integer.class);
        }
    }

    private List<ColorDepthMIP> retrieveColorDepthMipsWithSamples(String alignmentSpace, ListArg libraryArg, List<String> datasets, int offset, int pageLength) {
        return retrieveColorDepthMips(serverEndpoint.path("/data/colorDepthMIPsWithSamples")
                .queryParam("libraryName", libraryArg.input)
                .queryParam("alignmentSpace", alignmentSpace)
                .queryParam("dataset", datasets != null ? datasets.stream().filter(StringUtils::isNotBlank).reduce((s1, s2) -> s1 + "," + s2).orElse(null) : null)
                .queryParam("offset", offset)
                .queryParam("length", pageLength))
                ;
    }

    private List<ColorDepthMIP> retrieveColorDepthMipsWithoutSamples(String alignmentSpace, ListArg libraryArg, List<String> datasets, int offset, int pageLength) {
        return retrieveColorDepthMips(serverEndpoint.path("/data/colorDepthMIPs")
                .queryParam("libraryName", libraryArg.input)
                .queryParam("alignmentSpace", alignmentSpace)
                .queryParam("dataset", datasets != null ? datasets.stream().filter(StringUtils::isNotBlank).reduce((s1, s2) -> s1 + "," + s2).orElse(null) : null)
                .queryParam("offset", offset)
                .queryParam("length", pageLength))
                ;
    }

    private List<ColorDepthMIP> retrieveColorDepthMips(WebTarget endpoint) {
        Response response = createRequestWithCredentials(endpoint.request(MediaType.APPLICATION_JSON)).get();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new IllegalStateException("Invalid response from " + endpoint.getUri() + " -> " + response);
        } else {
            return response.readEntity(new GenericType<>(new TypeReference<List<ColorDepthMIP>>() {
            }.getType()));
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
        MainArgs mainArgs = new MainArgs();
        Args args = new Args();
        JCommander cmdline = JCommander.newBuilder()
                .addObject(mainArgs)
                .addCommand("groupMIPS", new GroupCDMIPArgs(args))
                .addCommand("prepareCDSArgs", new PrepareColorDepthSearchArgs(args))
                .build();

        try {
            cmdline.parse(argv);
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder(e.getMessage()).append('\n');
            cmdline.usage(sb);
            JCommander.getConsole().println(sb.toString());
            System.exit(1);
        }

        if (mainArgs.displayHelpMessage || args.displayHelpMessage || StringUtils.isBlank(cmdline.getParsedCommand())) {
            cmdline.usage();
            System.exit(0);
        }

        ExtractColorMIPsMetadata cdmipMetadataExtractor = new ExtractColorMIPsMetadata(args.dataServiceURL, args.authorization);
        args.libraries.forEach(library -> {
            switch (cmdline.getParsedCommand()) {
                case "groupMIPS":
                    cdmipMetadataExtractor.writeColorDepthMetadata(args.alignmentSpace, library, args.datasets, args.outputDir);
                    break;
                case "prepareCDSArgs":
                    cdmipMetadataExtractor.prepareColorDepthSearchArgs(args.alignmentSpace, library, args.datasets, args.segmentedMIPsBaseDir, args.outputDir);
                    break;
            }
        });
    }

}
