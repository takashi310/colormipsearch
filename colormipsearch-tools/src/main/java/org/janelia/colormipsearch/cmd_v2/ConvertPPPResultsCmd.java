package org.janelia.colormipsearch.cmd_v2;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.api_v2.Utils;
import org.janelia.colormipsearch.api_v2.pppsearch.EmPPPMatch;
import org.janelia.colormipsearch.api_v2.pppsearch.EmPPPMatches;
import org.janelia.colormipsearch.api_v2.pppsearch.RawPPPMatchesReader;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class ConvertPPPResultsCmd extends AbstractCmd {
    private static final Logger LOG = LoggerFactory.getLogger(ConvertPPPResultsCmd.class);
    private static final Random RAND = new Random();

    @Parameters(commandDescription = "Convert the original PPP results into NeuronBridge compatible results")
    static class ConvertPPPResultsArgs extends AbstractCmdArgs {
        @Parameter(names = {"--jacs-url", "--data-url"}, variableArity = true,
                description = "JACS data service base URL")
        List<String> dataServiceURLs;

        @Parameter(names = {"--authorization"}, description = "JACS authorization - this is the value of the authorization header")
        String authorization;

        @Parameter(names = {"--em-dataset"}, description = "EM Dataset")
        String emDataset = "hemibrain";

        @Parameter(names = {"--em-dataset-version"}, description = "EM Dataset version")
        String emDatasetVersion = "1.2.1";

        @Parameter(names = {"--results-dir", "-rd"}, converter = ListArg.ListArgConverter.class,
                description = "Location of the original PPP results")
        private ListArg resultsDir;

        @Parameter(names = {"--results-file", "-rf"}, variableArity = true,
                description = "File(s) containing original PPP results.")
        private List<String> resultsFiles;

        @Parameter(names = "--neuron-matches-sub-dir", description = "The name of the sub-directory containing the results")
        private String neuronMatchesSubDirName = "lm_cable_length_20_v4_adj_by_cov_numba_agglo_aT";

        @Parameter(names = "--matches-prefix", description = "The prefix of the JSON results file containing PPP matches")
        private String jsonPPPResultsPrefix = "cov_scores_";

        @Parameter(names = "--screenshots-dir", description = "The prefix of the JSON results file containing PPP matches")
        private String screenshotsDir = "screenshots";

        @Parameter(names = {"--alignment-space", "-as"}, description = "Alignment space")
        String alignmentSpace = "JRC2018_Unisex_20x_HR";

        @Parameter(names = {"--anatomical-area", "-area"}, description = "Anatomical area")
        String anatomicalArea = "Brain";

        @Parameter(names = {"--only-best-skeleton-matches"}, description = "Include only best skeleton matches", arity = 0)
        boolean onlyBestSkeletonMatches = false;

        @Parameter(names = {"--jacs-read-batch-size"}, description = "Batch size for getting data from JACS")
        int jacsReadBatchSize = 5000;

        @Parameter(names = {"--processing-partition-size", "-ps"}, description = "Processing partition size")
        int processingPartitionSize = 500;

        @ParametersDelegate
        final CommonArgs commonArgs;

        ConvertPPPResultsArgs(CommonArgs commonArgs) {
            this.commonArgs = commonArgs;
        }

        Path getOutputDir() {
            if (StringUtils.isNotBlank(commonArgs.outputDir)) {
                return Paths.get(commonArgs.outputDir);
            } else {
                return null;
            }
        }

        @Override
        List<String> validate() {
            List<String> errors = new ArrayList<>();
            boolean inputFound = resultsDir != null || CollectionUtils.isNotEmpty(resultsFiles);
            if (!inputFound) {
                errors.add("No result file(s) or directory containing original PPP results has been specified");
            }
            if (processingPartitionSize <= 0) {
                errors.add("Processing partition size must be greater than 0");
            }
            return errors;
        }
    }

    private final ObjectMapper mapper;
    private final ConvertPPPResultsArgs args;
    private final RawPPPMatchesReader originalPPPMatchesReader;

    ConvertPPPResultsCmd(String commandName, CommonArgs commonArgs) {
        super(commandName);
        this.args = new ConvertPPPResultsArgs(commonArgs);
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.originalPPPMatchesReader = new RawPPPMatchesReader();
    }

    @Override
    AbstractCmdArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        CmdUtils.createOutputDirs(args.getOutputDir());
        convertPPPResults(args);
    }

    private void convertPPPResults(ConvertPPPResultsArgs args) {
        long startTime = System.currentTimeMillis();
        List<Path> filesToProcess;
        if (CollectionUtils.isNotEmpty(args.resultsFiles)) {
            filesToProcess = args.resultsFiles.stream().map(Paths::get).collect(Collectors.toList());
        } else {
            Stream<Path> allDirsWithPPPResults = streamDirsWithPPPResults(args.resultsDir.getInputPath());
            Stream<Path> dirsToProcess;
            int offset = Math.max(0, args.resultsDir.offset);
            if (args.resultsDir.length > 0) {
                dirsToProcess = allDirsWithPPPResults.skip(offset).limit(args.resultsDir.length);
            } else {
                dirsToProcess = allDirsWithPPPResults.skip(offset);
            }
            filesToProcess = dirsToProcess.flatMap(d -> getPPPResultsFromDir(d).stream()).collect(Collectors.toList());
        }
        ItemsHandling.partitionCollection(filesToProcess, args.processingPartitionSize)
                        .entrySet().stream().parallel()
                        .forEach(indexedPartition -> this.processPPPFiles(indexedPartition.getValue()));
        LOG.info("Processed all files in {}s", (System.currentTimeMillis()-startTime)/1000.);
    }

    private void processPPPFiles(List<Path> listOfPPPResults) {
        long start = System.currentTimeMillis();
        Path outputDir = args.getOutputDir();
        listOfPPPResults.stream()
                .map(this::importPPPRResultsFromFile)
                .map(EmPPPMatches::pppMatchesBySingleNeuron)
                .forEach(pppMatches -> Utils.writeResultsToJSONFile(
                        pppMatches,
                        CmdUtils.getOutputFile(outputDir, new File(pppMatches.getNeuronName() + ".json")),
                        args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter()));
        LOG.info("Processed {} PPP results in {}s", listOfPPPResults.size(), (System.currentTimeMillis()-start)/1000.);
    }

    private Stream<Path> streamDirsWithPPPResults(Path startPath) {
        try {
            Stream.Builder<Path> builder = Stream.builder();
            Files.walkFileTree(startPath, new FileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    String name = dir.getFileName().toString();
                    if (name.equals(args.neuronMatchesSubDirName)) {
                        builder.add(dir);
                        return FileVisitResult.SKIP_SUBTREE;
                    } else if (name.startsWith("nblastScores") || name.equals("screenshots")) {
                        return FileVisitResult.SKIP_SUBTREE;
                    } else {
                        return FileVisitResult.CONTINUE;
                    }
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    return FileVisitResult.TERMINATE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    return FileVisitResult.CONTINUE;
                }
            });
            return builder.build();
        } catch (IOException e) {
            LOG.error("Error traversing {}", startPath, e);
            return Stream.empty();
        }
    }

    private List<Path> getPPPResultsFromDir(Path pppResultsDir) {
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(pppResultsDir, args.jsonPPPResultsPrefix + "*.json")) {
            return StreamSupport.stream(directoryStream.spliterator(), false)
                    .filter(Files::isRegularFile)
                    .filter(p -> {
                        String fn = p.getFileName().toString();
                        // filter out files <prefix><neuron>_01.json or <prefix><neuron>_02.json
                        String neuronName = fn
                                .replaceAll("(_\\d+)?\\.json$", "")
                                .replaceAll(args.jsonPPPResultsPrefix, "");
                        return fn.equals(args.jsonPPPResultsPrefix + neuronName + ".json");
                    })
                    .collect(Collectors.toList())
                    ;
        } catch (IOException e) {
            LOG.error("Error getting PPP JSON result file names from {}", pppResultsDir, e);
            return Collections.emptyList();
        }
    }

    /**
     * Import PPP results from a list of file matches that are all for the same neuron.
     *
     * @param pppResultsFile
     * @return
     */
    private List<EmPPPMatch> importPPPRResultsFromFile(Path pppResultsFile) {
        List<EmPPPMatch> neuronMatches = args.onlyBestSkeletonMatches
                ? originalPPPMatchesReader.readPPPMatchesWithBestSkeletonMatches(pppResultsFile.toString())
                : originalPPPMatchesReader.readPPPMatchesWithAllSkeletonMatches(pppResultsFile.toString());
        Set<String> matchedLMSampleNames = neuronMatches.stream()
                .peek(this::fillInPPPMetadata)
                .map(EmPPPMatch::getSampleName)
                .collect(Collectors.toSet());
        Set<String> neuronNames = neuronMatches.stream()
                .map(EmPPPMatch::getNeuronName)
                .collect(Collectors.toSet());

        Map<String, CDMIPSample> lmSamples = retrieveLMSamples(matchedLMSampleNames);
        Map<String, EMNeuron> emNeurons = retrieveEMNeurons(neuronNames);

        neuronMatches.forEach(pppMatch -> {
            if (pppMatch.getEmPPPRank() < 500) {
                Path screenshotsPath = pppResultsFile.getParent().resolve(args.screenshotsDir);
                lookupScreenshots(screenshotsPath, pppMatch);
            }
            CDMIPSample lmSample = lmSamples.get(pppMatch.getSampleName());
            if (lmSample != null) {
                pppMatch.setSourceLmDataset(lmSample.releaseLabel); // for now set this to the releaseLabel but this is not quite right
                pppMatch.setSampleId(lmSample.id);
                pppMatch.setLineName(lmSample.publishingName);
                pppMatch.setSlideCode(lmSample.slideCode);
                pppMatch.setGender(lmSample.gender);
                pppMatch.setMountingProtocol(lmSample.mountingProtocol);
                if (StringUtils.isBlank(pppMatch.getObjective())) {
                    if (CollectionUtils.size(lmSample.publishedObjectives) == 1) {
                        pppMatch.setObjective(lmSample.publishedObjectives.get(0));
                    } else {
                        throw new IllegalArgumentException("Too many published objectives for sample " + lmSample +
                                ". Cannot decide which objective to select for " + pppMatch);
                    }
                }
            }
            EMNeuron emNeuron = emNeurons.get(pppMatch.getNeuronName());
            if (emNeuron != null) {
                pppMatch.setNeuronId(emNeuron.id);
                pppMatch.setSourceEmDataset(emNeuron.datasetIdentifier); // this should be set to the library id which differs slightly from the EM dataset
                pppMatch.setNeuronType(emNeuron.neuronType);
                pppMatch.setNeuronInstance(emNeuron.neuronInstance);
                pppMatch.setNeuronStatus(emNeuron.status);
            }
        });
        return neuronMatches;
    }

    private void fillInPPPMetadata(EmPPPMatch pppMatch) {
        pppMatch.setAlignmentSpace(args.alignmentSpace);
        pppMatch.setAnatomicalArea(args.anatomicalArea);
        fillEMMMetadata(pppMatch.getSourceEmName(), pppMatch);
        fillLMMetadata(pppMatch.getSourceLmName(), pppMatch);
    }

    private void fillEMMMetadata(String emFullName, EmPPPMatch pppMatch) {
        Pattern emRegExPattern = Pattern.compile("([0-9]+)-([^-]*)-(.*)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = emRegExPattern.matcher(emFullName);
        if (matcher.find()) {
            pppMatch.setNeuronName(matcher.group(1));
            pppMatch.setNeuronType(matcher.group(2));
        }
    }

    private void fillLMMetadata(String lmFullName, EmPPPMatch pppMatch) {
        Pattern lmRegExPattern = Pattern.compile("(.+)_REG_UNISEX_(.+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = lmRegExPattern.matcher(lmFullName);
        if (matcher.find()) {
            pppMatch.setSampleName(matcher.group(1));
            String objectiveCandidate = matcher.group(2);
            if (!StringUtils.equalsIgnoreCase(args.anatomicalArea, objectiveCandidate)) {
                pppMatch.setObjective(objectiveCandidate);
            }
        }
    }

    private void lookupScreenshots(Path pppScreenshotsDir, EmPPPMatch pppMatch) {
        if (Files.exists(pppScreenshotsDir)) {
            try(DirectoryStream<Path> screenshotsDirStream = Files.newDirectoryStream(pppScreenshotsDir, pppMatch.getSourceEmName() + "*" + pppMatch.getSourceLmName() + "*.png")) {
                screenshotsDirStream.forEach(f -> {
                    pppMatch.addSourceImageFile(f.toString());
                });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private Map<String, CDMIPSample> retrieveLMSamples(Set<String> sampleNames) {
        if (CollectionUtils.isNotEmpty(args.dataServiceURLs) && CollectionUtils.isNotEmpty(sampleNames)) {
            LOG.debug("Read LM metadata for {} samples", sampleNames.size());
            Client httpClient = createHttpClient();
            return retrieveDataStream(() -> httpClient.target(getDataServiceURL())
                            .path("/data/samples")
                            .queryParam("withReducedFields", true),
                    args.jacsReadBatchSize,
                    sampleNames,
                    new TypeReference<List<CDMIPSample>>() {})
                    .filter(sample -> StringUtils.isNotBlank(sample.publishingName))
                    .collect(Collectors.toMap(n -> n.name, n -> n));
        } else {
            return Collections.emptyMap();
        }
    }

    private Map<String, EMNeuron> retrieveEMNeurons(Set<String> neuronIds) {
        if (CollectionUtils.isNotEmpty(args.dataServiceURLs) && CollectionUtils.isNotEmpty(neuronIds)) {
            LOG.debug("Read EM metadata for {} neurons", neuronIds.size());
            Client httpClient = createHttpClient();
            return retrieveDataStream(() -> httpClient.target(getDataServiceURL())
                            .path("/emdata/dataset")
                            .path(args.emDataset)
                            .path(args.emDatasetVersion),
                    args.jacsReadBatchSize,
                    neuronIds,
                    new TypeReference<List<EMNeuron>>() {})
                    .collect(Collectors.toMap(n -> n.name, n -> n));
        } else {
            return Collections.emptyMap();
        }
    }

    private String getDataServiceURL() {
        return args.dataServiceURLs.get(RAND.nextInt(args.dataServiceURLs.size()));
    }

    /**
     * @param endpointSupplier Data endpoint supplier
     * @param chunkSize
     * @param names is a non empty set of item names to be retrieved
     * @param t data type reference
     * @param <T> data type
     * @return
     */
    private <T> Stream<T> retrieveDataStream(Supplier<WebTarget> endpointSupplier, int chunkSize, Set<String> names, TypeReference<List<T>> t) {
        if (chunkSize > 0) {
            return ItemsHandling.partitionCollection(names, chunkSize).entrySet().stream().parallel()
                    .flatMap(indexedNamesSubset -> {
                        LOG.info("Retrieve {} items", indexedNamesSubset.getValue().size());
                        return retrieveChunk(
                                endpointSupplier.get().queryParam("name", indexedNamesSubset.getValue().stream().reduce((s1, s2) -> s1 + "," + s2).orElse(null)),
                                t).stream();
                    });
        } else {
            return retrieveChunk(endpointSupplier.get().queryParam("name", CollectionUtils.isNotEmpty(names) ? names.stream().reduce((s1, s2) -> s1 + "," + s2).orElse(null) : null),
                    t).stream();
        }
    }

    private <T> List<T> retrieveChunk(WebTarget endpoint, TypeReference<List<T>> t) {
        try (Response response = createRequestWithCredentials(endpoint.request(MediaType.APPLICATION_JSON), args.authorization).get()) {
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException("Invalid response from " + endpoint.getUri() + " -> " + response);
            } else {
                return response.readEntity(new GenericType<>(t.getType()));
            }
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

    private Invocation.Builder createRequestWithCredentials(Invocation.Builder requestBuilder, String credentials) {
        if (StringUtils.isNotBlank(credentials)) {
            return requestBuilder.header("Authorization", credentials);
        } else {
            return requestBuilder;
        }
    }

}
