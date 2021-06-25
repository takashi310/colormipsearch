package org.janelia.colormipsearch.cmd;

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
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import org.janelia.colormipsearch.api.Utils;
import org.janelia.colormipsearch.api.pppsearch.PPPMatch;
import org.janelia.colormipsearch.api.pppsearch.PPPMatches;
import org.janelia.colormipsearch.api.pppsearch.PPPUtils;
import org.janelia.colormipsearch.api.pppsearch.RawPPPMatchesReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertPPPResultsCmd extends AbstractCmd {
    private static final Logger LOG = LoggerFactory.getLogger(ConvertPPPResultsCmd.class);

    @Parameters(commandDescription = "Convert the original PPP results into NeuronBridge compatible results")
    static class ConvertPPPResultsArgs extends AbstractCmdArgs {
        @Parameter(names = {"--jacs-url", "--data-url"},
                description = "JACS data service base URL", required = true)
        String dataServiceURL;

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
                description = "File(s) containing original PPP results. As a note these can be either ")
        private List<String> resultsFiles;

        @Parameter(names = "--neuron-matches-sub-dir", description = "The name of the sub-directory containing the results")
        private String neuronMatchesSubDirName = "lm_cable_length_20_v4_adj_by_cov_numba_agglo_aT";

        @Parameter(names = "--matches-prefix", description = "The prefix of the JSON results file containing PPP matches")
        private String jsonPPPResultsPrefix = "cov_scores_";

        @Parameter(names = "--screenshots-dir", description = "The prefix of the JSON results file containing PPP matches")
        private String screenshotsDir = "screenshots";

        @Parameter(names = {"--processing-partition-size", "-ps"}, description = "Processing partition size")
        int processingPartitionSize = 100;

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
        Stream<Path> filesToProcess;
        if (CollectionUtils.isNotEmpty(args.resultsFiles)) {
            filesToProcess = args.resultsFiles.stream().map(Paths::get);
        } else {
            Stream<Path> allDirsWithPPPResults = streamDirsWithPPPResults(args.resultsDir.getInputPath());
            Stream<Path> dirsToProcess;
            int offset = Math.max(0, args.resultsDir.offset);
            if (args.resultsDir.length > 0) {
                dirsToProcess = allDirsWithPPPResults.skip(offset).limit(args.resultsDir.length);
            } else {
                dirsToProcess = allDirsWithPPPResults.skip(offset);
            }
            filesToProcess = dirsToProcess.flatMap(d -> getPPPResultsFromDir(d).stream());
        }
        Utils.processPartitionStream(
                filesToProcess.parallel(),
                args.processingPartitionSize,
                this::processPPPFiles);
        LOG.info("Processed all files in {}s", (System.currentTimeMillis()-startTime)/1000.);
    }

    private void processPPPFiles(List<Path> listOfPPPResults) {
        long start = System.currentTimeMillis();
        Path outputPath = args.getOutputDir();
        listOfPPPResults.stream()
                .map(this::importPPPRResultsFromFile)
                .forEach(pppMatches -> PPPUtils.writePPPMatchesToJSONFile(
                        pppMatches,
                        outputPath == null ? null : outputPath.resolve(pppMatches.getNeuronName() + ".json").toFile(),
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
        try {
            return StreamSupport.stream(Files.newDirectoryStream(pppResultsDir, "*.json").spliterator(), false)
                    .filter(Files::isRegularFile)
                    .filter(p -> {
                        String fn = p.getFileName().toString();
                        return fn.startsWith(args.jsonPPPResultsPrefix) && fn.endsWith(".json");
                    })
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
    private PPPMatches importPPPRResultsFromFile(Path pppResultsFile) {
        List<PPPMatch> neuronMatches = originalPPPMatchesReader.readPPPMatches(pppResultsFile.toFile());
        Set<String> matchedLMSampleNames = neuronMatches.stream()
                .peek(this::fillInPPPMetadata)
                .map(PPPMatch::getLmSampleName)
                .collect(Collectors.toSet());
        Set<String> neuronNames = neuronMatches.stream()
                .map(PPPMatch::getNeuronName)
                .collect(Collectors.toSet());
        Map<String, CDMIPSample> lmSamples = retrieveSamples(matchedLMSampleNames);
        Map<String, EMNeuron> emNeurons = retrieveEMData(neuronNames);

        neuronMatches.forEach(pppMatch -> {
            if (pppMatch.getEmPPPRank() < 500) {
                lookupScreenshots(pppResultsFile.getParent(), pppMatch);
            }
            CDMIPSample lmSample = lmSamples.get(pppMatch.getLmSampleName());
            if (lmSample != null) {
                pppMatch.setLineName(lmSample.publishingName);
                pppMatch.setSlideCode(lmSample.slideCode);
                pppMatch.setGender(lmSample.gender);
            }
            EMNeuron emNeuron = emNeurons.get(pppMatch.getNeuronName());
            if (emNeuron != null) {
                pppMatch.setNeuronType(emNeuron.neuronType);
                pppMatch.setNeuronInstance(emNeuron.neuronInstance);
                pppMatch.setNeuronStatus(emNeuron.status);
            }
        });
        return PPPMatches.pppMatchesBySingleNeuron(neuronMatches);
    }

    private void fillInPPPMetadata(PPPMatch pppMatch) {
        fillEMMMetadata(pppMatch.getFullEmName(), pppMatch);
        fillLMMetadata(pppMatch.getFullLmName(), pppMatch);
    }

    private void fillEMMMetadata(String emFullName, PPPMatch pppMatch) {
        Pattern emRegExPattern = Pattern.compile("([0-9]+)-([^-]*)-(.*)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = emRegExPattern.matcher(emFullName);
        if (matcher.find()) {
            pppMatch.setNeuronName(matcher.group(1));
            pppMatch.setNeuronType(matcher.group(2));
        }
    }

    private void fillLMMetadata(String lmFullName, PPPMatch pppMatch) {
        Pattern lmRegExPattern = Pattern.compile("(.+)_REG_UNISEX_(.+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = lmRegExPattern.matcher(lmFullName);
        if (matcher.find()) {
            pppMatch.setLmSampleName(matcher.group(1));
            pppMatch.setObjective(matcher.group(2));
        }
    }

    private void lookupScreenshots(Path pppResultsDir, PPPMatch pppMatch) {
        try {
            Files.newDirectoryStream(pppResultsDir.resolve(args.screenshotsDir), pppMatch.getFullEmName() + "*" + pppMatch.getFullLmName() + "*.png")
                    .forEach(f -> {
                        pppMatch.addImageVariant(f.toString());
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Map<String, CDMIPSample> retrieveSamples(Set<String> sampleNames) {
        WebTarget serverEndpoint = createHttpClient().target(args.dataServiceURL);
        WebTarget samplesEndpoint = serverEndpoint.path("/data/samples")
                .queryParam("name", sampleNames != null ? sampleNames.stream().filter(StringUtils::isNotBlank).reduce((s1, s2) -> s1 + "," + s2).orElse(null) : null);
        Response response = createRequestWithCredentials(samplesEndpoint.request(MediaType.APPLICATION_JSON), args.authorization).get();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new IllegalStateException("Invalid response from " + samplesEndpoint.getUri() + " -> " + response);
        } else {
            List<CDMIPSample> samples = response.readEntity(new GenericType<>(new TypeReference<List<CDMIPSample>>() {}.getType()));
            return samples.stream()
                    .filter(s -> StringUtils.isNotBlank(s.publishingName))
                    .collect(Collectors.toMap(s -> s.name, s -> s));
        }
    }

    private Map<String, EMNeuron> retrieveEMData(Set<String> neuronIds) {
        WebTarget serverEndpoint = createHttpClient().target(args.dataServiceURL);
        WebTarget emEndpoint = serverEndpoint.path("/emdata/dataset")
                .path(args.emDataset)
                .path(args.emDatasetVersion)
                .queryParam("name", neuronIds != null ? neuronIds.stream().filter(StringUtils::isNotBlank).reduce((s1, s2) -> s1 + "," + s2).orElse(null) : null);
        if (neuronIds != null && neuronIds.size() > 1000) {
            int s = neuronIds.size();
            int ncalls = s / 1000;
            return IntStream.range(0, ncalls + (s % 1000 == 0 ? 0 : 1)).boxed()
                    .flatMap(i -> retrieveEMNeurons(
                            emEndpoint.queryParam("offset", String.valueOf(i * 1000))
                                    .queryParam("length", String.valueOf(1000))).stream())
                    .collect(Collectors.toMap(n -> n.name, n -> n));
        } else {
            return retrieveEMNeurons(emEndpoint).stream().collect(Collectors.toMap(n -> n.name, n -> n));
        }
    }

    private List<EMNeuron> retrieveEMNeurons(WebTarget endpoint) {
        Response response = createRequestWithCredentials(endpoint.request(MediaType.APPLICATION_JSON), args.authorization).get();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new IllegalStateException("Invalid response from " + endpoint.getUri() + " -> " + response);
        } else {
            return response.readEntity(new GenericType<>(new TypeReference<List<EMNeuron>>() {}.getType()));
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
