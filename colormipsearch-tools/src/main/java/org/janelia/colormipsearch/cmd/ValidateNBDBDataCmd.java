package org.janelia.colormipsearch.cmd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import org.apache.commons.collections4.CollectionUtils;
import org.janelia.colormipsearch.cmd.jacsdata.CachedDataHelper;
import org.janelia.colormipsearch.cmd.jacsdata.ColorDepthMIP;
import org.janelia.colormipsearch.cmd.jacsdata.JacsDataGetter;
import org.janelia.colormipsearch.cmd.jacsdata.PublishedDataGetter;
import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.mips.NeuronMIPUtils;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.FileData;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This command is used to export data from the database to the file system in order to upload it to S3.
 */
class ValidateNBDBDataCmd extends AbstractCmd {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateNBDBDataCmd.class);

    @Parameters(commandDescription = "Export neuron matches")
    static class ValidateCmdArgs extends AbstractCmdArgs {

        @Parameter(names = {"--jacs-url", "--data-url"}, description = "JACS data service base URL")
        String dataServiceURL;

        @Parameter(names = {"--config-url"}, description = "Config URL that contains the library name mapping")
        String configURL = "https://config.int.janelia.org/config";

        @Parameter(names = {"--authorization"},
                description = "JACS authorization - this is the value of the authorization header")
        String authorization;

        @Parameter(names = {"-as", "--alignment-space"}, description = "Alignment space")
        String alignmentSpace;

        @Parameter(names = {"-l", "--library"},
                description = "Library names from which mips or matches are selected for export",
                variableArity = true)
        List<String> libraries = new ArrayList<>();

        @Parameter(names = {"--validated-names"},
                description = "If set only validate the specified names",
                variableArity = true)
        List<String> validatedNames = new ArrayList<>();

        @Parameter(names = {"--excluded-libraries"},
                description = "Library names that a neuron should not be part of",
                variableArity = true)
        List<String> excludedLibraries = new ArrayList<>();

        @Parameter(names = {"--no-grad-files-check"},
                description = "If set do not check input files used for gradient scoring",
                arity = 0)
        boolean noGradScoreFilesCheck = false;

        @Parameter(names = {"--processingPartitionSize", "-ps", "--libraryPartitionSize"}, description = "Processing partition size")
        int processingPartitionSize = 5000;

        @Parameter(names = {"--read-batch-size"}, description = "JACS read chunk size")
        int readBatchSize = 1000;

        @Parameter(names = {"--offset"}, description = "Offset of the data to be validated")
        long offset = 0;

        @Parameter(names = {"--size"}, description = "Size of the data to be validated")
        int size = 0;

        @Parameter(names = {"--tags"}, description = "Tags to be exported", variableArity = true)
        List<String> tags = new ArrayList<>();

        ValidateCmdArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }
    }

    private final ValidateCmdArgs args;

    ValidateNBDBDataCmd(String commandName, CommonArgs commonArgs) {
        super(commandName);
        this.args = new ValidateCmdArgs(commonArgs);
    }

    @Override
    ValidateCmdArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        runDataValidation();
    }

    private void runDataValidation() {
        Executor validationExecutor = CmdUtils.createCmdExecutor(args.commonArgs);
        NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao = getDaosProvider().getNeuronMetadataDao();
        CachedDataHelper dataHelper = new CachedDataHelper(
                new JacsDataGetter(
                        args.dataServiceURL,
                        args.configURL,
                        args.authorization,
                        args.readBatchSize),
                new PublishedDataGetter(
                        getDaosProvider().getPublishedImageDao(),
                        getDaosProvider().getNeuronPublishedUrlsDao(),
                        Collections.emptyMap())
        );

        long startProcessingTime = System.currentTimeMillis();
        List<AbstractNeuronEntity> neuronEntities = neuronMetadataDao.findNeurons(
                new NeuronSelector()
                        .setAlignmentSpace(args.alignmentSpace)
                        .addNames(args.validatedNames)
                        .addLibraries(args.libraries),
                new PagedRequest()
                        .setFirstPageOffset(args.offset)
                        .setPageSize(args.size)).getResultList();
        List<CompletableFuture<Void>> allValidationJobs =
                ItemsHandling.partitionCollection(neuronEntities, args.processingPartitionSize).entrySet().stream().parallel()
                        .map(indexedPartition -> CompletableFuture.<Void>supplyAsync(() -> {
                            runValidationForNeuronEntities(indexedPartition.getKey(), indexedPartition.getValue(), dataHelper);
                            return null;
                        }, validationExecutor))
                        .collect(Collectors.toList());
        CompletableFuture.allOf(allValidationJobs.toArray(new CompletableFuture<?>[0])).join();
        LOG.info("Finished all exports in {}s", (System.currentTimeMillis()-startProcessingTime)/1000.);
    }

    private void runValidationForNeuronEntities(int jobId, List<AbstractNeuronEntity> neuronEntities, CachedDataHelper dataHelper) {
        long startProcessingTime = System.currentTimeMillis();
        LOG.info("Start validating {} neuron entities from partition {}", neuronEntities.size(), jobId);
        // retrieve color depth from JACS
        Set<String> mipIds = neuronEntities.stream()
                .map(AbstractNeuronEntity::getMipId)
                .collect(Collectors.toSet());
        dataHelper.cacheCDMIPs(mipIds);
        neuronEntities.forEach(ne -> {
            List<String> errors = validateNeuronEntity(ne, dataHelper);
            if (CollectionUtils.isNotEmpty(errors)) {
                LOG.error("Errors found for {} -> {}", ne, errors);
            }
        });
        LOG.info("Finished validating partition {} in {}s", jobId, (System.currentTimeMillis()-startProcessingTime)/1000.);
    }

    private List<String> validateNeuronEntity(AbstractNeuronEntity ne, CachedDataHelper dataHelper) {
        List<String> errors = new ArrayList<>();
        ColorDepthMIP colorDepthMIP = dataHelper.getColorDepthMIP(ne.getMipId());
        if (colorDepthMIP == null) {
            errors.add(String.format("No color depth mip in JACS for %s", ne));
        } else {
            // check that the library is correct
            if (!colorDepthMIP.libraries.contains(ne.getLibraryName())) {
                errors.add(String.format("Entity %s is in %s but not in the %s library",
                        ne, colorDepthMIP.libraries, ne.getLibraryName()));
            }
            if (CollectionUtils.isNotEmpty(args.excludedLibraries) &&
                CollectionUtils.containsAny(colorDepthMIP.libraries, args.excludedLibraries)) {
                errors.add(String.format("Entity %s is also in %s libraries and it should not be in %s",
                        ne, colorDepthMIP.libraries, args.excludedLibraries));
            }
        }
        // check file types
        checkComputeFile(ne, ComputeFileType.SourceColorDepthImage, errors);
        checkComputeFile(ne, ComputeFileType.InputColorDepthImage, errors);
        if (!args.noGradScoreFilesCheck) {
            checkComputeFile(ne, ComputeFileType.GradientImage, errors);
            checkComputeFile(ne, ComputeFileType.ZGapImage, errors);
        }
        return errors;
    }

    void checkComputeFile(AbstractNeuronEntity ne,
                          ComputeFileType computeFileType,
                          List<String> errors) {
        if (!ne.hasComputeFile(computeFileType)) {
            errors.add(String.format("Entity %s has no file type %s", ne, computeFileType));
        } else {
            FileData fd = ne.getComputeFileData(computeFileType);
            if (!NeuronMIPUtils.exists(fd)) {
                errors.add(String.format(
                        "Compute file type %s:%s for %s was not found",
                        computeFileType, fd.getName(), ne));
            }
        }

    }
}
