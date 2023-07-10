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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.cmd.jacsdata.CachedDataHelper;
import org.janelia.colormipsearch.cmd.jacsdata.ColorDepthMIP;
import org.janelia.colormipsearch.cmd.jacsdata.JacsDataGetter;
import org.janelia.colormipsearch.cmd.jacsdata.PublishedDataGetter;
import org.janelia.colormipsearch.dao.AppendFieldValueHandler;
import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.dao.NeuronsMatchFilter;
import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.mips.NeuronMIPUtils;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.CDMatchEntity;
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

        @Parameter(names = {"--error-tag"}, description = "Error tag")
        String errorTag;

        @Parameter(names = {"--apply-error-tag-to-em-cdmatches"},
                description = "This is meaningful only if error-tag is set to apply the error tag to the corresponding EM - LM matches as well",
                arity = 0)
        boolean applyTagToEMMatches;

        @Parameter(names = {"--apply-error-tag-to-lm-cdmatches"},
                description = "This is meaningful only if error-tag is set to apply the error tag to the corresponding LM - EM matches as well",
                arity = 0)
        boolean applyTagToLMMatches;

        ValidateCmdArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }
    }

    private static class ErrorReport {
        long nEntities;
        final long nEntitiesWithErrors;
        final long nErrors;
        final List<Number> badEntities;

        ErrorReport(long nEntities, long nEntitiesWithErrors, long nErrors, List<Number> badEntities) {
            this.nEntities = nEntities;
            this.nEntitiesWithErrors = nEntitiesWithErrors;
            this.nErrors = nErrors;
            this.badEntities = badEntities;
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
        NeuronMatchesDao<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> neuronMatchesDao = getDaosProvider().getCDMatchesDao();
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
        List<CompletableFuture<ErrorReport>> allValidationJobs =
                ItemsHandling.partitionCollection(neuronEntities, args.processingPartitionSize).entrySet().stream().parallel()
                        .map(indexedPartition -> CompletableFuture
                                .supplyAsync(() -> runValidationForNeuronEntities(indexedPartition.getKey(), indexedPartition.getValue(), dataHelper), validationExecutor)
                                .thenApply(errorReport -> {
                                    processEntitiesWithErrors(errorReport.badEntities, neuronMetadataDao, neuronMatchesDao);
                                    return new ErrorReport(
                                            errorReport.nEntities,
                                            errorReport.nEntitiesWithErrors,
                                            errorReport.nErrors,
                                            Collections.emptyList()
                                    );
                                })
                        )
                        .collect(Collectors.toList());
        ErrorReport report = CompletableFuture.allOf(allValidationJobs.toArray(new CompletableFuture<?>[0]))
                .thenApply(voidResult -> allValidationJobs.stream().map(job -> job.join())
                                            .reduce(new ErrorReport(0L, 0L, 0L, Collections.emptyList()),
                                                    this::combineErrorReport))
                .join();
        LOG.info("Finished validating {} neuron entities - found {} errors for {} entities in {}s",
                report.nEntities, report.nErrors, report.nEntitiesWithErrors,
                (System.currentTimeMillis()-startProcessingTime)/1000.);
    }

    private ErrorReport runValidationForNeuronEntities(int jobId,
                                                       List<AbstractNeuronEntity> neuronEntities,
                                                       CachedDataHelper dataHelper) {
        long startProcessingTime = System.currentTimeMillis();
        LOG.info("Start validating {} neuron entities from partition {}", neuronEntities.size(), jobId);
        // retrieve color depth from JACS
        Set<String> mipIds = neuronEntities.stream()
                .map(AbstractNeuronEntity::getMipId)
                .collect(Collectors.toSet());
        dataHelper.cacheCDMIPs(mipIds);
        ErrorReport errorsReport = neuronEntities.stream()
                .map(ne -> {
                    List<String> errors = validateNeuronEntity(ne, dataHelper);
                    int nEntitiesWithErrors;
                    if (CollectionUtils.isNotEmpty(errors)) {
                        LOG.error("Errors found for {} -> {}", ne, errors);
                        nEntitiesWithErrors = 1;
                    } else {
                        nEntitiesWithErrors = 0;
                    }
                    return new ErrorReport(
                            0,
                            nEntitiesWithErrors,
                            errors.size(),
                            nEntitiesWithErrors == 0
                                    ? Collections.emptyList()
                                    : Collections.singletonList(ne.getEntityId()));
                })
                .reduce(new ErrorReport(0, 0, 0, Collections.emptyList()),
                        this::combineErrorReport);
        errorsReport.nEntities = neuronEntities.size();
        LOG.info("Finished validating {} neuron entities from partition {} - found {} errors for {} entities in {}s",
                errorsReport.nEntities, jobId, errorsReport.nErrors, errorsReport.nEntitiesWithErrors,
                (System.currentTimeMillis()-startProcessingTime)/1000.);
        return errorsReport;
    }

    private ErrorReport combineErrorReport(ErrorReport r1, ErrorReport r2) {
        return new ErrorReport(
                r1.nEntities + r2.nEntities,
                r1.nEntitiesWithErrors + r2.nEntitiesWithErrors,
                r1.nErrors + r2.nErrors,
                ImmutableList.<Number>builder().addAll(r1.badEntities).addAll(r2.badEntities).build());
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
                // typically if excludedLibraries is specified we want it to be only in the one that is a subset of the other;
                // for example in JACS mips from Annotator MCFO are both in MCFO and in Annotator MCFO
                // but in NeuronBridge we want the Annotator MCFO ones to only be treated as Annotator MCFO
                errors.add(String.format("Entity %s is also in %s libraries and should be both in %s and in %s",
                        ne, colorDepthMIP.libraries, args.libraries, args.excludedLibraries));
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

    private void checkComputeFile(AbstractNeuronEntity ne,
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

    private void processEntitiesWithErrors(List<Number> badEntityIds,
                                           NeuronMetadataDao<?> neuronMetadataDao,
                                           NeuronMatchesDao<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> neuronMatchesDao) {
        if (StringUtils.isNotBlank(args.errorTag) && !badEntityIds.isEmpty()) {
            long nUpdates = neuronMetadataDao.updateAll(
                    new NeuronSelector()
                            .setAlignmentSpace(args.alignmentSpace)
                            .addLibraries(args.libraries)
                            .addEntityIds(badEntityIds),
                    ImmutableMap.of("tags", new AppendFieldValueHandler<>(Collections.singleton(args.errorTag))));
            LOG.info("Marked {} entities as bad", nUpdates);
            if (args.applyTagToEMMatches) {
                long nMatchesUpdates = neuronMatchesDao.updateAll(
                        new NeuronsMatchFilter<CDMatchEntity<?, ?>>()
                                .setMaskEntityIds(badEntityIds),
                        ImmutableMap.of("tags", new AppendFieldValueHandler<>(Collections.singleton(args.errorTag)))
                );
                LOG.info("Marked {} EM CD matches as bad", nMatchesUpdates);
            }
            if (args.applyTagToLMMatches) {
                long nMatchesUpdates = neuronMatchesDao.updateAll(
                        new NeuronsMatchFilter<CDMatchEntity<?, ?>>()
                                .setTargetEntityIds(badEntityIds),
                        ImmutableMap.of("tags", new AppendFieldValueHandler<>(Collections.singleton(args.errorTag)))
                );
                LOG.info("Marked {} LM CD matches as bad", nMatchesUpdates);
            }
        } else if (badEntityIds.isEmpty()) {
            LOG.info("No bad entity to process");
        }
    }
}
