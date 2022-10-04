package org.janelia.colormipsearch.cmd.dataexport;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.janelia.colormipsearch.cmd.jacsdata.CachedJacsDataHelper;
import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.fileutils.ItemsWriterToJSONFile;
import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.dto.AbstractNeuronMetadata;
import org.janelia.colormipsearch.dto.EMNeuronMetadata;
import org.janelia.colormipsearch.dto.LMNeuronMetadata;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.FileType;
import org.janelia.colormipsearch.model.PublishedURLs;
import org.janelia.colormipsearch.results.GroupedItems;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MIPsExporter extends AbstractDataExporter {
    private static final Logger LOG = LoggerFactory.getLogger(MIPsExporter.class);
    private static final Pattern ARTIFICIALLY_CREATED_PATTERN = Pattern.compile("Created by .+import");

    private final NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao;
    private final ItemsWriterToJSONFile mipsWriter;
    private final int processingPartitionSize;
    private final Class<? extends AbstractNeuronMetadata> exportedClasstype;

    public MIPsExporter(CachedJacsDataHelper jacsDataHelper,
                        DataSourceParam dataSourceParam,
                        int relativesUrlsToComponent,
                        ImageStoreMapping imageStoreMapping,
                        Path outputDir,
                        Executor executor,
                        NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao,
                        ItemsWriterToJSONFile mipsWriter,
                        int processingPartitionSize,
                        Class<? extends AbstractNeuronMetadata> exportedClasstype) {
        super(jacsDataHelper, dataSourceParam, relativesUrlsToComponent, imageStoreMapping, outputDir, executor);
        this.neuronMetadataDao = neuronMetadataDao;
        this.mipsWriter = mipsWriter;
        this.processingPartitionSize = processingPartitionSize;
        this.exportedClasstype = exportedClasstype;
    }

    @Override
    public void runExport() {
        long startProcessingTime = System.currentTimeMillis();
        Set<String> publishedNames = neuronMetadataDao.findDistinctNeuronAttributeValues(
                Collections.singletonList("publishedName"),
                new NeuronSelector()
                        .setAlignmentSpace(dataSourceParam.getAlignmentSpace())
                        .addLibraries(dataSourceParam.getLibraries())
                        .addTags(dataSourceParam.getTags())
                        .addNames(dataSourceParam.getNames())
                        .withValidPubishingName(),
                new PagedRequest()
                        .setFirstPageOffset(dataSourceParam.getOffset())
                        .setPageSize(dataSourceParam.getSize()))
                .getResultList().stream().map(n -> (String) n.get("publishedName")).collect(Collectors.toSet());
        List<CompletableFuture<Void>> allExportsJobs = ItemsHandling.partitionCollection(publishedNames, processingPartitionSize).entrySet().stream().parallel()
                .map(indexedPartition -> CompletableFuture.<Void>supplyAsync(() -> {
                    runExportForPublishedNames(indexedPartition.getKey(), indexedPartition.getValue());
                    return null;
                }, executor))
                .collect(Collectors.toList());
        CompletableFuture.allOf(allExportsJobs.toArray(new CompletableFuture<?>[0])).join();
        LOG.info("Finished all exports in {}s", (System.currentTimeMillis()-startProcessingTime)/1000.);
    }

    private void runExportForPublishedNames(int jobId, List<String> publishedNames) {
        long startProcessingTime = System.currentTimeMillis();
        LOG.info("Start processing {} publishedNames from partition {}", publishedNames.size(), jobId);
        BiConsumer<AbstractNeuronMetadata, Map<Number, PublishedURLs>> updateNeuronMethod = getUpdateMethod();
        publishedNames.forEach(publishedName -> {
            LOG.info("Read mips for {}", publishedName);
            List<AbstractNeuronEntity> neuronMipEntities = neuronMetadataDao.findNeurons(
                    new NeuronSelector()
                            .setAlignmentSpace(dataSourceParam.getAlignmentSpace())
                            .addLibraries(dataSourceParam.getLibraries())
                            .addName(publishedName),
                    new PagedRequest()).getResultList();
            // retrieve the rest of the data needed for all <publishedName>'s MIPs
            Set<String> mipIds = neuronMipEntities.stream()
                    .map(AbstractNeuronEntity::getMipId)
                    .collect(Collectors.toSet());
            // retrieve color depth mips info from JACS
            jacsDataHelper.retrieveCDMIPs(mipIds);
            List<AbstractNeuronMetadata> neuronMips = neuronMipEntities.stream()
                    .filter(this::hasNotBeenArtificiallyCreated) // filter out MIPs artificially generated at import
                    .map(AbstractNeuronEntity::metadata)
                    .collect(Collectors.toList());
            // retrieve URLs associated with current neurons
            Map<Number, PublishedURLs> indexedNeuronURLs = jacsDataHelper.retrievePublishedURLs(neuronMipEntities);
            // update neurons info and filter out unpublished ones
            Set<AbstractNeuronMetadata> publishedNeuronMips = neuronMips.stream()
                    .peek(n -> updateNeuronMethod.accept(n, indexedNeuronURLs))
                    .filter(AbstractNeuronMetadata::isPublished)
                    .peek(n -> n.setNeuronFile(FileType.CDMInput, null)) // reset mip input
                    .peek(n -> n.transformAllNeuronFiles(this::relativizeURL))
                    .collect(Collectors.toSet());
            if (publishedNeuronMips.isEmpty()) {
                // skip export - simply log
                LOG.warn("No published MIPs for {}", publishedName);
            } else {
                LOG.info("Write {} mips for {}", publishedNeuronMips.size(), publishedName);
                mipsWriter.writeJSON(GroupedItems.createGroupedItems(null, publishedNeuronMips), outputDir, publishedName);
            }
        });
        LOG.info("Finished processing partition {} in {}s", jobId, (System.currentTimeMillis()-startProcessingTime)/1000.);
    }

    private BiConsumer<AbstractNeuronMetadata, Map<Number, PublishedURLs>> getUpdateMethod() {
        if (EMNeuronMetadata.class.getName().equals(exportedClasstype.getName())) {
            return (n, publishedURLsMap) -> this.updateEMNeuron((EMNeuronMetadata) n, publishedURLsMap.get(n.getInternalId()));
        } else if (LMNeuronMetadata.class.getName().equals(exportedClasstype.getName())) {
            return (n, publishedURLsMap) -> this.updateLMNeuron((LMNeuronMetadata) n, publishedURLsMap.get(n.getInternalId()));
        } else {
            throw new IllegalArgumentException("Invalid exported class");
        }
    }

    /**
     * This method checks if the neuron has been artificially created during the import.
     * Such neurons have a tag that looks like: "Created by dataset-vs-dataset import"
     *
     * @param n
     * @return
     */
    private boolean hasNotBeenArtificiallyCreated(AbstractNeuronEntity n) {
        boolean bresult = n.getTags().stream().noneMatch(t -> ARTIFICIALLY_CREATED_PATTERN.matcher(t).find());
        if (!bresult) {
            // if the MIP was artificially generated log this fact
            LOG.warn("Artificially created mip: {}", bresult);
        }
        return bresult;
    }
}
