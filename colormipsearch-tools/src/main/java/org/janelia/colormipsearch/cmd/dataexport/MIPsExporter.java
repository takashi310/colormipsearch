package org.janelia.colormipsearch.cmd.dataexport;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
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
import org.janelia.colormipsearch.results.GroupedItems;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MIPsExporter extends AbstractDataExporter {
    private static final Logger LOG = LoggerFactory.getLogger(MIPsExporter.class);

    private final NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao;
    private final ItemsWriterToJSONFile mipsWriter;
    private final int processingPartitionSize;
    private final Class<? extends AbstractNeuronMetadata> exportedClasstype;

    public MIPsExporter(CachedJacsDataHelper jacsDataHelper,
                        DataSourceParam dataSourceParam,
                        int relativesUrlsToComponent,
                        Path outputDir,
                        NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao,
                        ItemsWriterToJSONFile mipsWriter,
                        int processingPartitionSize,
                        Class<? extends AbstractNeuronMetadata> exportedClasstype) {
        super(jacsDataHelper, dataSourceParam, relativesUrlsToComponent, outputDir);
        this.neuronMetadataDao = neuronMetadataDao;
        this.mipsWriter = mipsWriter;
        this.processingPartitionSize = processingPartitionSize;
        this.exportedClasstype = exportedClasstype;
    }

    @Override
    public void runExport() {
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
        Consumer<AbstractNeuronMetadata> updateNeuronMethod = getUpdateMethod();
        ItemsHandling.partitionCollection(publishedNames, processingPartitionSize).entrySet().stream().parallel()
                .forEach(indexedPartition -> {
                    indexedPartition.getValue().forEach(publishedName -> {
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
                        jacsDataHelper.retrieveCDMIPs(mipIds);
                        List<AbstractNeuronMetadata> neuronMips = neuronMipEntities.stream()
                                .map(AbstractNeuronEntity::metadata)
                                .collect(Collectors.toList());
                        // update neurons info and filter out unpublished ones
                        Set<AbstractNeuronMetadata> publishedNeuronMips = neuronMips.stream()
                                .peek(updateNeuronMethod)
                                .filter(AbstractNeuronMetadata::isPublished)
                                .peek(n -> n.setNeuronFile(FileType.ColorDepthMipInput, null)) // reset mip input
                                .peek(n -> n.updateAllNeuronFiles(this::relativizeURL))
                                .collect(Collectors.toSet());
                        LOG.info("Write mips for {}", publishedName);
                        mipsWriter.writeJSON(GroupedItems.createGroupedItems(null, publishedNeuronMips), outputDir, publishedName);
                    });
                });
    }

    private Consumer<AbstractNeuronMetadata> getUpdateMethod() {
        if (EMNeuronMetadata.class.getName().equals(exportedClasstype.getName())) {
            return n -> this.updateEMNeuron((EMNeuronMetadata) n);
        } else if (LMNeuronMetadata.class.getName().equals(exportedClasstype.getName())) {
            return n -> this.updateLMNeuron((LMNeuronMetadata) n);
        } else {
            throw new IllegalArgumentException("Invalid exported class");
        }
    }
}
