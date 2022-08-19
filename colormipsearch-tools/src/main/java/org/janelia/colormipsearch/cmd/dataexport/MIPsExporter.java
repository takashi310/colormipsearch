package org.janelia.colormipsearch.cmd.dataexport;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.cmd.jacsdata.CachedJacsDataHelper;
import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.fileutils.ItemsWriterToJSONFile;
import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.datarequests.SortCriteria;
import org.janelia.colormipsearch.dto.AbstractNeuronMetadata;
import org.janelia.colormipsearch.dto.EMNeuronMetadata;
import org.janelia.colormipsearch.dto.LMNeuronMetadata;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
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
                        Path outputDir,
                        NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao,
                        ItemsWriterToJSONFile mipsWriter,
                        int processingPartitionSize,
                        Class<? extends AbstractNeuronMetadata> exportedClasstype) {
        super(jacsDataHelper, dataSourceParam, outputDir);
        this.neuronMetadataDao = neuronMetadataDao;
        this.mipsWriter = mipsWriter;
        this.processingPartitionSize = processingPartitionSize;
        this.exportedClasstype = exportedClasstype;
    }

    @Override
    public void runExport() {
        List<String> allPublishedNames = neuronMetadataDao.findNeurons(
                        new NeuronSelector()
                                .setAlignmentSpace(dataSourceParam.getAlignmentSpace())
                                .setLibraryName(dataSourceParam.getLibraryName()),
                        new PagedRequest().setSortCriteria(
                                Collections.singletonList(new SortCriteria("publishedName"))
                        )
                ).getResultList().stream()
                .map(AbstractNeuronEntity::getPublishedName)
                .filter(StringUtils::isNotBlank)
                .distinct()
                .collect(Collectors.toList());
        int from = dataSourceParam.hasOffset() ? Math.min((int) dataSourceParam.getOffset(), allPublishedNames.size()) : 0;
        int to = dataSourceParam.hasSize() ? from + dataSourceParam.getSize() : allPublishedNames.size();
        List<String> publishedNames = allPublishedNames.subList(from, Math.min(allPublishedNames.size(), to));
        Consumer<AbstractNeuronMetadata> updateNeuronMethod = getUpdateMethod();
        ItemsHandling.partitionCollection(publishedNames, processingPartitionSize).entrySet().stream().parallel()
                .forEach(indexedPartition -> {
                    indexedPartition.getValue().forEach(publishedName -> {
                        LOG.info("Read mips for {}", publishedName);
                        List<AbstractNeuronEntity> neuronMipEntities = neuronMetadataDao.findNeurons(
                                new NeuronSelector()
                                        .setAlignmentSpace(dataSourceParam.getAlignmentSpace())
                                        .setLibraryName(dataSourceParam.getLibraryName())
                                        .addName(publishedName),
                                new PagedRequest()).getResultList();
                        jacsDataHelper.retrieveCDMIPs(neuronMipEntities.stream()
                                .map(AbstractNeuronEntity::getMipId)
                                .collect(Collectors.toSet()));
                        List<AbstractNeuronMetadata> neuronMips = neuronMipEntities.stream()
                                .map(AbstractNeuronEntity::metadata)
                                .collect(Collectors.toList());
                        neuronMips.forEach(updateNeuronMethod);
                        LOG.info("Write mips for {}", publishedName);
                        mipsWriter.writeItems(neuronMips, outputDir, publishedName);
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
