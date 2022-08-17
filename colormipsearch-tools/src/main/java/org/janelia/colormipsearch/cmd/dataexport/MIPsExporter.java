package org.janelia.colormipsearch.cmd.dataexport;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.fileutils.ItemsWriterToJSONFile;
import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.datarequests.SortCriteria;
import org.janelia.colormipsearch.dto.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MIPsExporter implements DataExporter {
    private static final Logger LOG = LoggerFactory.getLogger(MIPsExporter.class);

    private final DataSourceParam dataSourceParam;
    private final Path outputDir;
    private final NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao;
    private final ItemsWriterToJSONFile mipsWriter;
    private final int processingPartitionSize;

    public MIPsExporter(DataSourceParam dataSourceParam,
                        Path outputDir,
                        NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao,
                        ItemsWriterToJSONFile mipsWriter,
                        int processingPartitionSize) {
        this.dataSourceParam = dataSourceParam;
        this.outputDir = outputDir;
        this.neuronMetadataDao = neuronMetadataDao;
        this.mipsWriter = mipsWriter;
        this.processingPartitionSize = processingPartitionSize;
    }

    @Override
    public DataSourceParam getDataSource() {
        return dataSourceParam;
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
                ).getResultList().stream().map(AbstractNeuronEntity::getPublishedName)
                .distinct()
                .collect(Collectors.toList());
        int from = dataSourceParam.hasOffset() ? Math.min((int) dataSourceParam.getOffset(), allPublishedNames.size()) : 0;
        int to = dataSourceParam.hasSize() ? from + dataSourceParam.getSize() : allPublishedNames.size();
        List<String> publishedNames = allPublishedNames.subList(from, Math.min(allPublishedNames.size(), to));
        ItemsHandling.partitionCollection(publishedNames, processingPartitionSize).entrySet().stream().parallel()
                .forEach(indexedPartition -> {
                    indexedPartition.getValue().forEach(publishedName -> {
                        LOG.info("Read mips for {}", publishedName);
                        List<AbstractNeuronMetadata> neuronMips = neuronMetadataDao.findNeurons(
                                new NeuronSelector()
                                        .setAlignmentSpace(dataSourceParam.getAlignmentSpace())
                                        .setLibraryName(dataSourceParam.getLibraryName())
                                        .addName(publishedName),
                                new PagedRequest()).getResultList().stream()
                                .map(AbstractNeuronEntity::metadata)
                                .collect(Collectors.toList());
                        LOG.info("Write mips for {}", publishedName);
                        mipsWriter.writeItems(neuronMips, outputDir, publishedName);
                    });
                });
    }
}
