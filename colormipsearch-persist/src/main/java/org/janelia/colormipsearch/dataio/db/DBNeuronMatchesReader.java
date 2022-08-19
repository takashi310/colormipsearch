package org.janelia.colormipsearch.dataio.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.janelia.colormipsearch.config.Config;
import org.janelia.colormipsearch.dao.DaosProvider;
import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.dao.NeuronsMatchFilter;
import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.datarequests.PagedResult;
import org.janelia.colormipsearch.datarequests.ScoresFilter;
import org.janelia.colormipsearch.datarequests.SortCriteria;
import org.janelia.colormipsearch.model.AbstractMatchEntity;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;

public class DBNeuronMatchesReader<R extends AbstractMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> implements NeuronMatchesReader<R> {

    private final static int PAGE_SIZE = 10000;

    private final NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao;
    private final NeuronMatchesDao<R> neuronMatchesDao;
    private final Function<AbstractNeuronEntity, String> neuronLocationNameSelector;

    public DBNeuronMatchesReader(NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao,
                                 NeuronMatchesDao<R> neuronMatchesDao) {
        this(neuronMetadataDao, neuronMatchesDao, AbstractNeuronEntity::getMipId);
    }

    public DBNeuronMatchesReader(NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao,
                                 NeuronMatchesDao<R> neuronMatchesDao,
                                 Function<AbstractNeuronEntity, String> neuronLocationNameSelector) {
        this.neuronMetadataDao = neuronMetadataDao;
        this.neuronMatchesDao = neuronMatchesDao;
        this.neuronLocationNameSelector = neuronLocationNameSelector;
    }

    @Override
    public List<String> listMatchesLocations(List<DataSourceParam> matchesSource) {
        return matchesSource.stream()
                        .flatMap(cdMatchInput -> neuronMetadataDao.findNeurons(
                                new NeuronSelector()
                                        .setAlignmentSpace(cdMatchInput.getAlignmentSpace())
                                        .setLibraryName(cdMatchInput.getLibraryName())
                                        .addTags(cdMatchInput.getTags()),
                                new PagedRequest()
                                        .setFirstPageOffset(cdMatchInput.getOffset())
                                        .setPageSize(cdMatchInput.getSize())
                        ).getResultList().stream().map(neuronLocationNameSelector))
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public List<R> readMatchesForMasks(String alignmentSpace,
                                       String maskLibrary,
                                       List<String> maskMipIds,
                                       ScoresFilter matchScoresFilter,
                                       List<String> matchTags,
                                       List<SortCriteria> sortCriteriaList) {
        NeuronSelector maskSelector = new NeuronSelector().setAlignmentSpace(alignmentSpace).setLibraryName(maskLibrary).addMipIDs(maskMipIds);
        NeuronSelector targetSelector = new NeuronSelector().setAlignmentSpace(alignmentSpace);
        NeuronsMatchFilter<R> neuronsMatchFilter = new NeuronsMatchFilter<R>()
                .setScoresFilter(matchScoresFilter)
                .setMaskEntityIds(getNeuronEntityIds(maskSelector))
                .addTags(matchTags);

        return readMatches(neuronsMatchFilter, maskSelector, targetSelector, sortCriteriaList);
    }

    @Override
    public List<R> readMatchesForTargets(String alignmentSpace,
                                         String targetLibrary,
                                         List<String> targetMipIds,
                                         ScoresFilter matchScoresFilter,
                                         List<String> matchTags,
                                         List<SortCriteria> sortCriteriaList) {
        NeuronSelector maskSelector = new NeuronSelector().setAlignmentSpace(alignmentSpace);
        NeuronSelector targetSelector = new NeuronSelector().setAlignmentSpace(alignmentSpace).setLibraryName(targetLibrary).addMipIDs(targetMipIds);
        NeuronsMatchFilter<R> neuronsMatchFilter = new NeuronsMatchFilter<R>()
                .setScoresFilter(matchScoresFilter)
                .setTargetEntityIds(getNeuronEntityIds(targetSelector))
                .addTags(matchTags);

        return readMatches(neuronsMatchFilter, maskSelector, targetSelector, sortCriteriaList);
    }

    private List<Number> getNeuronEntityIds(NeuronSelector neuronSelector) {
        if (neuronSelector.isEmpty()) {
            return Collections.emptyList();
        } else {
            return neuronMetadataDao.findNeurons(neuronSelector, new PagedRequest())
                    .getResultList().stream()
                    .map(AbstractNeuronEntity::getEntityId)
                    .collect(Collectors.toList());
        }
    }

    private List<R> readMatches(NeuronsMatchFilter<R> matchesFilter,
                                NeuronSelector maskSelector,
                                NeuronSelector targetSelector,
                                List<SortCriteria> sortCriteriaList) {
        PagedRequest pagedRequest = new PagedRequest().setSortCriteria(sortCriteriaList).setPageSize(PAGE_SIZE);
        List<R> matches = new ArrayList<>();
        for (long offset = 0; ; offset += PAGE_SIZE) {
            PagedResult<R> currentMatches = neuronMatchesDao.findNeuronMatches(
                    matchesFilter,
                    maskSelector,
                    targetSelector,
                    pagedRequest.setFirstPageOffset(offset)
            );
            if (currentMatches.isEmpty()) {
                break;
            }
            matches.addAll(currentMatches.getResultList());
        }
        return matches;
    }
}
