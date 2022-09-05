package org.janelia.colormipsearch.dataio.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.dao.NeuronsMatchFilter;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.datarequests.PagedResult;
import org.janelia.colormipsearch.datarequests.ScoresFilter;
import org.janelia.colormipsearch.datarequests.SortCriteria;
import org.janelia.colormipsearch.model.AbstractMatchEntity;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;

public class DBNeuronMatchesReader<R extends AbstractMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> implements NeuronMatchesReader<R> {

    private final static int PAGE_SIZE = 10000;

    private final NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao;
    private final NeuronMatchesDao<R> neuronMatchesDao;
    private final String neuronLocationAttributeName;

    public DBNeuronMatchesReader(NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao,
                                 NeuronMatchesDao<R> neuronMatchesDao,
                                 String neuronLocationAttributeName) {
        this.neuronMetadataDao = neuronMetadataDao;
        this.neuronMatchesDao = neuronMatchesDao;
        this.neuronLocationAttributeName = neuronLocationAttributeName;
    }

    @Override
    public List<String> listMatchesLocations(Collection<DataSourceParam> matchesSource) {
        return matchesSource.stream()
                        .flatMap(cdMatchInput -> neuronMetadataDao.findDistinctNeuronAttributeValues(
                                Collections.singletonList(neuronLocationAttributeName),
                                new NeuronSelector()
                                        .setAlignmentSpace(cdMatchInput.getAlignmentSpace())
                                        .addLibraries(cdMatchInput.getLibraries())
                                        .addNames(cdMatchInput.getNames())
                                        .addTags(cdMatchInput.getTags()),
                                new PagedRequest()
                                        .setFirstPageOffset(cdMatchInput.getOffset())
                                        .setPageSize(cdMatchInput.getSize())
                        ).getResultList().stream()
                                .map(this::getNeuronLocationAttributeValue))
                .collect(Collectors.toList());
    }

    private String getNeuronLocationAttributeValue(Map<String, Object> n) {
        try {
            return (String) n.get(neuronLocationAttributeName);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public List<R> readMatchesForMasks(String alignmentSpace,
                                       Collection<String> maskLibraries,
                                       Collection<String> maskMipIds,
                                       ScoresFilter matchScoresFilter,
                                       Collection<String> matchTags,
                                       List<SortCriteria> sortCriteriaList) {
        NeuronSelector maskSelector = new NeuronSelector().setAlignmentSpace(alignmentSpace).addLibraries(maskLibraries).addMipIDs(maskMipIds);
        NeuronSelector targetSelector = new NeuronSelector().setAlignmentSpace(alignmentSpace);
        NeuronsMatchFilter<R> neuronsMatchFilter = new NeuronsMatchFilter<R>()
                .setScoresFilter(matchScoresFilter)
                .setMaskEntityIds(getNeuronEntityIds(maskSelector))
                .addTags(matchTags);

        return readMatches(neuronsMatchFilter, maskSelector, targetSelector, sortCriteriaList);
    }

    @Override
    public List<R> readMatchesForTargets(String alignmentSpace,
                                         Collection<String> targetLibraries,
                                         Collection<String> targetMipIds,
                                         ScoresFilter matchScoresFilter,
                                         Collection<String> matchTags,
                                         List<SortCriteria> sortCriteriaList) {
        NeuronSelector maskSelector = new NeuronSelector().setAlignmentSpace(alignmentSpace);
        NeuronSelector targetSelector = new NeuronSelector().setAlignmentSpace(alignmentSpace).addLibraries(targetLibraries).addMipIDs(targetMipIds);
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
