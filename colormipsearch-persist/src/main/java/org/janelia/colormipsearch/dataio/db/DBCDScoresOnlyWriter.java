package org.janelia.colormipsearch.dataio.db;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.config.Config;
import org.janelia.colormipsearch.dao.DaosProvider;
import org.janelia.colormipsearch.dao.EntityFieldValueHandler;
import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.dao.SetFieldValueHandler;
import org.janelia.colormipsearch.dataio.NeuronMatchesWriter;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.CDMatchEntity;

/**
 * This implementation of the ResultMatchesWriter tries to update the scores for an existing ColorDepth match,
 * if the match already exists.
 * If the match does not exist it will create it.
 *
 * @param <R> match type
 */
public class DBCDScoresOnlyWriter<R extends CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> implements NeuronMatchesWriter<R> {

    private final NeuronMatchesDao<R> neuronMatchesDao;

    private final List<Function<R, NeuronMatchesDao.NeuronField<?>>> fieldsToUpdate =
            Arrays.asList(
                    m -> new NeuronMatchesDao.NeuronField<>("sessionRefId", false, m.getSessionRefId()),
                    m -> new NeuronMatchesDao.NeuronField<>("mirrored", false, m.isMirrored()),
                    m -> new NeuronMatchesDao.NeuronField<>("matchingPixels", false, m.getMatchingPixels()),
                    m -> new NeuronMatchesDao.NeuronField<>("matchingPixelsRatio", false, m.getMatchingPixelsRatio()),
                    m -> new NeuronMatchesDao.NeuronField<>("normalizedScore", false, m.getNormalizedScore()),
                    m -> new NeuronMatchesDao.NeuronField<>("tags", true, m.getTags())
            );

    public DBCDScoresOnlyWriter(NeuronMatchesDao<R> neuronMatchesDao) {
        this.neuronMatchesDao = neuronMatchesDao;
    }

    public void write(List<R> matches) {
        neuronMatchesDao.createOrUpdateAll(matches, fieldsToUpdate);
    }

    @Override
    public void writeUpdates(List<R> matches, List<Function<R, Pair<String, ?>>> fieldSelectors) {
        for (R match : matches) {
            Map<String, EntityFieldValueHandler<?>> fieldValueHandlerMap =
                    fieldSelectors
                            .stream()
                            .map(fieldSelector -> fieldSelector.apply(match))
                            .collect(Collectors.toMap(Pair::getLeft, fld -> new SetFieldValueHandler<>(fld.getRight())));
            neuronMatchesDao.update(match.getEntityId(), fieldValueHandlerMap);
        }
    }
}
