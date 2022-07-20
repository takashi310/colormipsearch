package org.janelia.colormipsearch.dataio.db;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.colormipsearch.config.Config;
import org.janelia.colormipsearch.dao.DaosProvider;
import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.dataio.NeuronMatchesWriter;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;

/**
 * This implementation of the ResultMatchesWriter tries to update the scores for an existing ColorDepth match,
 * if the match already exists.
 * If the match does not exist it will create it.
 *
 * @param <M> mask type
 * @param <T>
 */
public class DBCDScoresOnlyWriter<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata>
        implements NeuronMatchesWriter<M, T, CDMatch<M, T>> {

    private final NeuronMatchesDao<M, T, CDMatch<M, T>> neuronMatchesDao;

    public DBCDScoresOnlyWriter(Config config) {
        this.neuronMatchesDao = DaosProvider.getInstance(config).getNeuronMatchesDao();
    }

    public void write(List<CDMatch<M, T>> matches) {
        neuronMatchesDao.saveOrUpdateAll(
                matches,
                Arrays.asList(
                        m -> ImmutablePair.of("matchingPixels", m.getMatchingPixels()),
                        m -> ImmutablePair.of("matchingPixelsRatio", m.getMatchingPixelsRatio()),
                        m -> ImmutablePair.of("gradientAreaGap", m.getGradientAreaGap()),
                        m -> ImmutablePair.of("highExpressionArea", m.getHighExpressionArea()),
                        m -> ImmutablePair.of("normalizedScore", m.getNormalizedScore())
                ));
    }
}
