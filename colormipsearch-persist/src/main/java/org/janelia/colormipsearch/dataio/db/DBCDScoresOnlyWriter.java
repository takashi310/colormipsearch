package org.janelia.colormipsearch.dataio.db;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.config.Config;
import org.janelia.colormipsearch.dao.DaosProvider;
import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.dataio.NeuronMatchesWriter;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;

/**
 * This implementation of the ResultMatchesWriter tries to update the scores for an existing ColorDepth match,
 * if the match already exists.
 * If the match does not exist it will create it.
 *
 * @param <R> match type
 */
public class DBCDScoresOnlyWriter<R extends CDMatch<? extends AbstractNeuronMetadata, ? extends AbstractNeuronMetadata>> implements NeuronMatchesWriter<R> {

    private final NeuronMatchesDao<R> neuronMatchesDao;
    private final List<Function<R, Pair<String, ?>>> fieldsToUpdate =
            Arrays.asList(
                    m -> ImmutablePair.of("matchingPixels", m.getMatchingPixels()),
                    m -> ImmutablePair.of("matchingPixelsRatio", m.getMatchingPixelsRatio()),
                    m -> ImmutablePair.of("gradientAreaGap", m.getGradientAreaGap()),
                    m -> ImmutablePair.of("highExpressionArea", m.getHighExpressionArea()),
                    m -> ImmutablePair.of("normalizedScore", m.getNormalizedScore())
            );

    public DBCDScoresOnlyWriter(Config config) {
        this.neuronMatchesDao = DaosProvider.getInstance(config).getNeuronMatchesDao();
    }

    public void write(List<R> matches) {
        neuronMatchesDao.saveOrUpdateAll(matches, fieldsToUpdate);
    }
}
