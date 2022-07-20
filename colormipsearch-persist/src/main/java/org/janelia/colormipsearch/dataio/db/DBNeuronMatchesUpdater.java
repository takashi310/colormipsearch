package org.janelia.colormipsearch.dataio.db;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.config.Config;
import org.janelia.colormipsearch.dao.DaosProvider;
import org.janelia.colormipsearch.dao.EntityFieldValueHandler;
import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.dao.support.SetFieldValueHandler;
import org.janelia.colormipsearch.dataio.NeuronMatchesUpdater;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public class DBNeuronMatchesUpdater<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>>
        implements NeuronMatchesUpdater<M, T, R> {

    private final NeuronMatchesDao<M, T, R> neuronMatchesDao;

    public DBNeuronMatchesUpdater(Config config) {
        this.neuronMatchesDao = DaosProvider.getInstance(config).getNeuronMatchesDao();
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