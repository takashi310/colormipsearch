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
import org.janelia.colormipsearch.dao.SetFieldValueHandler;
import org.janelia.colormipsearch.dataio.NeuronMatchesWriter;
import org.janelia.colormipsearch.model.AbstractMatchEntity;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;

public class DBNeuronMatchesWriter<R extends AbstractMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>>
        implements NeuronMatchesWriter<R> {

    private final NeuronMatchesDao<R> neuronMatchesDao;

    public DBNeuronMatchesWriter(Config config) {
        this.neuronMatchesDao = DaosProvider.getInstance(config).getNeuronMatchesDao();
    }

    @Override
    public void write(List<R> matches) {
        neuronMatchesDao.saveAll(matches);
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
