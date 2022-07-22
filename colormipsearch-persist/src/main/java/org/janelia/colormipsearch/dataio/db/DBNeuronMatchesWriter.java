package org.janelia.colormipsearch.dataio.db;

import java.util.List;

import org.janelia.colormipsearch.config.Config;
import org.janelia.colormipsearch.dao.DaosProvider;
import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.dataio.NeuronMatchesWriter;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public class DBNeuronMatchesWriter<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>>
        implements NeuronMatchesWriter<M, T, R> {

    private final NeuronMatchesDao<M, T, R> neuronMatchesDao;

    public DBNeuronMatchesWriter(Config config) {
        this.neuronMatchesDao = DaosProvider.getInstance(config).getNeuronMatchesDao();
    }

    @Override
    public void write(List<R> matches) {
        neuronMatchesDao.saveAll(matches);
    }

}
