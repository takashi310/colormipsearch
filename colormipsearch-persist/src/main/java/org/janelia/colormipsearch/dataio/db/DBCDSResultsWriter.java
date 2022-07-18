package org.janelia.colormipsearch.dataio.db;

import java.util.List;

import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.dataio.ResultMatchesWriter;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public class DBCDSResultsWriter<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>>
        implements ResultMatchesWriter<M, T, R> {

    NeuronMatchesDao<M, T, R> neuronMatchesDao;

    public void write(List<R> matches) {
        neuronMatchesDao.saveAll(matches);
    }
}
