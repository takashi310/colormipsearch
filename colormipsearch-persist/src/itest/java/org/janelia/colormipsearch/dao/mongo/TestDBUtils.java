package org.janelia.colormipsearch.dao.mongo;

import com.mongodb.client.MongoDatabase;

import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dao.support.IdGenerator;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public class TestDBUtils {
    public static <N extends AbstractNeuronMetadata> NeuronMetadataDao<N> createNeuronMetadataDao(
            MongoDatabase testDatabase,
            IdGenerator idGenerator) {
        return new NeuronMetadataMongoDao<>(testDatabase, idGenerator);
    }

    public static <M extends AbstractNeuronMetadata,
                   T extends AbstractNeuronMetadata,
                   R extends AbstractMatch<M, T>>
    NeuronMatchesDao<M, T, R> createNeuronMatchesDao(MongoDatabase testDatabase,
                                                     IdGenerator idGenerator) {
        return new NeuronMatchesMongoDao<>(testDatabase, idGenerator);
    }

}
