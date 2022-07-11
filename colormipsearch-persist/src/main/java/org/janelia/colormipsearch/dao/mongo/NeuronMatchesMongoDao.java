package org.janelia.colormipsearch.dao.mongo;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;

import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.dao.support.IdGenerator;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public class NeuronMatchesMongoDao<M extends AbstractNeuronMetadata,
                                   T extends AbstractNeuronMetadata,
                                   R extends AbstractMatch<M, T>> extends AbstractMongoDao<R>
                                                                  implements NeuronMatchesDao<M, T, R> {

    public NeuronMatchesMongoDao(MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }

    @Override
    protected void createDocumentIndexes() {
        mongoCollection.createIndex(Indexes.ascending("class"));
    }

}
