package org.janelia.colormipsearch.dao.mongo;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;

import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dao.support.IdGenerator;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public class NeuronMetadataMongoDao<N extends AbstractNeuronMetadata> extends AbstractMongoDao<N>
                                                                      implements NeuronMetadataDao<N> {
    public NeuronMetadataMongoDao(MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }

    @Override
    protected void createDocumentIndexes() {
        mongoCollection.createIndex(Indexes.ascending("class"));
    }
}
