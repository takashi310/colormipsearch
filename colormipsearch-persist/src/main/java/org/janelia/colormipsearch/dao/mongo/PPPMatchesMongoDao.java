package org.janelia.colormipsearch.dao.mongo;

import com.mongodb.client.MongoDatabase;

import org.janelia.colormipsearch.dao.IdGenerator;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.PPPMatchEntity;

public class PPPMatchesMongoDao<R extends PPPMatchEntity<? extends AbstractNeuronEntity,
                                                         ? extends AbstractNeuronEntity>> extends AbstractNeuronMatchesMongoDao<R> {
    public PPPMatchesMongoDao(MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }
}
