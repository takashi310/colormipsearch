package org.janelia.colormipsearch.dao;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

import org.janelia.colormipsearch.config.Config;
import org.janelia.colormipsearch.dao.mongo.NeuronMatchesMongoDao;
import org.janelia.colormipsearch.dao.mongo.NeuronMetadataMongoDao;
import org.janelia.colormipsearch.dao.mongo.support.MongoDBHelper;
import org.janelia.colormipsearch.dao.support.IdGenerator;
import org.janelia.colormipsearch.dao.support.TimebasedIdGenerator;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public class DaosProvider {

    private static DaosProvider instance;

    public static synchronized DaosProvider getInstance(Config config) {
        if (instance == null) {
            instance = new DaosProvider(config);
        }
        return instance;
    }

    private final MongoClient mongoClient;
    private final MongoDatabase mongoDatabase;
    private final IdGenerator idGenerator;

    private DaosProvider(Config config) {
        mongoClient = MongoDBHelper.createMongoClient(config.getStringPropertyValue("MongoDB.ConnectionURL"),
                config.getStringPropertyValue("MongoDB.Server"),
                config.getStringPropertyValue("MongoDB.AuthDatabase"),
                config.getStringPropertyValue("MongoDB.Username"),
                config.getStringPropertyValue("MongoDB.Password"),
                config.getStringPropertyValue("MongoDB.ReplicaSet"),
                config.getBooleanPropertyValue("MongoDB.UseSSL"),
                config.getIntegerPropertyValue("MongoDB.Connections", 0),
                config.getIntegerPropertyValue("MongoDB.ConnectionTimeoutMillis", 0),
                config.getIntegerPropertyValue("MongoDB.MaxConnecting", 0),
                config.getIntegerPropertyValue("MongoDB.MaxConnectTimeSecs", 0),
                config.getIntegerPropertyValue("MongoDB.MaxConnectionIdleSecs", 0),
                config.getIntegerPropertyValue("MongoDB.MaxConnectionLifeSecs", 0)
        );
        this.mongoDatabase = MongoDBHelper.createMongoDatabase(mongoClient, config.getStringPropertyValue("MongoDB.Database"));
        this.idGenerator = new TimebasedIdGenerator(config.getIntegerPropertyValue("TimebasedId.Context", 0));
    }

    public <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>> NeuronMatchesDao<M, T, R>
    getNeuronMatchesDao() {
        return new NeuronMatchesMongoDao<>(mongoDatabase, idGenerator);
    }

    public <N extends AbstractNeuronMetadata> NeuronMetadataDao<N>
    getNeuronMetadataDao() {
        return new NeuronMetadataMongoDao<>(mongoDatabase, idGenerator);
    }

}
