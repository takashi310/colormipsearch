package org.janelia.colormipsearch.dao.mongo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.janelia.colormipsearch.AbstractITest;
import org.janelia.colormipsearch.dao.Dao;
import org.janelia.colormipsearch.dao.mongo.support.RegistryHelper;
import org.janelia.colormipsearch.dao.support.IdGenerator;
import org.janelia.colormipsearch.dao.support.TimebasedIdGenerator;
import org.janelia.colormipsearch.model.BaseEntity;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class AbstractMongoDaoITest extends AbstractITest {
    private static MongoClient testMongoClient;

    MongoDatabase testMongoDatabase;
    IdGenerator idGenerator;

    @BeforeClass
    public static void setUpMongoClient() {
        CodecRegistry codecRegistry = RegistryHelper.createCodecRegistry();
        MongoClientSettings.Builder mongoClientSettingsBuilder = MongoClientSettings.builder()
                .codecRegistry(CodecRegistries.fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        codecRegistry))
                .applyToConnectionPoolSettings(builder -> builder.maxConnectionIdleTime(60, TimeUnit.SECONDS))
                .applyConnectionString(new ConnectionString(getTestProperty("MongoDB.ConnectionURL", "mongodb://localhost:27017")))
                ;
        testMongoClient = MongoClients.create(mongoClientSettingsBuilder.build());
    }

    @Before
    public final void setUpDaoResources() {
        idGenerator = new TimebasedIdGenerator(0);
        testMongoDatabase = testMongoClient.getDatabase(getTestProperty("MongoDB.Database", null));
    }

    protected <R extends BaseEntity> void deleteAll(Dao<R> dao, List<R> es) {
        for (R e : es) {
            delete(dao, e);
        }
    }

    protected <R extends BaseEntity> void delete(Dao<R> dao, R e) {
        if (e.hasEntityId()) {
            dao.delete(e);
        }
    }

    protected <R extends BaseEntity> R persistEntity(Dao<R> dao, R e) {
        dao.save(e);
        return e;
    }

}
