package org.janelia.colormipsearch.dao.mongo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReturnDocument;

import org.bson.conversions.Bson;
import org.janelia.colormipsearch.dao.IdGenerator;
import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.dao.SetFieldValueHandler;
import org.janelia.colormipsearch.dao.SetOnCreateValueHandler;
import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.datarequests.PagedResult;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.ComputeFileType;

public class NeuronMetadataMongoDao<N extends AbstractNeuronEntity> extends AbstractMongoDao<N>
                                                                      implements NeuronMetadataDao<N> {
    private static final int MAX_UPDATE_RETRIES = 3;

    private ClientSession session;

    public NeuronMetadataMongoDao(MongoClient mongoClient, MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        super(mongoClient, mongoDatabase, idGenerator);
        createDocumentIndexes();
        session = mongoClient.startSession();
    }

    @Override
    protected void createDocumentIndexes() {
        mongoCollection.createIndex(Indexes.hashed("class"));
        mongoCollection.createIndex(Indexes.hashed("libraryName"));
        mongoCollection.createIndex(Indexes.hashed("publishedName"));
        mongoCollection.createIndex(Indexes.hashed("mipId"));
        mongoCollection.createIndex(Indexes.ascending("tags"));
    }

    @Override
    public N createOrUpdate(N neuron) {
        FindOneAndReplaceOptions updateOptions = new FindOneAndReplaceOptions();
        updateOptions.upsert(false); // here the document should not be created - minimalCreateOrUpdate will create it
        updateOptions.returnDocument(ReturnDocument.AFTER);
        if (isIdentifiable(neuron)) {
            N toUpdate = minimalCreateOrUpdate(neuron);
            return mongoCollection.findOneAndReplace(
                    session,
                    MongoDaoHelper.createFilterById(toUpdate.getEntityId()),
                    neuron,
                    updateOptions
            );
        } else {
            save(neuron);
            return neuron;
        }
    }

    private N minimalCreateOrUpdate(N neuron) {
        FindOneAndUpdateOptions updateOptions = new FindOneAndUpdateOptions();
        updateOptions.upsert(true);
        updateOptions.returnDocument(ReturnDocument.AFTER);

        List<Bson> selectFilters = new ArrayList<>();
        List<Bson> updates = new ArrayList<>();

        if (!neuron.hasEntityId()) {
            selectFilters.add(MongoDaoHelper.createFilterByClass(neuron.getClass()));
            updates.add(MongoDaoHelper.getFieldUpdate("_id", new SetOnCreateValueHandler<>(idGenerator.generateId())));
            updates.add(MongoDaoHelper.getFieldUpdate("createdDate", new SetOnCreateValueHandler<>(new Date())));
        } else {
            selectFilters.add(MongoDaoHelper.createFilterById(neuron.getEntityId()));
        }
        if (neuron.hasMipID()) {
            selectFilters.add(MongoDaoHelper.createEqFilter("mipId", neuron.getMipId()));
            updates.add(MongoDaoHelper.getFieldUpdate("mipId", new SetOnCreateValueHandler<>(neuron.getMipId())));
        }
        selectFilters.add(MongoDaoHelper.createEqFilter(
                "computeFiles.InputColorDepthImage",
                neuron.getComputeFileName(ComputeFileType.InputColorDepthImage))
        );
        selectFilters.add(MongoDaoHelper.createEqFilter(
                "computeFiles.SourceColorDepthImage",
                neuron.getComputeFileName(ComputeFileType.SourceColorDepthImage))
        );
        updates.add(MongoDaoHelper.getFieldUpdate("computeFiles.InputColorDepthImage",
                new SetFieldValueHandler<>(neuron.getComputeFileData(ComputeFileType.InputColorDepthImage))));
        updates.add(MongoDaoHelper.getFieldUpdate("computeFiles.SourceColorDepthImage",
                new SetFieldValueHandler<>(neuron.getComputeFileData(ComputeFileType.SourceColorDepthImage))));
        for (int i = 0; ; i++) {
            try {
                N updatedNeuron = mongoCollection
                        .withReadConcern(ReadConcern.LINEARIZABLE)
                        .withWriteConcern(WriteConcern.MAJORITY)
                        .withReadPreference(ReadPreference.primaryPreferred())
                        .findOneAndUpdate(
                                session,
                                MongoDaoHelper.createBsonFilterCriteria(selectFilters),
                                MongoDaoHelper.combineUpdates(updates),
                                updateOptions
                        );
                neuron.setEntityId(updatedNeuron.getEntityId());
                neuron.setCreatedDate(updatedNeuron.getCreatedDate());
                break;
            } catch (Exception e) {
                System.out.println("!!!!! FIND AND UPDATE " + neuron + " " + i + " failed " + e.toString());
                if (i >= MAX_UPDATE_RETRIES) {
                    throw new IllegalStateException(e);
                }
            }
        }
        return neuron;
    }

    private boolean isIdentifiable(N neuron) {
        return neuron.hasComputeFile(ComputeFileType.InputColorDepthImage)
            && neuron.hasComputeFile(ComputeFileType.SourceColorDepthImage);
    }

    @Override
    public PagedResult<N> findNeurons(NeuronSelector neuronSelector, PagedRequest pageRequest) {
        return new PagedResult<>(
                pageRequest,
                MongoDaoHelper.aggregateAsList(
                        createQueryPipeline(NeuronSelectionHelper.getNeuronFilter(null, neuronSelector)),
                        MongoDaoHelper.createBsonSortCriteria(pageRequest.getSortCriteria()),
                        pageRequest.getOffset(),
                        pageRequest.getPageSize(),
                        mongoCollection,
                        getEntityType()
                )
        );
    }

    public List<String> findAllNeuronAttributeValues(String attributeName, NeuronSelector neuronSelector) {
        return MongoDaoHelper.distinctAttributes(
                attributeName,
                NeuronSelectionHelper.getNeuronFilter(null, neuronSelector),
                mongoCollection,
                String.class);
    }

    private List<Bson> createQueryPipeline(Bson matchFilter) {
        return Collections.singletonList(Aggregates.match(matchFilter));
    }
}
