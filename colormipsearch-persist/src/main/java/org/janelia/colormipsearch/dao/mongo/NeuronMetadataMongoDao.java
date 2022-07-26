package org.janelia.colormipsearch.dao.mongo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReturnDocument;

import org.apache.commons.lang3.tuple.Pair;
import org.bson.conversions.Bson;
import org.janelia.colormipsearch.dao.EntityFieldNameValueHandler;
import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.dao.SetFieldValueHandler;
import org.janelia.colormipsearch.dao.SetOnCreateValueHandler;
import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.datarequests.PagedResult;
import org.janelia.colormipsearch.dao.IdGenerator;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.ComputeFileType;

public class NeuronMetadataMongoDao<N extends AbstractNeuronMetadata> extends AbstractMongoDao<N>
                                                                      implements NeuronMetadataDao<N> {
    public NeuronMetadataMongoDao(MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
        createDocumentIndexes();
    }

    @Override
    protected void createDocumentIndexes() {
        mongoCollection.createIndex(Indexes.hashed("class"));
        mongoCollection.createIndex(Indexes.hashed("libraryName"));
        mongoCollection.createIndex(Indexes.hashed("publishedName"));
        mongoCollection.createIndex(Indexes.hashed("id"));
    }

    @Override
    public void createOrUpdate(N neuron) {
        FindOneAndUpdateOptions updateOptions = new FindOneAndUpdateOptions();
        updateOptions.upsert(true);
        updateOptions.returnDocument(ReturnDocument.AFTER);
        List<Bson> selectFilters = new ArrayList<>();
        Stream<EntityFieldNameValueHandler<?>> createSetters;
        if (!neuron.hasEntityId()) {
            // set entity ID just in case we need to insert it
            neuron.setEntityId(idGenerator.generateId());
            neuron.setCreatedDate(new Date());
            createSetters = Stream.of(
                    new EntityFieldNameValueHandler<>("_id", new SetOnCreateValueHandler<>(neuron.getEntityId())),
                    new EntityFieldNameValueHandler<>("createdDate", new SetOnCreateValueHandler<>(neuron.getCreatedDate()))
            );
            selectFilters.add(MongoDaoHelper.createFilterByClass(neuron.getClass()));
        } else {
            createSetters = Stream.of();
            selectFilters.add(MongoDaoHelper.createFilterById(neuron.getEntityId()));
        }
        if (neuron.hasMipID()) {
            selectFilters.add(MongoDaoHelper.createEqFilter("id", neuron.getMipId()));
        }
        if (neuron.hasComputeFile(ComputeFileType.InputColorDepthImage)) {
            selectFilters.add(MongoDaoHelper.createEqFilter(
                    "computeFiles.InputColorDepthImage",
                    neuron.getComputeFileName(ComputeFileType.InputColorDepthImage))
            );
        }
        if (neuron.hasComputeFile(ComputeFileType.SourceColorDepthImage)) {
            selectFilters.add(MongoDaoHelper.createEqFilter(
                    "computeFiles.SourceColorDepthImage",
                    neuron.getComputeFileName(ComputeFileType.SourceColorDepthImage))
            );
        }

        mongoCollection.findOneAndUpdate(
                MongoDaoHelper.createBsonFilterCriteria(selectFilters),
                getUpdates(
                        Stream.concat(
                                        createSetters,
                                        neuron.updatableFields().stream()
                                                .map(p -> new EntityFieldNameValueHandler<>(p.getLeft(), new SetFieldValueHandler<>(p.getRight())))
                                )
                                .collect(Collectors.toMap(
                                        EntityFieldNameValueHandler::getFieldName,
                                        EntityFieldNameValueHandler::getValueHandler))
                ),
                updateOptions
        );

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

    private List<Bson> createQueryPipeline(Bson matchFilter) {
        return Collections.singletonList(Aggregates.match(matchFilter));
    }
}
