package org.janelia.colormipsearch.dao.mongo;

import java.util.Collections;
import java.util.List;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Indexes;

import org.bson.conversions.Bson;
import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.dao.PagedRequest;
import org.janelia.colormipsearch.dao.PagedResult;
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
        mongoCollection.createIndex(Indexes.ascending("libraryName"));
        mongoCollection.createIndex(Indexes.ascending("publishedName"));
        mongoCollection.createIndex(Indexes.ascending("id"));
    }

    @Override
    public PagedResult<N> findNeuronMatches(NeuronSelector neuronSelector, PagedRequest pageRequest) {
        return new PagedResult<>(
                pageRequest,
                MongoDaoHelper.aggregateAsList(
                        createQueryPipeline(NeuronSelectionHelper.getNeuronMatchFilter(null, neuronSelector)),
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
