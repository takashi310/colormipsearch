package org.janelia.colormipsearch.dao.mongo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UnwindOptions;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.dao.PagedRequest;
import org.janelia.colormipsearch.dao.PagedResult;
import org.janelia.colormipsearch.dao.mongo.support.NeuronSelectionHelper;
import org.janelia.colormipsearch.dao.support.EntityUtils;
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

    @Override
    public R findByEntityId(Number id) {
        List<R> results = findNeuronMatches(
                Filters.eq("_id", id),
                null,
                null,
                null,
                0,
                -1
        );
        if (results.isEmpty()) {
            return null;
        } else {
            return results.get(0);
        }
    }

    @Override
    public List<R> findByEntityIds(Collection<Number> ids) {
        if (CollectionUtils.isEmpty(ids)) {
            return Collections.emptyList();
        } else {
            return findNeuronMatches(
                    Filters.in("_id", ids),
                    null,
                    null,
                    null,
                    0,
                    -1
            );
        }
    }

    @Override
    public PagedResult<R> findAll(PagedRequest pageRequest) {
        return new PagedResult<>(
                pageRequest,
                findNeuronMatches(
                        NeuronSelectionHelper.NO_FILTER,
                        null,
                        null,
                        createBsonSortCriteria(pageRequest.getSortCriteria()),
                        pageRequest.getOffset(),
                        pageRequest.getPageSize()
                )
        );
    }

    @Override
    public PagedResult<R> findNeuronMatches(NeuronSelector maskSelector, NeuronSelector targetSelector, PagedRequest pageRequest) {
        return new PagedResult<>(
                pageRequest,
                findNeuronMatches(
                        NeuronSelectionHelper.NO_FILTER,
                        maskSelector,
                        targetSelector,
                        createBsonSortCriteria(pageRequest.getSortCriteria()),
                        pageRequest.getOffset(),
                        pageRequest.getPageSize()
                )
        );
    }

    @Override
    public long countNeuronMatches(NeuronSelector maskSelector, NeuronSelector targetSelector) {
        return countAggregate(createQueryPipeline(NeuronSelectionHelper.NO_FILTER, maskSelector, targetSelector));
    }

    private List<R> findNeuronMatches(Bson matchFilter, NeuronSelector maskImageFilter, NeuronSelector matchedImageFilter, Bson sortCriteria, long offset, int length) {
        return aggregateAsList(
                createQueryPipeline(matchFilter, maskImageFilter, matchedImageFilter),
                sortCriteria,
                offset,
                length,
                getEntityType());
    }

    private List<Bson> createQueryPipeline(Bson matchFilter, NeuronSelector maskImageFilter, NeuronSelector matchedImageFilter) {
        List<Bson> pipeline = new ArrayList<>();

        pipeline.add(Aggregates.match(matchFilter));
        pipeline.add(Aggregates.lookup(
                EntityUtils.getPersistenceInfo(AbstractNeuronMetadata.class).storeName(),
                "maskImageRefId",
                "_id",
                "maskImage"
        ));
        pipeline.add(Aggregates.lookup(
                EntityUtils.getPersistenceInfo(AbstractNeuronMetadata.class).storeName(),
                "matchedImageRefId",
                "_id",
                "image" // matchedImage field name is 'image'
        ));
        UnwindOptions unwindOptions = new UnwindOptions().preserveNullAndEmptyArrays(true);
        pipeline.add(Aggregates.unwind("$maskImage", unwindOptions));
        pipeline.add(Aggregates.unwind("$image", unwindOptions));
        pipeline.add(Aggregates.match(NeuronSelectionHelper.getNeuronMatchFilter("maskImage", maskImageFilter)));
        pipeline.add(Aggregates.match(NeuronSelectionHelper.getNeuronMatchFilter("image", matchedImageFilter)));

        return pipeline;
    }

}
