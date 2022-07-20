package org.janelia.colormipsearch.dao.mongo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UnwindOptions;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.conversions.Bson;
import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.dao.PagedRequest;
import org.janelia.colormipsearch.dao.PagedResult;
import org.janelia.colormipsearch.dao.mongo.support.NeuronSelectionHelper;
import org.janelia.colormipsearch.dao.support.EntityUtils;
import org.janelia.colormipsearch.dao.support.IdGenerator;
import org.janelia.colormipsearch.dao.support.SetFieldValueHandler;
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
    public PagedResult<R> findAll(Class<R> type, PagedRequest pageRequest) {
        return new PagedResult<>(
                pageRequest,
                findNeuronMatches(
                        MongoDaoHelper.createFilterByClass(type),
                        null,
                        null,
                        MongoDaoHelper.createBsonSortCriteria(pageRequest.getSortCriteria()),
                        pageRequest.getOffset(),
                        pageRequest.getPageSize()
                )
        );
    }

    @Override
    public long countNeuronMatches(NeuronSelector maskSelector, NeuronSelector targetSelector, Class<R> matchType) {
        return countAggregate(createQueryPipeline(MongoDaoHelper.createFilterByClass(matchType), maskSelector, targetSelector));
    }

    @Override
    public PagedResult<R> findNeuronMatches(NeuronSelector maskSelector, NeuronSelector targetSelector, Class<R> matchType,
                                            PagedRequest pageRequest) {
        return new PagedResult<>(
                pageRequest,
                findNeuronMatches(
                        NeuronSelectionHelper.NO_FILTER,
                        maskSelector,
                        targetSelector,
                        MongoDaoHelper.createBsonSortCriteria(pageRequest.getSortCriteria()),
                        pageRequest.getOffset(),
                        pageRequest.getPageSize()
                )
        );
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

    @Override
    public void saveOrUpdateAll(List<R> matches, List<Function<R, Pair<String, ?>>> fieldsToUpdateSelectors) {
        matches.forEach(m -> {
            if (isIdentifiable(m)) {
                findAndUpdate(m, fieldsToUpdateSelectors);
            } else {
                save(m);
            }
        });
    }

    private R findAndUpdate(R match, List<Function<R, Pair<String, ?>>> fieldsToUpdateSelectors) {
        FindOneAndUpdateOptions updateOptions = new FindOneAndUpdateOptions();
        updateOptions.upsert(true);
        updateOptions.returnDocument(ReturnDocument.AFTER);
        List<Bson> selectFilters = new ArrayList<>();
        if (!match.hasEntityId()) {
            // set entity ID just in case we need to insert it
            match.setEntityId(idGenerator.generateId());
            match.setCreatedDate(new Date());
            selectFilters.add(Filters.eq("class", match.getClass().getName()));
        } else {
            selectFilters.add(Filters.eq("_id", match.getEntityId()));
        }
        if (match.hasMaskImageRefId() && match.hasMatchedImageRefId()) {
            selectFilters.add(Filters.eq("maskImageRefId", match.getMaskImageRefId()));
            selectFilters.add(Filters.eq("matchedImageRefId", match.getMatchedImageRefId()));
        }

        return mongoCollection.findOneAndUpdate(
                MongoDaoHelper.createBsonFilterCriteria(selectFilters),
                getUpdates(
                        fieldsToUpdateSelectors.stream()
                                .map(fieldSelector -> fieldSelector.apply(match))
                                .collect(Collectors.toMap(Pair::getLeft, p -> new SetFieldValueHandler<>(p.getRight())))
                ),
                updateOptions
        );
    }

    private boolean isIdentifiable(R match) {
        return match.hasEntityId() || (match.hasMaskImageRefId() && match.hasMatchedImageRefId());
    }

}
