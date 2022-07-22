package org.janelia.colormipsearch.dao.mongo;

import java.util.ArrayList;
import java.util.Collection;
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
import com.mongodb.client.model.UnwindOptions;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.conversions.Bson;
import org.janelia.colormipsearch.dao.EntityFieldNameValueHandler;
import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.dao.NeuronsMatchFilter;
import org.janelia.colormipsearch.dao.PagedRequest;
import org.janelia.colormipsearch.dao.PagedResult;
import org.janelia.colormipsearch.dao.EntityUtils;
import org.janelia.colormipsearch.dao.IdGenerator;
import org.janelia.colormipsearch.dao.SetFieldValueHandler;
import org.janelia.colormipsearch.dao.SetOnCreateValueHandler;
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
                MongoDaoHelper.createFilterById(id),
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
                    MongoDaoHelper.createFilterByIds(ids),
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
    public long countNeuronMatches(NeuronsMatchFilter<R> neuronsMatchFilter,
                                   NeuronSelector maskSelector,
                                   NeuronSelector targetSelector) {
        return MongoDaoHelper.countAggregate(
                createQueryPipeline(NeuronSelectionHelper.getNeuronsMatchFilter(neuronsMatchFilter), maskSelector, targetSelector),
                mongoCollection);
    }

    @Override
    public PagedResult<R> findNeuronMatches(NeuronsMatchFilter<R> neuronsMatchFilter,
                                            NeuronSelector maskSelector,
                                            NeuronSelector targetSelector,
                                            PagedRequest pageRequest) {
        return new PagedResult<>(
                pageRequest,
                findNeuronMatches(
                        NeuronSelectionHelper.getNeuronsMatchFilter(neuronsMatchFilter),
                        maskSelector,
                        targetSelector,
                        MongoDaoHelper.createBsonSortCriteria(pageRequest.getSortCriteria()),
                        pageRequest.getOffset(),
                        pageRequest.getPageSize()
                )
        );
    }

    private List<R> findNeuronMatches(Bson matchFilter, NeuronSelector maskImageFilter, NeuronSelector matchedImageFilter, Bson sortCriteria, long offset, int length) {
        return MongoDaoHelper.aggregateAsList(
                createQueryPipeline(matchFilter, maskImageFilter, matchedImageFilter),
                sortCriteria,
                offset,
                length,
                mongoCollection,
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
        pipeline.add(Aggregates.match(NeuronSelectionHelper.getNeuronFilter("maskImage", maskImageFilter)));
        pipeline.add(Aggregates.match(NeuronSelectionHelper.getNeuronFilter("image", matchedImageFilter)));

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
        Stream<EntityFieldNameValueHandler<?>> createSetters;
        if (!match.hasEntityId()) {
            // set entity ID just in case we need to insert it
            match.setEntityId(idGenerator.generateId());
            match.setCreatedDate(new Date());
            createSetters = Stream.of(
                    new EntityFieldNameValueHandler<>("_id", new SetOnCreateValueHandler<>(match.getEntityId())),
                    new EntityFieldNameValueHandler<>("createdDate", new SetOnCreateValueHandler<>(match.getCreatedDate()))
            );
            selectFilters.add(MongoDaoHelper.createFilterByClass(match.getClass()));
        } else {
            createSetters = Stream.of();
            selectFilters.add(MongoDaoHelper.createFilterById(match.getEntityId()));
        }
        if (match.hasMaskImageRefId() && match.hasMatchedImageRefId()) {
            selectFilters.add(MongoDaoHelper.createAttributeFilter("maskImageRefId", match.getMaskImageRefId()));
            selectFilters.add(MongoDaoHelper.createAttributeFilter("matchedImageRefId", match.getMatchedImageRefId()));
        }

        return mongoCollection.findOneAndUpdate(
                MongoDaoHelper.createBsonFilterCriteria(selectFilters),
                getUpdates(
                        Stream.concat(
                                createSetters,
                                fieldsToUpdateSelectors.stream()
                                        .map(fieldSelector -> fieldSelector.apply(match))
                                        .map(p -> new EntityFieldNameValueHandler<>(p.getLeft(), new SetFieldValueHandler<>(p.getRight())))
                                )
                                .collect(Collectors.toMap(
                                        EntityFieldNameValueHandler::getFieldName,
                                        EntityFieldNameValueHandler::getValueHandler))
                ),
                updateOptions
        );
    }

    private boolean isIdentifiable(R match) {
        return match.hasEntityId() || (match.hasMaskImageRefId() && match.hasMatchedImageRefId());
    }

}
