package org.janelia.colormipsearch.dao.mongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Var;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Variable;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.janelia.colormipsearch.dao.EntityUtils;
import org.janelia.colormipsearch.dao.IdGenerator;
import org.janelia.colormipsearch.dao.PublishedImageDao;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.PublishedImage;
import org.janelia.colormipsearch.model.PublishedImageFields;

public class PublishedImageMongoDao extends AbstractMongoDao<PublishedImage>
        implements PublishedImageDao {

    private static final List<String> GAL4_RELEASES = Arrays.asList("Gen1 GAL4", "Gen1 LexA");

    public PublishedImageMongoDao(MongoClient mongoClient, MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        super(mongoClient, mongoDatabase, idGenerator);
        createDocumentIndexes();
    }

    @Override
    public List<PublishedImage> getPublishedImagesBySample(String sampleRef) {
        if (StringUtils.isBlank(sampleRef)) {
            return Collections.emptyList();
        } else {
            return MongoDaoHelper.aggregateAsList(
                    createQueryPipeline(null, Collections.singleton(sampleRef), null),
                    null,
                    0,
                    0,
                    mongoCollection,
                    getEntityType());
        }
    }

    @Override
    public Map<String, List<PublishedImage>> getPublishedImagesBySampleObjectives(@Nullable String alignmentSpace,
                                                                                  Collection<String> sampleRefs,
                                                                                  @Nullable String objective) {
        if (CollectionUtils.isEmpty(sampleRefs)) {
            return Collections.emptyMap();
        } else {
            return MongoDaoHelper.aggregateAsList(
                    createQueryPipeline(alignmentSpace, sampleRefs, objective),
                    null,
                    0,
                    0,
                    mongoCollection,
                    getEntityType()).stream()
                    .collect(Collectors.groupingBy(
                            PublishedImageFields::getSampleRef,
                            Collectors.toList()
                    ));
        }
    }

    private List<Bson> createQueryPipeline(@Nullable String alignmentSpace,
                                           Collection<String> sampleRefs,
                                           @Nullable String objective) {
        List<Bson> pipeline = new ArrayList<>();
        pipeline.add(Aggregates.match(
                MongoDaoHelper.createBsonFilterCriteria(
                        Arrays.asList(
                                StringUtils.isBlank(alignmentSpace)
                                        ? null
                                        : MongoDaoHelper.createEqFilter("alignmentSpace", alignmentSpace),
                                Filters.in("sampleRef", sampleRefs),
                                StringUtils.isBlank(objective)
                                        ? null
                                        : MongoDaoHelper.createEqFilter("objective", objective)
                        )
                )
        ));
        pipeline.add(Aggregates.lookup(
                EntityUtils.getPersistenceInfo(PublishedImage.class).storeName(),
                Arrays.asList(
                        new Variable<>("sourceLine", "$originalLine"),
                        new Variable<>("sourceArea", "$area"),
                        new Variable<>("gal4Releases", GAL4_RELEASES)
                ),
                Arrays.asList(
                        Aggregates.match(
                                Filters.expr(
                                        Filters.and(
                                                MongoDaoHelper.createAggregateExpr("$eq", "$originalLine", "$$sourceLine"),
                                                MongoDaoHelper.createAggregateExpr("$eq", "$area", "$$sourceArea"),
                                                MongoDaoHelper.createAggregateExpr("$in", "$releaseName", "$$gal4Releases")
                                        )
                                )
                        )
                ),
                "gal4"
                ));
        return pipeline;
    }

    @Override
    protected void createDocumentIndexes() {
        // do nothing here
    }
}
