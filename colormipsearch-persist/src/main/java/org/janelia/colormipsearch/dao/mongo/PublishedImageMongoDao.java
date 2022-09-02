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
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.conversions.Bson;
import org.janelia.colormipsearch.dao.IdGenerator;
import org.janelia.colormipsearch.dao.PublishedImageDao;
import org.janelia.colormipsearch.model.PublishedImage;

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
            return MongoDaoHelper.find(
                    MongoDaoHelper.createEqFilter("sampleRef", sampleRef),
                    null,
                    0,
                    -1,
                    mongoCollection,
                    PublishedImage.class
            );
        }
    }

    @Override
    public Map<String, List<PublishedImage>> getPublishedImagesBySampleObjectives(@Nullable String alignmentSpace,
                                                                                  Collection<String> sampleRefs,
                                                                                  @Nullable String objective) {
        if (CollectionUtils.isEmpty(sampleRefs)) {
            return Collections.emptyMap();
        } else {
            List<Bson> filters = new ArrayList<>();
            if (StringUtils.isNotBlank(alignmentSpace)) {
                filters.add(MongoDaoHelper.createEqFilter("alignmentSpace", alignmentSpace));
            }
            filters.add(Filters.in("sampleRef", sampleRefs));
            if (StringUtils.isNotBlank(objective)) {
                filters.add(MongoDaoHelper.createEqFilter("objective", objective));
            }
            List<PublishedImage> publishedImages = MongoDaoHelper.find(
                    MongoDaoHelper.createBsonFilterCriteria(filters),
                    null,
                    0,
                    -1,
                    mongoCollection,
                    PublishedImage.class
            );
            return publishedImages.stream().collect(Collectors.groupingBy(
                    PublishedImage::getSampleRef,
                    Collectors.toList()
            ));
        }
    }

    @Override
    public List<PublishedImage> getGal4ExpressionImages(Collection<String> originalLines,
                                                        @Nullable String anatomicalArea) {
        if (CollectionUtils.isEmpty(originalLines)) {
            return Collections.emptyList();
        } else {
            List<Bson> filters = new ArrayList<>();
            filters.add(Filters.in("originalLine", originalLines));
            if (StringUtils.isNotBlank(anatomicalArea)) {
                filters.add(MongoDaoHelper.createEqFilter("area", anatomicalArea));
            }
            filters.add(Filters.in("releaseName", GAL4_RELEASES));
            return MongoDaoHelper.find(
                    // these images must come from one of two releases, but we don't know
                    //  which a priori
                    MongoDaoHelper.createBsonFilterCriteria(filters),
                    null,
                    0,
                    -1,
                    mongoCollection,
                    PublishedImage.class
            );
        }
    }

    @Override
    public List<PublishedImage> getAllGal4ExpressionImagesForLine(String originalLine) {
        if (StringUtils.isBlank(originalLine)) {
            return Collections.emptyList();
        } else {
            return MongoDaoHelper.find(
                    // these images must come from one of two releases, but we don't know
                    //  which a priori
                    MongoDaoHelper.createBsonFilterCriteria(
                            Arrays.asList(
                                    MongoDaoHelper.createEqFilter("originalLine", originalLine),
                                    Filters.in("releaseName", GAL4_RELEASES)
                            )
                    ),
                    null,
                    0,
                    -1,
                    mongoCollection,
                    PublishedImage.class
            );
        }
    }

    @Override
    protected void createDocumentIndexes() {
        // do nothing here
    }
}
