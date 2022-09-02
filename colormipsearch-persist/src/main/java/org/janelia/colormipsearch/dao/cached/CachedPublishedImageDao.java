package org.janelia.colormipsearch.dao.cached;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.dao.EntityFieldValueHandler;
import org.janelia.colormipsearch.dao.PublishedImageDao;
import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.datarequests.PagedResult;
import org.janelia.colormipsearch.model.PublishedImage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedPublishedImageDao implements PublishedImageDao {
    private static final Logger LOG = LoggerFactory.getLogger(CachedPublishedImageDao.class);

    private final PublishedImageDao impl;
    private final LoadingCache<String, List<PublishedImage>> publishedImagesBySampleRefs;

    public CachedPublishedImageDao(PublishedImageDao impl, long maxSize) {
        this.impl = impl;
        this.publishedImagesBySampleRefs = CacheBuilder.newBuilder()
                .maximumSize(maxSize)
                .build(new CacheLoader<String, List<PublishedImage>>() {
                    @Override
                    public List<PublishedImage> load(String sampleRef) {
                        return retrievePublishedImagesBySampleRef(sampleRef);
                    }
                });
    }

    @Override
    public PublishedImage findByEntityId(Number id) {
        return impl.findByEntityId(id);
    }

    @Override
    public List<PublishedImage> findByEntityIds(Collection<Number> ids) {
        return impl.findByEntityIds(ids);
    }

    @Override
    public PagedResult<PublishedImage> findAll(Class<PublishedImage> type, PagedRequest pageRequest) {
        return impl.findAll(type, pageRequest);
    }

    @Override
    public long countAll() {
        return impl.countAll();
    }

    @Override
    public void save(PublishedImage entity) {
        impl.save(entity);
    }

    @Override
    public void saveAll(List<PublishedImage> entities) {
        impl.saveAll(entities);
    }

    @Override
    public PublishedImage update(Number entityId, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        return impl.update(entityId, fieldsToUpdate);
    }

    @Override
    public void delete(PublishedImage entity) {
        impl.delete(entity);
    }

    @Override
    public List<PublishedImage> getPublishedImagesBySample(String sampleRef) {
        try {
            return publishedImagesBySampleRefs.get(sampleRef);
        } catch (ExecutionException e) {
            LOG.error("Error loading published images for {}", sampleRef, e);
            return Collections.emptyList();
        }
    }

    @Override
    public Map<String, List<PublishedImage>> getPublishedImagesBySampleObjectives(@Nullable String alignmentSpace, Collection<String> sampleRefs, @Nullable String objective) {
        if (CollectionUtils.isEmpty(sampleRefs)) {
            return Collections.emptyMap();
        } else {
            return sampleRefs.stream()
                    .flatMap(sr -> getPublishedImagesBySample(sr).stream())
                    .filter(pi -> StringUtils.isBlank(alignmentSpace) || alignmentSpace.equals(pi.getAlignmentSpace()))
                    .filter(pi -> StringUtils.isBlank(objective) || objective.equals(pi.getObjective()))
                    .collect(Collectors.groupingBy(
                            PublishedImage::getSampleRef,
                            Collectors.toList()
                    ));
        }
    }

    private List<PublishedImage> retrievePublishedImagesBySampleRef(String sampleRef) {
        return impl.getPublishedImagesBySample(sampleRef);
    }

    @Override
    public List<PublishedImage> getGal4ExpressionImages(Collection<String> originalLines, @Nullable String anatomicalArea) {
        return null;
    }

    @Override
    public List<PublishedImage> getAllGal4ExpressionImagesForLine(String originalLine) {
        return null;
    }

    private List<PublishedImage> retrieveAllGal4ExpressionImagesForLine(String originalLine) {
        return impl.getAllGal4ExpressionImagesForLine(originalLine);
    }

}
