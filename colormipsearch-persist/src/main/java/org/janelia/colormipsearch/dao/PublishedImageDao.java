package org.janelia.colormipsearch.dao;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.janelia.colormipsearch.model.PublishedImage;

public interface PublishedImageDao extends Dao<PublishedImage> {
    /**
     * @param alignmentSpace optional alignment space
     * @param sampleRefs     list of sample refs - if empty the result will automatically be empty
     * @param objective      optional objective
     * @return a map of published images per sampleRef; there may be more than one published image based on objective
     * or if alignmentSpace is not set by area
     */
    Map<String, List<PublishedImage>> getPublishedImages(@Nullable String alignmentSpace,
                                                         Collection<String> sampleRefs,
                                                         @Nullable String objective);

    /**
     * @param originalLines     list of original lines - if empty the result will automatically be empty
     * @param area              optional anatomical area
     * @return a map of published images per original line
     */
    Map<String, List<PublishedImage>> getGAL4ExpressionImages(Collection<String> originalLines,
                                                              @Nullable String area);

    /**
     * @param sampleRef sample reference
     * @return a list of all published images for the sample for all releases, all objectives and all areas.
     */
    List<PublishedImage> getPublishedImagesWithGal4BySample(String sampleRef);

    /**
     * @param alignmentSpace optional alignment space
     * @param sampleRefs     list of sample refs - if empty the result will automatically be empty
     * @param objective      optional objective
     * @return a map of published images per sampleRef; there may be more than one published image based on objective
     * or if alignmentSpace is not set by area
     */
    Map<String, List<PublishedImage>> getPublishedImagesWithGal4BySampleObjectives(@Nullable String alignmentSpace,
                                                                                   Collection<String> sampleRefs,
                                                                                   @Nullable String objective);
}
