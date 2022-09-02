package org.janelia.colormipsearch.dao;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.janelia.colormipsearch.model.PublishedImage;

public interface PublishedImageDao extends Dao<PublishedImage> {
    /**
     * @param sampleRef sample reference
     * @return a list of all published images for the sample for all releases, all objectives and all areas.
     */
    List<PublishedImage> getPublishedImagesBySample(String sampleRef);

    /**
     * @param alignmentSpace optional alignment space
     * @param sampleRefs list of sample refs - if empty the result will automatically be empty
     * @param objective optional objective
     * @return a map of published images per sampleRef; there may be more than one published image based on objective
     *         or if alignmentSpace is not set by area
     */
    Map<String, List<PublishedImage>> getPublishedImagesBySampleObjectives(@Nullable String alignmentSpace,
                                                                           Collection<String> sampleRefs,
                                                                           @Nullable String objective);

    /**
     * @param originalLines
     * @param anatomicalArea
     * @return
     */
    List<PublishedImage> getGal4ExpressionImages(Collection<String> originalLines, @Nullable String anatomicalArea);

    /**
     * @param originalLine
     * @return a list with all GAL4 expression images for the specified original
     */
    List<PublishedImage> getAllGal4ExpressionImagesForLine(String originalLine);

}
