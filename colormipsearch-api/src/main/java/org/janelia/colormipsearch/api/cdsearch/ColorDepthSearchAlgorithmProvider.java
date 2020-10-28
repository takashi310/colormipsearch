package org.janelia.colormipsearch.api.cdsearch;

import java.io.Serializable;

import org.janelia.colormipsearch.api.imageprocessing.ImageArray;

/**
 * Creator of a ColorMIPQuerySearch for a given mask that generates a certain score type.
 *
 * @param <S> color depth match score type
 */
public interface ColorDepthSearchAlgorithmProvider<S extends ColorDepthMatchScore> extends Serializable {
    /**
     * @return default color depth search parameters.
     */
    ColorDepthSearchParams getDefaultCDSParams();

    /**
     * This method is essentially a constructor for a ColorMIPQuerySearch
     * for the given query
     *
     * @param queryImage encapsulated query image
     * @param queryThreshold query image threshold
     * @param cdsParams color depth search parameters encapsulated in the algorithm. These could be mask specific
     *                  or global parameters
     * @return a color depth query search instance for the given query
     */
    ColorDepthSearchAlgorithm<S> createColorDepthQuerySearchAlgorithm(ImageArray<?> queryImage,
                                                                      int queryThreshold,
                                                                      ColorDepthSearchParams cdsParams);

    default ColorDepthSearchAlgorithm<S> createColorDepthQuerySearchAlgorithmWithDefaultParams(ImageArray<?> queryImage, int queryThreshold) {
        return createColorDepthQuerySearchAlgorithm(queryImage, queryThreshold, new ColorDepthSearchParams());
    }

}
