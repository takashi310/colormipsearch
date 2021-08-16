package org.janelia.colormipsearch.api.cdsearch;

import java.io.Serializable;

public interface ColorDepthMatchScore extends Serializable {
    /**
     * Return the score value
     * @return
     */
    long getScore();

    boolean isBestScoreMirrored();
}
