package org.janelia.colormipsearch.api_v2.cdsearch;

import java.io.Serializable;

/**
 * @see org.janelia.colormipsearch.cds.ColorDepthMatchScore
 */
@Deprecated
public interface ColorDepthMatchScore extends Serializable {
    /**
     * Return the score value
     * @return
     */
    long getScore();

    /**
     * @return true if the best score comes from the mirrored mask
     */
    boolean isMirrored();
}
