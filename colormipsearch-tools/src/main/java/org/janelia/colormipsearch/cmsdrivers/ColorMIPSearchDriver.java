package org.janelia.colormipsearch.cmsdrivers;

import java.util.List;

import org.janelia.colormipsearch.api.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResult;

public interface ColorMIPSearchDriver {
    List<ColorMIPSearchResult> findAllColorDepthMatches(List<MIPMetadata> queryMIPS, List<MIPMetadata> targetMIPS);
    void terminate();
}
