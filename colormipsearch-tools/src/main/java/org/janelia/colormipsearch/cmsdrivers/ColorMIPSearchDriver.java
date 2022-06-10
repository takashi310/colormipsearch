package org.janelia.colormipsearch.cmsdrivers;

import java.util.List;

import org.janelia.colormipsearch.api_v2.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api_v2.cdsearch.ColorMIPSearchResult;

public interface ColorMIPSearchDriver {
    List<ColorMIPSearchResult> findAllColorDepthMatches(List<MIPMetadata> queryMIPS, List<MIPMetadata> targetMIPS);
    void terminate();
}
