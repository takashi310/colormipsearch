package org.janelia.colormipsearch.cmsdrivers;

import java.util.List;

import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResult;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;

public interface ColorMIPSearchDriver {
    List<ColorMIPSearchResult> findAllColorDepthMatches(List<MIPMetadata> maskMIPS, List<MIPMetadata> libraryMIPS);
    void terminate();
}
