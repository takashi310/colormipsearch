package org.janelia.colormipsearch.cmsdrivers;

import java.util.List;

import org.janelia.colormipsearch.tools.ColorMIPSearchResult;
import org.janelia.colormipsearch.tools.MIPInfo;

public interface ColorMIPSearchDriver {
    List<ColorMIPSearchResult> findAllColorDepthMatches(List<MIPInfo> maskMIPS, List<MIPInfo> libraryMIPS);
    void terminate();
}
