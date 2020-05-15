package org.janelia.colormipsearch.cmsdrivers;

import java.util.List;

import org.janelia.colormipsearch.ColorMIPSearchResult;
import org.janelia.colormipsearch.MIPInfo;

public interface ColorMIPSearchDriver {
    List<ColorMIPSearchResult> findAllColorDepthMatches(List<MIPInfo> maskMIPS, List<MIPInfo> libraryMIPS);
    void terminate();
}
