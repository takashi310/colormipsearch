package org.janelia.colormipsearch.cmd;

import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.model.PPPMatch;

public enum MatchResultTypes {
    CDS(CDMatch.class), // color depth search results
    PPP(PPPMatch.class); // PPP matches

    private Class<? extends AbstractMatch> matchType;

    MatchResultTypes(Class<? extends AbstractMatch> matchType) {
        this.matchType = matchType;
    }

    public String getMatchType() {
        return matchType.getName();
    }

}
