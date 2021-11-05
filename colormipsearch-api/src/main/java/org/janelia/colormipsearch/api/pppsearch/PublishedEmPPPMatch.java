package org.janelia.colormipsearch.api.pppsearch;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * This is the published PPP match. It is very similar with the source PPP match
 * but without internal fields that we do not to want to expose.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "id", "publishedName", "libraryName",
        "pppRank", "pppScore",
        "slideCode", "anatomicalArea", "objective",
        "gender", "alignmentSpace", "mountingProtocol",
        "coverageScore", "aggregateCoverage", "mirrored",
        "files"
})
@JsonIgnoreProperties({
        "sampleName", "sourceImageFiles", "skeletonMatches"
})
public class PublishedEmPPPMatch extends EmPPPMatch {

    public static <S extends AbstractPPPMatch> PublishedEmPPPMatch createReleaseCopy(S pppMatch) {
        PublishedEmPPPMatch pppReleaseCopy = new PublishedEmPPPMatch();
        copyFrom(pppMatch, pppReleaseCopy);
        return pppReleaseCopy;
    }
}
