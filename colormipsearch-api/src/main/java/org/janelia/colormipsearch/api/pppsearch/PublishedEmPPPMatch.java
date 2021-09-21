package org.janelia.colormipsearch.api.pppsearch;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
        "slideCode", "objective",
        "gender", "alignmentSpace", "mountingProtocol",
        "coverageScore", "aggregateCoverage", "mirrored",
        "files"
})
public class PublishedEmPPPMatch extends EmPPPMatch {

    public static <S extends AbstractPPPMatch> PublishedEmPPPMatch createReleaseCopy(S pppMatch) {
        PublishedEmPPPMatch pppReleaseCopy = new PublishedEmPPPMatch();
        copyFrom(pppMatch, pppReleaseCopy);
        return pppReleaseCopy;
    }

    @JsonIgnore
    @Override
    public Map<PPPScreenshotType, String> getSourceImageFiles() {
        return super.getSourceImageFiles();
    }

    @JsonIgnore
    @Override
    public List<SourceSkeletonMatch> getSkeletonMatches() {
        return super.getSkeletonMatches();
    }
}
