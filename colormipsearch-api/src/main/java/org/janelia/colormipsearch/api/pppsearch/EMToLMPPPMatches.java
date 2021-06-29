package org.janelia.colormipsearch.api.pppsearch;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.janelia.colormipsearch.api.Results;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"maskPublishedName", "results"})
public class EMToLMPPPMatches extends Results<List<PPPMatch>> {

    public static List<EMToLMPPPMatches> pppMatchesByNeurons(List<PPPMatch> pppMatchList) {
        if (CollectionUtils.isNotEmpty(pppMatchList)) {
            Comparator<PPPMatch> pppComparator = Comparator.comparingDouble(pppMatch -> Math.abs(pppMatch.getEmPPPRank()));
            return pppMatchList.stream()
                    .filter(PPPMatch::hasSkeletonMatches)
                    .collect(Collectors.groupingBy(
                            pppMatch -> pppMatch.getNeuronName(),
                            Collectors.collectingAndThen(
                                    Collectors.toList(),
                                    listOfPPPMatches -> {
                                        listOfPPPMatches.sort(pppComparator);
                                        return listOfPPPMatches;
                                    })))
                    .entrySet().stream().map(e -> new EMToLMPPPMatches(e.getKey(), e.getValue()))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private final String maskPublishedName;

    EMToLMPPPMatches(String maskPublishedName, List<PPPMatch> results) {
        super(results);
        this.maskPublishedName = maskPublishedName;
    }

    public String getMaskPublishedName() {
        return maskPublishedName;
    }
}
