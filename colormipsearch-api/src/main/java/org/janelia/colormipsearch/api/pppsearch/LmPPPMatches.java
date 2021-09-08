package org.janelia.colormipsearch.api.pppsearch;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.janelia.colormipsearch.api.Results;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"maskPublishedName", "maskLibraryName", "results"})
public class LmPPPMatches extends Results<List<LmPPPMatch>> {

    private static class LmPPPID {
        private final String name;
        private final String dataset;

        LmPPPID(String name, String dataset) {
            this.name = name;
            this.dataset = dataset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            LmPPPID that = (LmPPPID) o;

            return new EqualsBuilder()
                    .append(name, that.name)
                    .append(dataset, that.dataset)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(name)
                    .append(dataset)
                    .toHashCode();
        }
    }

    public static List<LmPPPMatches> pppMatchesByLines(List<LmPPPMatch> pppMatchList) {
        if (CollectionUtils.isNotEmpty(pppMatchList)) {
            Comparator<LmPPPMatch> pppComparator = Comparator.comparingDouble(pppMatch -> Math.abs(pppMatch.getEmPPPRank()));
            return pppMatchList.stream()
                    .filter(LmPPPMatch::hasSkeletonMatches)
                    .collect(Collectors.groupingBy(
                            pppMatch -> new LmPPPID(
                                    pppMatch.getLineName(),
                                    pppMatch.getSourceLmDataset()),
                            Collectors.collectingAndThen(
                                    Collectors.toList(),
                                    listOfPPPMatches -> {
                                        listOfPPPMatches.sort(pppComparator);
                                        return listOfPPPMatches;
                                    })))
                    .entrySet().stream().map(e -> new LmPPPMatches(
                            e.getKey().name,
                            e.getKey().dataset,
                            e.getValue()))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private final String lineName;
    private final String lmDataset;

    LmPPPMatches(String lineName,
                 String lmDataset,
                 List<LmPPPMatch> results) {
        super(results);
        this.lineName = lineName;
        this.lmDataset = lmDataset;
    }

    @JsonProperty("maskPublishedName")
    public String getLineName() {
        return lineName;
    }

    @JsonProperty("maskLibraryName")
    public String getLmDataset() {
        return lmDataset;
    }

}
