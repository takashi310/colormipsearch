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
@JsonPropertyOrder({"maskPublishedName", "maskLibraryName", "neuronType", "neuronInstance", "results"})
public class SourcePPPMatches extends Results<List<SourcePPPMatch>> {

    private static class PPPID {
        private final String fullName;
        private final String name;
        private final String type;
        private final String instance;
        private final String dataset;

        PPPID(String fullName, String name, String type, String instance, String dataset) {
            this.fullName = fullName;
            this.name = name;
            this.type = type;
            this.instance = instance;
            this.dataset = dataset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            PPPID that = (PPPID) o;

            return new EqualsBuilder()
                    .append(fullName, that.fullName)
                    .append(name, that.name)
                    .append(type, that.type)
                    .append(instance, that.instance)
                    .append(dataset, that.dataset)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(fullName)
                    .append(name)
                    .append(type)
                    .append(instance)
                    .toHashCode();
        }
    }

    public static List<SourcePPPMatches> pppMatchesByNeurons(List<SourcePPPMatch> pppMatchList) {
        if (CollectionUtils.isNotEmpty(pppMatchList)) {
            Comparator<SourcePPPMatch> pppComparator = Comparator.comparingDouble(pppMatch -> Math.abs(pppMatch.getEmPPPRank()));
            return pppMatchList.stream()
                    .filter(SourcePPPMatch::hasSkeletonMatches)
                    .collect(Collectors.groupingBy(
                            pppMatch -> new PPPID(
                                    pppMatch.getSourceEmName(),
                                    pppMatch.getNeuronName(),
                                    pppMatch.getNeuronType(),
                                    pppMatch.getNeuronInstance(),
                                    pppMatch.getSourceEmDataset()),
                            Collectors.collectingAndThen(
                                    Collectors.toList(),
                                    listOfPPPMatches -> {
                                        listOfPPPMatches.sort(pppComparator);
                                        return listOfPPPMatches;
                                    })))
                    .entrySet().stream().map(e -> new SourcePPPMatches(
                            e.getKey().fullName,
                            e.getKey().name,
                            e.getKey().type,
                            e.getKey().instance,
                            e.getKey().dataset,
                            e.getValue()))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    public static SourcePPPMatches pppMatchesBySingleNeuron(List<SourcePPPMatch> pppMatchList) {
        List<SourcePPPMatches> allPPPMatches = pppMatchesByNeurons(pppMatchList);
        if (allPPPMatches.size() != 1) {
            throw new IllegalArgumentException("Expected all matches to be for the same neuron. Found " + allPPPMatches.size() + " neurons");
        } else {
            return allPPPMatches.get(0);
        }
    }

    private final String fullName;
    private final String neuronName;
    private final String neuronType;
    private final String neuronInstance;
    private final String neuronDataset;

    SourcePPPMatches(String fullName,
                     String neuronName,
                     String neuronType,
                     String neuronInstance,
                     String neuronDataset,
                     List<SourcePPPMatch> results) {
        super(results);
        this.fullName = fullName;
        this.neuronName = neuronName;
        this.neuronType = neuronType;
        this.neuronDataset = neuronDataset;
        this.neuronInstance = neuronInstance;
    }

    public String getFullName() {
        return fullName;
    }

    @JsonProperty("maskPublishedName")
    public String getNeuronName() {
        return neuronName;
    }

    public String getNeuronType() {
        return neuronType;
    }

    @JsonProperty("maskLibraryName")
    public String getNeuronDataset() {
        return neuronDataset;
    }

    public String getNeuronInstance() {
        return neuronInstance;
    }
}
