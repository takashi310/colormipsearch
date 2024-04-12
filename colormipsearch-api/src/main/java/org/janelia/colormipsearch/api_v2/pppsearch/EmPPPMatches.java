package org.janelia.colormipsearch.api_v2.pppsearch;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.janelia.colormipsearch.api_v2.Results;

@Deprecated
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"maskId", "maskPublishedName", "maskLibraryName", "neuronType", "neuronInstance", "sourceMaskName", "results"})
public class EmPPPMatches extends Results<List<EmPPPMatch>> {

    private static class EmPPPID {
        private final String fullName;
        private final String id;
        private final String name;
        private final String type;
        private final String instance;
        private final String dataset;

        EmPPPID(String fullName, String id ,String name, String type, String instance, String dataset) {
            this.fullName = fullName;
            this.id = id;
            this.name = name;
            this.type = type;
            this.instance = instance;
            this.dataset = dataset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            EmPPPID that = (EmPPPID) o;

            return new EqualsBuilder()
                    .append(fullName, that.fullName)
                    .append(id, that.id)
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
                    .append(id)
                    .append(name)
                    .append(type)
                    .append(instance)
                    .toHashCode();
        }
    }

    private static EmPPPMatches EMPTY = new EmPPPMatches(
            null,
            null,
            null,
            null,
            null,
            null,
            Collections.emptyList());

    @JsonCreator
    public static EmPPPMatches createEmPPPMatches(@JsonProperty("maskId") String maskId,
                                                  @JsonProperty("maskPublishedName") String maskPublishedName,
                                                  @JsonProperty("maskLibraryName") String maskLibraryName,
                                                  @JsonProperty("neuronType") String neuronType,
                                                  @JsonProperty("neuronInstance") String neuronInstance,
                                                  @JsonProperty("results") List<EmPPPMatch> results) {
        results.forEach(pppMatch -> {
            pppMatch.setNeuronId(maskId);
            pppMatch.setNeuronName(maskPublishedName);
            pppMatch.setNeuronType(neuronType);
            pppMatch.setNeuronInstance(neuronInstance);
            pppMatch.setSourceEmDataset(maskLibraryName);
        });
        return new EmPPPMatches(
                null,
                maskId,
                maskPublishedName,
                neuronType,
                neuronInstance,
                maskLibraryName,
                results);
    }

    public static EmPPPMatches pppMatchesBySingleNeuron(List<EmPPPMatch> pppMatchList) {
        List<EmPPPMatches> allPPPMatches = pppMatchesByNeurons(pppMatchList);
        if (allPPPMatches.size() > 1) {
            throw new IllegalArgumentException("Expected all matches to be for the same neuron. Found " + allPPPMatches.size() + " neurons");
        } else {
            return allPPPMatches.size() > 0 ? allPPPMatches.get(0) : EmPPPMatches.EMPTY;
        }
    }

    private static List<EmPPPMatches> pppMatchesByNeurons(List<EmPPPMatch> pppMatchList) {
        if (CollectionUtils.isNotEmpty(pppMatchList)) {
            Comparator<EmPPPMatch> pppComparator = Comparator.comparingDouble(pppMatch -> Math.abs(pppMatch.getEmPPPRank()));
            return pppMatchList.stream()
                    .filter(EmPPPMatch::hasSkeletonMatches)
                    .collect(Collectors.groupingBy(
                            pppMatch -> new EmPPPID(
                                    pppMatch.getSourceEmName(),
                                    pppMatch.getNeuronId(),
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
                    .entrySet().stream().map(e -> new EmPPPMatches(
                            e.getKey().fullName,
                            e.getKey().id,
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

    private final String sourceNeuronName;
    private final String neuronId;
    private final String neuronName;
    private final String neuronType;
    private final String neuronInstance;
    private final String neuronDataset;

    EmPPPMatches(String sourceNeuronName,
                 String neuronId,
                 String neuronName,
                 String neuronType,
                 String neuronInstance,
                 String neuronDataset,
                 List<EmPPPMatch> results) {
        super(results);
        this.sourceNeuronName = sourceNeuronName;
        this.neuronId = neuronId;
        this.neuronName = neuronName;
        this.neuronType = neuronType;
        this.neuronDataset = neuronDataset;
        this.neuronInstance = neuronInstance;
    }

    @JsonProperty("sourceMaskName")
    public String getSourceNeuronName() {
        return sourceNeuronName;
    }

    @JsonProperty("maskId")
    public String getNeuronId() {
        return neuronId;
    }

    @JsonProperty("maskPublishedName")
    public String getNeuronName() {
        return neuronName;
    }

    @JsonProperty
    public String getNeuronType() {
        return neuronType;
    }

    @JsonProperty("maskLibraryName")
    public String getNeuronDataset() {
        return neuronDataset;
    }

    @JsonProperty
    public String getNeuronInstance() {
        return neuronInstance;
    }
}
