package org.janelia.colormipsearch.api.pppsearch;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.janelia.colormipsearch.api.Results;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PPPMatches extends Results<List<PPPMatch>> {

    private static class PPPID {
        private final String fullName;
        private final String name;
        private final String type;
        private final String instance;

        PPPID(String fullName, String name, String type, String instance) {
            this.fullName = fullName;
            this.name = name;
            this.type = type;
            this.instance = instance;
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

    public static List<PPPMatches> pppMatchesByNeurons(List<PPPMatch> pppMatchList) {
        if (CollectionUtils.isNotEmpty(pppMatchList)) {
            Comparator<PPPMatch> pppComparator = Comparator.comparingDouble(pppMatch -> Math.abs(pppMatch.getCoverageScore()));
            Comparator<PPPMatch> reversePPPComparator = pppComparator.reversed();
            return pppMatchList.stream()
                    .filter(PPPMatch::hasSkeletonMatches)
                    .collect(Collectors.groupingBy(
                            pppMatch -> new PPPID(
                                    pppMatch.getFullEmName(),
                                    pppMatch.getNeuronName(),
                                    pppMatch.getNeuronType(),
                                    pppMatch.getNeuronInstance()),
                            Collectors.collectingAndThen(
                                    Collectors.toList(),
                                    listOfPPPMatches -> {
                                        listOfPPPMatches.sort(reversePPPComparator);
                                        return listOfPPPMatches;
                                    })))
                    .entrySet().stream().map(e -> new PPPMatches(
                            e.getKey().fullName,
                            e.getKey().name,
                            e.getKey().type,
                            e.getKey().instance,
                            e.getValue()))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    public static PPPMatches pppMatchesBySingleNeuron(List<PPPMatch> pppMatchList) {
        List<PPPMatches> allPPPMatches = pppMatchesByNeurons(pppMatchList);
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

    PPPMatches(String fullName,
               String neuronName,
               String neuronType,
               String neuronInstance,
               List<PPPMatch> results) {
        super(results);
        this.fullName = fullName;
        this.neuronName = neuronName;
        this.neuronType = neuronType;
        this.neuronInstance = neuronInstance;
    }

    public String getFullName() {
        return fullName;
    }

    public String getNeuronName() {
        return neuronName;
    }

    public String getNeuronType() {
        return neuronType;
    }

    public String getNeuronInstance() {
        return neuronInstance;
    }
}
