package org.janelia.colormipsearch.api.pppsearch;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PPPUtils {
    private static final Logger LOG = LoggerFactory.getLogger(PPPUtils.class);

    public static List<PPPMatch> mergeMatches(List<PPPMatch> l1, List<PPPMatch> l2) {
        return Stream.concat(stream(l1), stream(l2))
                .collect(Collectors.groupingBy(
                        pppMatch -> ImmutablePair.of(pppMatch.getFullEmName(), pppMatch.getFullLmName()),
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                listOfPPPMatches -> listOfPPPMatches.stream()
                                        .reduce(PPPUtils::mergePPPMatch))))
                .values().stream()
                .map(om -> om.orElse(null))
                .filter(m -> m != null)
                .collect(Collectors.toList())
                ;
    }

    private static PPPMatch mergePPPMatch(PPPMatch m1, PPPMatch m2) {
        PPPMatch m = new PPPMatch();
        m.setFullEmName(m1.getFullEmName());
        m.setNeuronName(m1.getNeuronName());
        m.setNeuronType(m1.getNeuronType());
        m.setNeuronInstance(m1.getNeuronInstance());
        m.setFullLmName(m1.getFullLmName());
        m.setLineName(m1.getLineName());
        m.setSlideCode(m1.getSlideCode());
        m.setObjective(m1.getObjective());
        m.setAlignmentSpace(m1.getAlignmentSpace());
        m.setCoverageScore(m1.getCoverageScore());
        m.setAggregateCoverage(m1.getAggregateCoverage());
        m.setMirrored(m1.getMirrored());
        m.setSkeletonMatches(mergeSkeletonMatches(m1.getSkeletonMatches(), m2.getSkeletonMatches()));
        if (m1.hasEmPPPRank()) {
            m.setEmPPPRank(m1.getEmPPPRank());
        } else if (m2.hasEmPPPRank()) {
            m.setEmPPPRank(m2.getEmPPPRank());
        }
        return m;
    }

    private static List<SkeletonMatch> mergeSkeletonMatches(List<SkeletonMatch> l1, List<SkeletonMatch> l2) {
        return mergeSkeletonsStream(Stream.concat(stream(l1), stream(l2)));
    }

    private static List<SkeletonMatch> mergeSkeletonsStream(Stream<SkeletonMatch> matches) {
        return matches.collect(Collectors.groupingBy(SkeletonMatch::getId, Collectors.toList()))
                .values().stream()
                .map(e -> e.stream().max(Comparator.comparingDouble(SkeletonMatch::getNblastScore)).orElse(null))
                .filter(e -> e != null)
                .sorted(Comparator.comparingDouble(SkeletonMatch::getNblastScore).reversed())
                .collect(Collectors.toList());
    }

    private static <E> Stream<E> stream(List<E> l) {
        return CollectionUtils.isEmpty(l) ? Stream.empty() : l.stream();
    }

    public static void writePPPMatchesToJSONFile(PPPMatches pppMatches, File f, ObjectWriter objectWriter) {
        try {
            if (CollectionUtils.isNotEmpty(pppMatches.results)) {
                if (f == null) {
                    objectWriter.writeValue(System.out, pppMatches);
                } else {
                    LOG.info("Writing {}", f);
                    objectWriter.writeValue(f, pppMatches);
                }
            }
        } catch (IOException e) {
            LOG.error("Error writing CDS results to json file {}", f, e);
            throw new UncheckedIOException(e);
        }

    }

}
