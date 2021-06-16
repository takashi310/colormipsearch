package org.janelia.colormipsearch.api.pppsearch;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.CollectionUtils;

public class PPPUtils {
    public static List<SkeletonMatch> mergeSkeletonMatches(List<SkeletonMatch> l1, List<SkeletonMatch> l2) {
        if (CollectionUtils.isEmpty(l1) && CollectionUtils.isEmpty(l2)) {
            return Collections.emptyList();
        } else {
            return dedupAndSort(Stream.concat(stream(l1), stream(l2)));
        }
    }

    public List<SkeletonMatch> sortedMatches(List<SkeletonMatch> l) {
        return dedupAndSort(stream(l));
    }

    private static List<SkeletonMatch> dedupAndSort(Stream<SkeletonMatch> matches) {
        return matches.collect(Collectors.groupingBy(SkeletonMatch::getId, Collectors.toList()))
                .values().stream()
                .map(e -> e.stream().max(Comparator.comparingDouble(SkeletonMatch::getNblastScore)).orElse(null))
                .filter(e -> e != null)
                .sorted(Comparator.comparingDouble(SkeletonMatch::getNblastScore))
                .collect(Collectors.toList());
    }

    private static Stream<SkeletonMatch> stream(List<SkeletonMatch> l) {
        return CollectionUtils.isEmpty(l) ? Stream.empty() : l.stream();
    }
}
