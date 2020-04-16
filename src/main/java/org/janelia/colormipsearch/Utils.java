package org.janelia.colormipsearch;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class Utils {

    static <T> List<List<T>> partitionList(List<T> l, int partitionSize) {
        BiFunction<Pair<List<List<T>>, List<T>>, T, Pair<List<List<T>>, List<T>>> partitionAcumulator = (partitionResult, s) -> {
            List<T> currentPartition;
            if (partitionResult.getRight().size() == partitionSize) {
                currentPartition = new ArrayList<>();
            } else {
                currentPartition = partitionResult.getRight();
            }
            currentPartition.add(s);
            if (currentPartition.size() == 1) {
                partitionResult.getLeft().add(currentPartition);
            }
            return ImmutablePair.of(partitionResult.getLeft(), currentPartition);
        };
        return l.stream().reduce(
                ImmutablePair.of(new ArrayList<>(), new ArrayList<>()),
                partitionAcumulator,
                (r1, r2) -> r2.getLeft().stream().flatMap(p -> p.stream())
                        .map(s -> partitionAcumulator.apply(r1, s))
                        .reduce((first, second) -> second)
                        .orElse(r1)).getLeft()
                ;
    }

}
