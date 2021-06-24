package org.janelia.colormipsearch.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class Utils {
    static class PartitionSpliterator<T> implements Spliterator<List<T>> {
        private final Spliterator<T> sourceSpliterator;
        private final AtomicReference<List<T>> currentPartition;
        private final int partitionSize;

        PartitionSpliterator(Spliterator<T> sourceSpliterator,
                             AtomicReference<List<T>> currentPartition,
                             int partitionSize) {
            this.sourceSpliterator = sourceSpliterator;
            this.currentPartition = currentPartition;
            this.partitionSize = partitionSize;
        }

        @Override
        public boolean tryAdvance(Consumer<? super List<T>> action) {
            boolean hasNext = sourceSpliterator.tryAdvance((e) -> {
                List<T> l = currentPartition.getAndAccumulate(Collections.singletonList(e), (l1, l2) -> {
                    if (l1.size() == partitionSize) {
                        return l2;
                    } else {
                        List<T> updatedList = new ArrayList<>(l1);
                        updatedList.addAll(l2);
                        return updatedList;
                    }
                });
                if (l.size() == partitionSize) {
                    action.accept(l);
                }
            });
            if (hasNext) {
                return true;
            } else {
                List<T> currentContent = currentPartition.get();
                if (currentContent.size() > 0) {
                    action.accept(currentContent);
                }
                return false;
            }
        }

        @Override
        public PartitionSpliterator<T> trySplit() {
            Spliterator<T> newSourceSpliterator = sourceSpliterator.trySplit();
            return newSourceSpliterator == null
                    ? null
                    : new PartitionSpliterator<>(
                            newSourceSpliterator,
                            currentPartition,
                            partitionSize);
        }

        @Override
        public long estimateSize() {
//            long s = sourceSpliterator.estimateSize();
//            if (s < Long.MAX_VALUE) {
//                return s / partitionSize + (s % partitionSize == 0 ? 0 : 1);
//            } else {
                return Long.MAX_VALUE;
//            }
        }

        @Override
        public int characteristics() {
            return 0;
        }
    }

    public static <T> Stream<List<T>> partitionStream(Stream<T> stream, int partitionSize) {
        return StreamSupport.stream(() -> {
                    return new PartitionSpliterator<>(stream.spliterator(), new AtomicReference<>(new ArrayList<>()), partitionSize);
                },
                0,
                stream.isParallel())
                ;
    }

    public static <T> List<List<T>> partitionList(List<T> l, int partitionSize) {
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
                (r1, r2) -> r2.getLeft().stream().flatMap(Collection::stream)
                        .map(s -> partitionAcumulator.apply(r1, s))
                        .reduce((first, second) -> second)
                        .orElse(r1)).getLeft()
                ;
    }

    public static <T> List<ScoredEntry<List<T>>> pickBestMatches(List<T> l,
                                                                 Function<T, String> groupingCriteria,
                                                                 Function<T, Number> scoreExtractor,
                                                                 int topResults,
                                                                 int limitSubResults) {
        Comparator<T> csrComparison = Comparator.comparing(scoreExtractor.andThen(n -> n.doubleValue()));
        Map<String, List<T>> groupedResults = l.stream()
                .collect(Collectors.groupingBy(
                        val -> StringUtils.defaultIfBlank(groupingCriteria.apply(val), "UNKNOWN"),
                        Collectors.collectingAndThen(Collectors.toList(), r -> {
                            r.sort(csrComparison.reversed());
                            if (limitSubResults > 0 && limitSubResults < r.size()) {
                                return r.subList(0, limitSubResults);
                            } else {
                                return r;
                            }
                        })));
        List<ScoredEntry<List<T>>> bestResultsForSpecifiedCriteria = groupedResults.entrySet().stream()
                .map(e -> {
                    T maxValue = Collections.max(e.getValue(), csrComparison);
                    return new ScoredEntry<>(e.getKey(), scoreExtractor.apply(maxValue), e.getValue());
                })
                .sorted((se1, se2) -> Double.compare(se2.getScore().doubleValue(), se1.getScore().doubleValue())) // sort in reverse order
                .collect(Collectors.toList());
        if (topResults > 0 && bestResultsForSpecifiedCriteria.size() > topResults) {
            return bestResultsForSpecifiedCriteria.subList(0, topResults);
        } else {
            return bestResultsForSpecifiedCriteria;
        }
    }

}
