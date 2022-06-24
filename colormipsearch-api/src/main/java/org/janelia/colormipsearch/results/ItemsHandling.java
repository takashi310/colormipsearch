package org.janelia.colormipsearch.results;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class ItemsHandling {

    /**
     * @param items               to be grouped by a certain criteria specified by @groupingCriteria@
     * @param toGroupingCriteria    criteria used for grouping @items@
     * @param fromGroupingCriteria extract element from grouping criteria
     * @param finalRankComparator comparator used for ranking grouped items
     * @param groupFactory        factory for creating an object to hold items from a group
     * @param <E>                 type of the elements that need to be grouped together
     * @param <K>                 type of the key used to group the elements
     * @param <G>                 result group type
     * @return
     */
    public static <E, K, G extends AbstractGroupedItems<E, K>>
    List<G> groupItems(List<E> items,
                       Function<E, GroupingCriteria<E, K>> toGroupingCriteria,
                       Function<GroupingCriteria<E, K>, E> fromGroupingCriteria,
                       Comparator<E> finalRankComparator,
                       Supplier<G> groupFactory) {
        if (CollectionUtils.isEmpty(items)) {
            return Collections.emptyList();
        } else {
            return new ArrayList<>(
                    items.stream()
                            .map(toGroupingCriteria)
                            .collect(Collectors.groupingBy(
                                    item -> item,
                                    Collectors.collectingAndThen(
                                            Collectors.toList(),
                                            sameKeyItems -> {
                                                G groupedItems = groupFactory.get();
                                                // get the key from the first item
                                                // the grouped items should always have at least one element
                                                groupedItems.setKey(sameKeyItems.get(0).getKey());
                                                List<E> groupElements = sameKeyItems.stream()
                                                        .map(fromGroupingCriteria)
                                                        .collect(Collectors.toList());
                                                if (finalRankComparator != null) {
                                                    groupElements.sort(finalRankComparator);
                                                }
                                                groupedItems.setItems(groupElements);
                                                return groupedItems;
                                            }
                                    )))
                            .values())
                    ;
        }
    }

    public static <T> Collection<List<T>> partitionCollection(Collection<T> l, int partitionSizeArg) {
        final AtomicInteger index = new AtomicInteger();
        int partitionSize = partitionSizeArg > 0 ? partitionSizeArg : 1;
        return l.stream()
                .collect(Collectors.groupingBy(docId -> index.getAndIncrement() / partitionSize)).values();
    }

    public static <T> void processPartitionStream(Stream<T> stream,
                                                  int partitionSize,
                                                  Consumer<List<T>> partitionHandler) {
        if (partitionSize == 1) {
            // trivial cause because the other one gets messed up
            // it's here only for completion purpose
            stream.map(Collections::singletonList).forEach(partitionHandler);
        } else {
            AtomicReference<List<T>> currentPartitionHolder = new AtomicReference<>(Collections.emptyList());
            Stream<List<T>> streamOfPartitions = stream
                    .flatMap(e -> {
                        List<T> l = currentPartitionHolder.accumulateAndGet(Collections.singletonList(e), (l1, l2) -> {
                            if (l1.size() == partitionSize) {
                                return l2;
                            } else {
                                List<T> updatedList = new ArrayList<>(l1);
                                updatedList.addAll(l2);
                                return updatedList;
                            }
                        });
                        return l.size() == partitionSize ? Stream.of(l) : Stream.empty();
                    });
            if (stream.isParallel()) {
                streamOfPartitions.parallel().forEach(partitionHandler);
            } else {
                streamOfPartitions.forEach(partitionHandler);
            }
            List<T> leftContent = currentPartitionHolder.get();
            if (leftContent.size() > 0 && leftContent.size() < partitionSize) {
                partitionHandler.accept(leftContent);
            }
        }
    }

    public static <T> List<ScoredEntry<List<T>>> selectTopRankedElements(List<T> l,
                                                                         Function<T, String> groupingCriteria,
                                                                         Function<T, Number> scoreExtractor,
                                                                         int topResults,
                                                                         int limitSubResults) {
        Comparator<T> csrComparison = Comparator.comparing(scoreExtractor.andThen(Number::doubleValue));
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
