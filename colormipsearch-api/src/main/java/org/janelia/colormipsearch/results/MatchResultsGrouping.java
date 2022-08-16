package org.janelia.colormipsearch.results;

import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;

import org.janelia.colormipsearch.dto.AbstractMatchedTarget;
import org.janelia.colormipsearch.dto.AbstractNeuronMetadata;
import org.janelia.colormipsearch.dto.ResultMatches;
import org.janelia.colormipsearch.model.AbstractMatchEntity;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.FileType;
import org.janelia.colormipsearch.model.MatchComputeFileType;

public class MatchResultsGrouping {

    /**
     * The method groups persisted matched neurons by mask and returns the metadata fields only.
     *
     * @param matches persisted matches that need to be grouped by mask.
     * @param maskFieldSelectors mask fields used for grouping
     * @param ranking comparator used for ranking results
     * @param <R> persisted matches type
     * @param <M> grouping type
     * @param <R1> grouped result types
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <R extends AbstractMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>,
                   M extends AbstractNeuronMetadata,
                   R1 extends AbstractMatchedTarget<? extends AbstractNeuronMetadata>>
    List<ResultMatches<M, R1>> groupByMask(List<R> matches,
                                           List<Function<M, ?>> maskFieldSelectors,
                                           Comparator<R1> ranking) {
        return ItemsHandling.groupItems(
                matches,
                aMatch -> new GroupingCriteria<R1, M>(
                        (R1) aMatch.metadata(),
                        m -> {
                            AbstractNeuronEntity maskImage = aMatch.getMaskImage();
                            return (M) maskImage.metadata();
                        },
                        maskFieldSelectors
                ),
                GroupingCriteria::getItem,
                ranking,
                ResultMatches::new
        );
    }

    /**
     * The method groups persisted matched neurons by target and returns the metadata fields only.
     *
     * @param matches persisted matches that need to be grouped by mask.
     * @param targetFieldSelectors mask fields used for grouping
     * @param ranking comparator used for ranking results
     * @param <R> persisted matches type
     * @param <M> grouping type
     * @param <R1> grouped result types
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <R extends AbstractMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>,
                   M extends AbstractNeuronMetadata,
                   R1 extends AbstractMatchedTarget<? extends AbstractNeuronMetadata>>
    List<ResultMatches<M, R1>> groupByTarget(List<R> matches,
                                             List<Function<M, ?>> targetFieldSelectors,
                                             Comparator<R1> ranking) {
        return ItemsHandling.groupItems(
                matches,
                aMatch -> new GroupingCriteria<R1, M>(
                        (R1) aMatch.metadata(),
                        m -> {
                            AbstractNeuronEntity targetImage = aMatch.getMatchedImage();
                            return (M) targetImage.metadata();
                        },
                        targetFieldSelectors
                ),
                GroupingCriteria::getItem,
                ranking,
                ResultMatches::new
        );
    }

}
