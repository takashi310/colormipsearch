package org.janelia.colormipsearch.results;

import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.janelia.colormipsearch.model.AbstractMatchEntity;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.FileType;
import org.janelia.colormipsearch.model.MatchComputeFileType;

public class MatchResultsGrouping {

    /**
     * The method performs a simple non-optimized (in the sense that it does not remove redundant data) grouping
     * of the results by the specified mask field selectors.
     *
     * @param matches
     * @param maskKeyFieldSelectors
     * @param <M> mask type
     * @param <T> target type
     * @param <R> match result type
     * @return
     */
    public static <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity, R extends AbstractMatchEntity<M, T>>
    List<ResultMatches<M, T, R>> simpleGroupByMaskFields(List<R> matches, List<Function<M, ?>> maskKeyFieldSelectors) {
        return ItemsHandling.groupItems(
                matches,
                aMatch -> new GroupingCriteria<R, M>(
                        aMatch,
                        AbstractMatchEntity::getMaskImage,
                        maskKeyFieldSelectors
                ),
                GroupingCriteria::getItem,
                null, // no ranking
                ResultMatches::new
        );
    }

    /**
     * Group matches by mask.
     *
     * @param matches to be grouped
     * @param ranking matches sort criteria
     * @param <M>     mask type
     * @param <T>     target type
     * @param <R>     match type
     * @return a list of grouped matches by the mask neuron
     */
    @SuppressWarnings("unchecked")
    public static <M extends AbstractNeuronEntity,
                   T extends AbstractNeuronEntity,
                   R extends AbstractMatchEntity<M, T>> List<ResultMatches<M, T, R>> groupByMaskFields(List<R> matches,
                                                                                                       List<Function<M, ?>> maskFieldSelectors,
                                                                                                       Comparator<R> ranking) {
        return ItemsHandling.groupItems(
                matches,
                aMatch -> new GroupingCriteria<R, M>(
                        (R) aMatch.duplicate((AbstractMatchEntity<AbstractNeuronEntity, AbstractNeuronEntity> src, AbstractMatchEntity<AbstractNeuronEntity, AbstractNeuronEntity> dest) -> {
                            dest.setMaskImage(src.getMaskImage());
                            dest.setMatchedImage(src.getMatchedImage());
                            // set match files
                            dest.setMatchFileData(FileType.ColorDepthMipInput,
                                    src.getMaskImage().getNeuronFileData(FileType.ColorDepthMipInput));
                            dest.setMatchFileData(FileType.ColorDepthMipMatch,
                                    src.getMatchedImage().getNeuronFileData(FileType.ColorDepthMipInput));
                            // set compute match files
                            dest.setMatchComputeFileData(MatchComputeFileType.MaskColorDepthImage,
                                    src.getMaskImage().getComputeFileData(ComputeFileType.InputColorDepthImage));
                            dest.setMatchComputeFileData(MatchComputeFileType.MaskGradientImage,
                                    src.getMaskImage().getComputeFileData(ComputeFileType.GradientImage));
                            dest.setMatchComputeFileData(MatchComputeFileType.MaskZGapImage,
                                    src.getMaskImage().getComputeFileData(ComputeFileType.ZGapImage));
                        }),
                        m -> {
                            M maskImage = (M) m.getMaskImage().duplicate();
                            maskImage.resetComputeFileData(EnumSet.of(
                                    ComputeFileType.InputColorDepthImage,
                                    ComputeFileType.GradientImage,
                                    ComputeFileType.ZGapImage
                            ));
                            maskImage.resetNeuronFiles(EnumSet.of(
                                    FileType.ColorDepthMipInput
                            ));
                            return maskImage;
                        },
                        maskFieldSelectors
                ),
                g -> {
                    R aMatch = g.getItem();
                    aMatch.resetMaskImage();
                    return aMatch;
                },
                ranking,
                ResultMatches::new
        );
    }

    /**
     * Group matches by matched image.
     *
     * @param matches to be grouped
     * @param ranking sorting criteria
     * @param <M>     mask neuron type
     * @param <T>     target neuron type
     * @param <R>     type of the matches parameter
     * @param <R1>    type of the final matches
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <M extends AbstractNeuronEntity,
                   T extends AbstractNeuronEntity,
                   R extends AbstractMatchEntity<M, T>,
                   R1 extends AbstractMatchEntity<T, M>> List<ResultMatches<T, M, R1>> groupByTargetFields(List<R> matches,
                                                                                                           List<Function<T, ?>> matchedFieldSelectors,
                                                                                                           Comparator<R1> ranking) {
        return ItemsHandling.groupItems(
                matches,
                aMatch -> new GroupingCriteria<R1, T>(
                        (R1) aMatch.duplicate((src, dest) -> {
                            dest.setMaskImage(src.getMatchedImage());
                            dest.setMatchedImage(src.getMaskImage());
                            // set match files
                            dest.setMatchFileData(FileType.ColorDepthMipInput,
                                    src.getMatchedImage().getNeuronFileData(FileType.ColorDepthMipInput));
                            dest.setMatchFileData(FileType.ColorDepthMipMatch,
                                    src.getMaskImage().getNeuronFileData(FileType.ColorDepthMipInput));
                            // set compute match files
                            dest.setMatchComputeFileData(MatchComputeFileType.MaskColorDepthImage,
                                    src.getMatchedImage().getComputeFileData(ComputeFileType.InputColorDepthImage));
                            dest.setMatchComputeFileData(MatchComputeFileType.MaskGradientImage,
                                    src.getMatchedImage().getComputeFileData(ComputeFileType.GradientImage));
                            dest.setMatchComputeFileData(MatchComputeFileType.MaskZGapImage,
                                    src.getMatchedImage().getComputeFileData(ComputeFileType.ZGapImage));
                        }),
                        m -> {
                            T maskImage = (T) m.getMaskImage().duplicate();
                            maskImage.resetComputeFileData(EnumSet.of(
                                    ComputeFileType.InputColorDepthImage,
                                    ComputeFileType.GradientImage,
                                    ComputeFileType.ZGapImage
                            ));
                            maskImage.resetNeuronFiles(EnumSet.of(
                                    FileType.ColorDepthMipInput
                            ));
                            return maskImage;
                        },
                        matchedFieldSelectors
                ),
                g -> {
                    R1 aMatch = g.getItem();
                    aMatch.resetMaskImage();
                    return aMatch;
                },
                ranking,
                ResultMatches::new
        );
    }

    @SuppressWarnings("unchecked")
    public static <M extends AbstractNeuronEntity,
                   T extends AbstractNeuronEntity,
                   R extends AbstractMatchEntity<M, T>> List<R> expandResultsByMask(ResultMatches<M, T, R> matchesResults) {
        return matchesResults.getItems().stream()
                .map(persistedMatch -> {
                    return (R) persistedMatch.duplicate((src, dest) -> {
                        M maskImage = (M) matchesResults.getKey().duplicate();
                        T targetImage = persistedMatch.getMatchedImage();
                        maskImage.setComputeFileData(ComputeFileType.InputColorDepthImage,
                                persistedMatch.getMatchComputeFileData(MatchComputeFileType.MaskColorDepthImage));
                        maskImage.setComputeFileData(ComputeFileType.GradientImage,
                                persistedMatch.getMatchComputeFileData(MatchComputeFileType.MaskGradientImage));
                        maskImage.setComputeFileData(ComputeFileType.ZGapImage,
                                persistedMatch.getMatchComputeFileData(MatchComputeFileType.MaskZGapImage));
                        maskImage.setNeuronFileData(FileType.ColorDepthMipInput,
                                persistedMatch.getMatchFileData(FileType.ColorDepthMipInput));
                        dest.setMaskImage(maskImage);
                        dest.setMatchedImage(targetImage);
                        // no reason to keep these around
                        dest.resetMatchComputeFiles();
                        dest.resetMatchFiles();
                    });
                })
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    public static <M extends AbstractNeuronEntity,
                   T extends AbstractNeuronEntity,
                   R1 extends AbstractMatchEntity<T, M>,
                   R extends AbstractMatchEntity<M, T>> List<R> expandResultsByTarget(ResultMatches<T, M, R1> matchesResults) {
        return matchesResults.getItems().stream()
                .map(persistedMatch -> {
                    return (R) persistedMatch.duplicate((src, dest) -> {
                        T maskImage = (T) matchesResults.getKey().duplicate();
                        M targetImage = persistedMatch.getMatchedImage();
                        maskImage.setComputeFileData(ComputeFileType.InputColorDepthImage,
                                persistedMatch.getMatchComputeFileData(MatchComputeFileType.MaskColorDepthImage));
                        maskImage.setComputeFileData(ComputeFileType.GradientImage,
                                persistedMatch.getMatchComputeFileData(MatchComputeFileType.MaskGradientImage));
                        maskImage.setComputeFileData(ComputeFileType.ZGapImage,
                                persistedMatch.getMatchComputeFileData(MatchComputeFileType.MaskZGapImage));
                        maskImage.setNeuronFileData(FileType.ColorDepthMipInput,
                                persistedMatch.getMatchFileData(FileType.ColorDepthMipInput));
                        dest.setMaskImage(targetImage);
                        dest.setMatchedImage(maskImage);
                        // no reason to keep these around
                        dest.resetMatchComputeFiles();
                        dest.resetMatchFiles();
                    });
                })
                .collect(Collectors.toList());
    }

}
