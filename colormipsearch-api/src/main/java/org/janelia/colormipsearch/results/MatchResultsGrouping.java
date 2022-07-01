package org.janelia.colormipsearch.results;

import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;

import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.FileType;

public class MatchResultsGrouping {
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
    public static <M extends AbstractNeuronMetadata,
            T extends AbstractNeuronMetadata,
            R extends AbstractMatch<M, T>> List<ResultMatches<M, T, R>> groupByMaskFields(List<R> matches,
                                                                                          List<Function<M, ?>> maskFieldSelectors,
                                                                                          Comparator<R> ranking) {
        return ItemsHandling.groupItems(
                matches,
                aMatch -> new GroupingCriteria<R, M>(
                        (R) aMatch.duplicate((src, dest) -> {
                            dest.setMaskImage(src.getMaskImage());
                            dest.setMatchedImage(src.getMatchedImage());
                        }),
                        m -> {
                            M maskImage = m.getMaskImage().duplicate();
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
    public static <M extends AbstractNeuronMetadata,
            T extends AbstractNeuronMetadata,
            R extends AbstractMatch<M, T>,
            R1 extends AbstractMatch<T, M>> List<ResultMatches<T, M, R1>> groupByMatchedFields(List<R> matches,
                                                                                               List<Function<T, ?>> matchedFieldSelectors,
                                                                                               Comparator<R1> ranking) {
        return ItemsHandling.groupItems(
                matches,
                aMatch -> new GroupingCriteria<R1, T>(
                        (R1) aMatch.duplicate((src, dest) -> {
                            dest.setMaskImage(src.getMatchedImage());
                            dest.setMatchedImage(src.getMaskImage());
                            dest.setMatchFileData(FileType.ColorDepthMipInput,
                                    src.getMatchedImage().getNeuronFileData(FileType.ColorDepthMipInput));
                            dest.setMatchFileData(FileType.ColorDepthMipMatch,
                                    src.getMaskImage().getNeuronFileData(FileType.ColorDepthMipInput));
                            // set compute match files
                            dest.setMatchComputeFileData(ComputeFileType.InputColorDepthImage,
                                    src.getMatchedImage().getComputeFileData(ComputeFileType.InputColorDepthImage));
                            dest.setMatchComputeFileData(ComputeFileType.GradientImage,
                                    src.getMatchedImage().getComputeFileData(ComputeFileType.GradientImage));
                            dest.setMatchComputeFileData(ComputeFileType.ZGapImage,
                                    src.getMatchedImage().getComputeFileData(ComputeFileType.ZGapImage));
                            dest.setMatchComputeFileData(ComputeFileType.MatchedColorDepthImage,
                                    src.getMaskImage().getComputeFileData(ComputeFileType.InputColorDepthImage));
                            dest.setMatchComputeFileData(ComputeFileType.MatchedGradientImage,
                                    src.getMaskImage().getComputeFileData(ComputeFileType.GradientImage));
                            dest.setMatchComputeFileData(ComputeFileType.MatchedZGapImage,
                                    src.getMaskImage().getComputeFileData(ComputeFileType.ZGapImage));
                        }),
                        m -> {
                            T maskImage = m.getMaskImage().duplicate();
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

}
