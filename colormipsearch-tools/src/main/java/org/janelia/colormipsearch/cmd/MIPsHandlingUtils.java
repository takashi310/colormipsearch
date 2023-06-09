package org.janelia.colormipsearch.cmd;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.cmd.jacsdata.ColorDepthMIP;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.FileData;
import org.janelia.colormipsearch.model.LMNeuronEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MIPsHandlingUtils {
    private static final Logger LOG = LoggerFactory.getLogger(MIPsHandlingUtils.class);

    static int extractColorChannelFromMIPName(String mipName, int channelBase) {
        Pattern regExPattern = Pattern.compile("[_-]ch?(\\d+)([_-]|(\\.))", Pattern.CASE_INSENSITIVE);
        Matcher chMatcher = regExPattern.matcher(mipName);
        if (chMatcher.find()) {
            String channel = chMatcher.group(1);
            return Integer.parseInt(channel) - channelBase;
        } else {
            return -1;
        }
    }

    static String extractEMBodyIdFromName(String name) {
        Matcher matcher = emSkeletonRegexPattern().matcher(name);
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return null;
        }
    }

    static String extractObjectiveFromMIPName(String mipName) {
        Pattern regExPattern = Pattern.compile("[_-]([0-9]+x)[_-]", Pattern.CASE_INSENSITIVE);
        Matcher objectiveMatcher = regExPattern.matcher(mipName);
        if (objectiveMatcher.find()) {
            return objectiveMatcher.group(1);
        } else {
            return null;
        }
    }

    static String extractGenderFromMIPName(String mipName) {
        // this assumes the gender is right before the objective
        Pattern regExPattern = Pattern.compile("(m|f)[_-]([0-9]+x)[_-]", Pattern.CASE_INSENSITIVE);
        Matcher objectiveMatcher = regExPattern.matcher(mipName);
        if (objectiveMatcher.find()) {
            return objectiveMatcher.group(1);
        } else {
            return null;
        }
    }

    static boolean isEmLibrary(String lname) {
        return lname != null && StringUtils.startsWithIgnoreCase(lname, "flyem");
    }

    static <N extends AbstractNeuronEntity> List<N> findNeuronMIPs(N neuronMetadata,
                                                                   String sourceObjective,
                                                                   int sourceChannel,
                                                                   String neuronImagesBasePath,
                                                                   Pair<FileData.FileDataType, Map<String, List<String>>> neuronImages,
                                                                   boolean includeOriginal,
                                                                   int neuronImageChannelBase) {
        if (StringUtils.isBlank(neuronImagesBasePath)) {
            return Collections.singletonList(originalAsInput(neuronMetadata));
        } else {
            List<N> segmentedCDMIPs = lookupSegmentedImages(
                    neuronMetadata,
                    sourceObjective,
                    sourceChannel,
                    neuronImagesBasePath,
                    neuronImages.getLeft(), neuronImages.getRight(), neuronImageChannelBase);
            return Stream.concat(
                            includeOriginal
                                ? Stream.of(originalAsInput(neuronMetadata)) // return both the segmentation and the original
                                : Stream.of(),
                            segmentedCDMIPs.stream())
                    .collect(Collectors.toList());
        }
    }

    static int getColorChannel(ColorDepthMIP cmip) {
        int channel = cmip.channelNumber();
        if (channel > 0) {
            return channel - 1; // mip channels are 1 based so make it 0 based
        } else {
            return -1;
        }
    }

    @SuppressWarnings("unchecked")
    private static <N extends AbstractNeuronEntity> List<N> lookupSegmentedImages(N neuronMetadata,
                                                                                  String sourceObjective,
                                                                                  int sourceChannel,
                                                                                  String inputDataBasePath,
                                                                                  FileData.FileDataType fileDataType,
                                                                                  Map<String, List<String>> computeInputImages,
                                                                                  int inputImageChannelBase) {
        Predicate<String> segmentedImageMatcher;
        String neuronIndexing = neuronMetadata.getNeuronId();
        if (isEmLibrary(neuronMetadata.getLibraryName())) {
            Pattern emNeuronStateRegExPattern = Pattern.compile("[0-9]+[_-]([0-9A-Z]*)_.*", Pattern.CASE_INSENSITIVE);
            segmentedImageMatcher = p -> {
                String fn = RegExUtils.replacePattern(Paths.get(p).getFileName().toString(), "\\.\\D*$", "");
                Preconditions.checkArgument(fn.contains(neuronIndexing));
                String cmFN = RegExUtils.replacePattern(Paths.get(neuronMetadata.getComputeFileName(ComputeFileType.SourceColorDepthImage)).getFileName().toString(), "\\.\\D*$", "");
                String fnState = extractEMNeuronStateFromName(fn, emNeuronStateRegExPattern);
                String cmFNState = extractEMNeuronStateFromName(cmFN, emNeuronStateRegExPattern);
                return StringUtils.isBlank(fnState) && StringUtils.isBlank(cmFNState) ||
                        StringUtils.isNotBlank(cmFNState) && fnState.startsWith(cmFNState); // fnState may be LV or TC which is actually the same as L or T respectivelly so for now this check should work
            };
        } else {
            segmentedImageMatcher = p -> {
                String fn = Paths.get(p).getFileName().toString();
                Preconditions.checkArgument(fn.contains(neuronIndexing));
                int channelFromFN = extractColorChannelFromMIPName(fn.replace(neuronIndexing, ""), inputImageChannelBase);
                LOG.debug("Compare channel from {} ({}) with channel from {} ({})",
                        neuronMetadata.getComputeFileData(ComputeFileType.SourceColorDepthImage), sourceChannel, fn, channelFromFN);
                String objectiveFromFN = extractObjectiveFromImageName(fn.replace(neuronIndexing, ""));
                if (channelFromFN == -1) {
                    LOG.info("No channel info found in segmentation MIP name: {}", fn);
                }
                return matchMIPChannelWithSegmentedImageChannel(sourceChannel, channelFromFN) &&
                        matchMIPObjectiveWithSegmentedImageObjective(sourceObjective, objectiveFromFN);
            };
        }
        if (computeInputImages.get(neuronIndexing) == null) {
            return Collections.emptyList();
        } else {
            return computeInputImages.get(neuronIndexing).stream()
                    .filter(segmentedImageMatcher)
                    .map(p -> {
                        String sifn = Paths.get(p).getFileName().toString();
                        int scIndex = sifn.indexOf(neuronIndexing);
                        Preconditions.checkArgument(scIndex != -1);
                        N segmentedNeuron = (N) neuronMetadata.duplicate();
                        segmentedNeuron.setComputeFileData(ComputeFileType.InputColorDepthImage, FileData.fromComponents(fileDataType, inputDataBasePath, p, true));
                        return segmentedNeuron;
                    })
                    .collect(Collectors.toList());
        }
    }

    private static String extractEMNeuronStateFromName(String name, Pattern emNeuronStatePattern) {
        Matcher matcher = emNeuronStatePattern.matcher(name);
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return "";
        }
    }

    private static String extractObjectiveFromImageName(String imageName) {
        Pattern regExPattern = Pattern.compile("[_-]([0-9]+x)[_-]", Pattern.CASE_INSENSITIVE);
        Matcher objectiveMatcher = regExPattern.matcher(imageName);
        if (objectiveMatcher.find()) {
            return objectiveMatcher.group(1);
        } else {
            return null;
        }
    }

    private static boolean matchMIPChannelWithSegmentedImageChannel(int mipChannel, int segmentImageChannel) {
        if (mipChannel == -1 && segmentImageChannel == -1) {
            return true;
        } else if (mipChannel == -1)  {
            return true;
        } else if (segmentImageChannel == -1) {
            return true;
        } else {
            return mipChannel == segmentImageChannel;
        }
    }

    private static boolean matchMIPObjectiveWithSegmentedImageObjective(String mipObjective, String segmentImageObjective) {
        if (StringUtils.isBlank(mipObjective) && StringUtils.isBlank(segmentImageObjective)) {
            return true;
        } else if (StringUtils.isBlank(mipObjective) )  {
            LOG.warn("No objective found in the mip");
            return false;
        } else if (StringUtils.isBlank(segmentImageObjective)) {
            // if the segmented image does not have objective match it against every image
            return true;
        } else {
            return StringUtils.equalsIgnoreCase(mipObjective, segmentImageObjective);
        }
    }

    @SuppressWarnings("unchecked")
    private static <N extends AbstractNeuronEntity> N originalAsInput(N neuronMetadata) {
        N segmentedNeuron = (N) neuronMetadata.duplicate();
        segmentedNeuron.setComputeFileData(
                ComputeFileType.InputColorDepthImage,
                FileData.fromString(neuronMetadata.getComputeFileName(ComputeFileType.SourceColorDepthImage)));
        return segmentedNeuron;
    }

    static Pair<FileData.FileDataType, Map<String, List<String>>> getLibraryImageFiles(String library, String libraryPath, String ignorePattern, String nameSuffixFilter) {
        if (isEmLibrary(library)) {
            return getImageFiles(emSkeletonRegexPattern(), libraryPath, ignorePattern, nameSuffixFilter);
        } else {
            return getImageFiles(lmSlideCodeRegexPattern(), libraryPath, ignorePattern, nameSuffixFilter);
        }
    }

    static Pair<FileData.FileDataType, List<String>> listLibraryImageFiles(String libraryPath, String ignorePattern, String nameSuffixFilter) {
        if (StringUtils.isBlank(libraryPath)) {
            return Pair.of(FileData.FileDataType.file, Collections.emptyList());
        } else {
            Path imagesBasePath = Paths.get(libraryPath);
            if (Files.isDirectory(imagesBasePath)) {
                return Pair.of(
                        FileData.FileDataType.file,
                        listImageFilesFromDir(imagesBasePath, ignorePattern, nameSuffixFilter));
            } else if (Files.isRegularFile(imagesBasePath)) {
                return Pair.of(
                        FileData.FileDataType.zipEntry,
                        listImageFilesFromZip(imagesBasePath.toFile(), ignorePattern, nameSuffixFilter));
            } else {
                return Pair.of(FileData.FileDataType.file, Collections.emptyList());
            }
        }
    }

    private static Pattern emSkeletonRegexPattern() {
        return Pattern.compile("([0-9]{5,})[_-].*");
    }

    private static Pattern lmSlideCodeRegexPattern() {
        return Pattern.compile("[-_](\\d\\d\\d\\d\\d\\d\\d\\d_[a-zA-Z0-9]+_[a-zA-Z0-9]+)([-_][mf])?[-_](.+[_-])ch?(\\d+)([_-]|(\\.))", Pattern.CASE_INSENSITIVE);
    }

    private static Pair<FileData.FileDataType, Map<String, List<String>>> getImageFiles(Pattern indexingFieldRegExPattern,
                                                                                        String imagesBaseDir,
                                                                                        String ignorePattern,
                                                                                        String nameSuffixFilter) {
        if (StringUtils.isBlank(imagesBaseDir)) {
            return ImmutablePair.of(FileData.FileDataType.file, Collections.emptyMap());
        } else {
            Path imagesBasePath = Paths.get(imagesBaseDir);
            Function<String, String> indexingFieldFromName = entryName -> {
                String n = Paths.get(entryName).getFileName().toString();
                Matcher m = indexingFieldRegExPattern.matcher(n);
                if (m.find()) {
                    return m.group(1);
                } else {
                    LOG.warn("Indexing field could not be extracted from {} - no match found using {}", n, indexingFieldRegExPattern);
                    return null;
                }
            };
            if (Files.isDirectory(imagesBasePath)) {
                return ImmutablePair.of(FileData.FileDataType.file, getImageFilesFromDir(indexingFieldFromName, imagesBasePath, ignorePattern, nameSuffixFilter));
            } else if (Files.isRegularFile(imagesBasePath)) {
                return ImmutablePair.of(FileData.FileDataType.zipEntry, getImageFilesFromZip(indexingFieldFromName, imagesBasePath.toFile(), ignorePattern, nameSuffixFilter));
            } else {
                return ImmutablePair.of(FileData.FileDataType.file, Collections.emptyMap());
            }
        }
    }

    private static Map<String, List<String>> getImageFilesFromDir(Function<String, String> indexingFieldFromName, Path baseDir, String ignorePattern, String nameSuffixFilter) {
        try {
            return Files.find(baseDir, 1,
                            (p, fa) -> fa.isRegularFile())
                    .map(p -> p.getFileName().toString())
                    .filter(entryName -> StringUtils.isNotBlank(indexingFieldFromName.apply(entryName)))
                    .filter(entryName -> {
                        if (StringUtils.isBlank(nameSuffixFilter)) {
                            return true;
                        } else {
                            String entryNameWithNoExt = RegExUtils.replacePattern(entryName, "\\.\\D*$", "");
                            return StringUtils.endsWithIgnoreCase(entryNameWithNoExt, nameSuffixFilter);
                        }
                    })
                    .filter(entryName -> {
                        if (StringUtils.isBlank(ignorePattern)) {
                            return true;
                        } else {
                            return !StringUtils.containsIgnoreCase(entryName, ignorePattern);
                        }
                    })
                    .collect(Collectors.groupingBy(indexingFieldFromName));
        } catch (IOException e) {
            LOG.warn("Error scanning {} for image files", baseDir, e);
            return Collections.emptyMap();
        }
    }

    private static Map<String, List<String>> getImageFilesFromZip(Function<String, String> indexingFieldFromName, File imagesFileArchive, String ignorePattern, String nameSuffixFilter) {
        ZipFile imagesZipFile;
        try {
            imagesZipFile = new ZipFile(imagesFileArchive);
        } catch (Exception e) {
            LOG.warn("Error opening image archive {}", imagesFileArchive, e);
            return Collections.emptyMap();
        }
        try {
            return imagesZipFile.stream()
                    .filter(ze -> !ze.isDirectory())
                    .map(ZipEntry::getName)
                    .filter(entryName -> StringUtils.isNotBlank(indexingFieldFromName.apply(entryName)))
                    .filter(entryName -> {
                        if (StringUtils.isBlank(nameSuffixFilter)) {
                            return true;
                        } else {
                            String entryNameWithNoExt = RegExUtils.replacePattern(entryName, "\\.\\D*$", "");
                            return StringUtils.endsWithIgnoreCase(entryNameWithNoExt, nameSuffixFilter);
                        }
                    })
                    .filter(entryName -> {
                        if (StringUtils.isBlank(ignorePattern)) {
                            return true;
                        } else {
                            return !StringUtils.containsIgnoreCase(entryName, ignorePattern);
                        }
                    })
                    .collect(Collectors.groupingBy(indexingFieldFromName));
        } finally {
            try {
                imagesZipFile.close();
            } catch (IOException ignore) {
            }
        }
    }

    private static List<String> listImageFilesFromDir(Path baseDir, String ignorePattern, String nameSuffixFilter) {
        try {
            return Files.find(baseDir, 1,
                            (p, fa) -> fa.isRegularFile())
                    .map(p -> p.getFileName().toString())
                    .filter(entryName -> {
                        if (StringUtils.isBlank(nameSuffixFilter)) {
                            return true;
                        } else {
                            String entryNameWithNoExt = RegExUtils.replacePattern(entryName, "\\.\\D*$", "");
                            return StringUtils.endsWithIgnoreCase(entryNameWithNoExt, nameSuffixFilter);
                        }
                    })
                    .filter(entryName -> {
                        if (StringUtils.isBlank(ignorePattern)) {
                            return true;
                        } else {
                            return !StringUtils.containsIgnoreCase(entryName, ignorePattern);
                        }
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            LOG.warn("Error scanning {} for image files", baseDir, e);
            return Collections.emptyList();
        }
    }

    private static List<String> listImageFilesFromZip(File imagesFileArchive, String ignorePattern, String nameSuffixFilter) {
        ZipFile imagesZipFile;
        try {
            imagesZipFile = new ZipFile(imagesFileArchive);
        } catch (Exception e) {
            LOG.warn("Error opening image archive {}", imagesFileArchive, e);
            return Collections.emptyList();
        }
        try {
            return imagesZipFile.stream()
                    .filter(ze -> !ze.isDirectory())
                    .map(ZipEntry::getName)
                    .filter(entryName -> {
                        if (StringUtils.isBlank(nameSuffixFilter)) {
                            return true;
                        } else {
                            String entryNameWithNoExt = RegExUtils.replacePattern(entryName, "\\.\\D*$", "");
                            return StringUtils.endsWithIgnoreCase(entryNameWithNoExt, nameSuffixFilter);
                        }
                    })
                    .filter(entryName -> {
                        if (StringUtils.isBlank(ignorePattern)) {
                            return true;
                        } else {
                            return !StringUtils.containsIgnoreCase(entryName, ignorePattern);
                        }
                    })
                    .collect(Collectors.toList());
        } finally {
            try {
                imagesZipFile.close();
            } catch (IOException ignore) {
            }
        }
    }

}
