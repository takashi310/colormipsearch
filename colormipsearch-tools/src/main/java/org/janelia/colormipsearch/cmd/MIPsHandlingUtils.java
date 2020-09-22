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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MIPsHandlingUtils {
    private static final Logger LOG = LoggerFactory.getLogger(MIPsHandlingUtils.class);
    private static final int MAX_SEGMENTED_DATA_DEPTH = 5;

    static boolean isEmLibrary(String lname) {
        return lname != null && StringUtils.containsIgnoreCase(lname, "flyem") && StringUtils.containsIgnoreCase(lname, "hemibrain");
    }

    static Pair<String, Map<String, List<String>>> getLibrarySegmentedImages(String library, String librarySegmentationPath) {
        if (isEmLibrary(library)) {
            return MIPsHandlingUtils.getSegmentedImages(MIPsHandlingUtils.emSkeletonRegexPattern(), librarySegmentationPath);
        } else {
            return MIPsHandlingUtils.getSegmentedImages(MIPsHandlingUtils.lmSlideCodeRegexPattern(), librarySegmentationPath);
        }
    }

    private static Pattern emSkeletonRegexPattern() {
        return Pattern.compile("([0-9]{7,})[_-].*");
    }

    private static Pattern lmSlideCodeRegexPattern() {
        return Pattern.compile("[-_](\\d\\d\\d\\d\\d\\d\\d\\d_[a-zA-Z0-9]+_[a-zA-Z0-9]+)([-_][mf])?[-_](.+[_-])ch?(\\d+)[_-]", Pattern.CASE_INSENSITIVE);
    }

    private static Pair<String, Map<String, List<String>>> getSegmentedImages(Pattern indexingFieldRegExPattern, String segmentedMIPsBaseDir) {
        if (StringUtils.isBlank(segmentedMIPsBaseDir)) {
            return ImmutablePair.of("", Collections.emptyMap());
        } else {
            Path segmentdMIPsBasePath = Paths.get(segmentedMIPsBaseDir);
            Function<String, String> indexingFieldFromName = n -> {
                Matcher m = indexingFieldRegExPattern.matcher(n);
                if (m.find()) {
                    return m.group(1);
                } else {
                    LOG.warn("Indexing field could not be extracted from {} - no match found using {}", n, indexingFieldRegExPattern);
                    return null;
                }
            };

            if (Files.isDirectory(segmentdMIPsBasePath)) {
                return ImmutablePair.of("file", getSegmentedImagesFromDir(indexingFieldFromName, segmentdMIPsBasePath));
            } else if (Files.isRegularFile(segmentdMIPsBasePath)) {
                return ImmutablePair.of("zipEntry", getSegmentedImagesFromZip(indexingFieldFromName, segmentdMIPsBasePath.toFile()));
            } else {
                return ImmutablePair.of("file", Collections.emptyMap());
            }
        }
    }

    private static Map<String, List<String>> getSegmentedImagesFromDir(Function<String, String> indexingFieldFromName, Path segmentedMIPsBasePath) {
        try {
            return Files.find(segmentedMIPsBasePath, MAX_SEGMENTED_DATA_DEPTH,
                    (p, fa) -> fa.isRegularFile())
                    .map(p -> p.getFileName().toString())
                    .filter(entryName -> StringUtils.isNotBlank(indexingFieldFromName.apply(entryName)))
                    .collect(Collectors.groupingBy(indexingFieldFromName));
        } catch (IOException e) {
            LOG.warn("Error scanning {} for segmented images", segmentedMIPsBasePath, e);
            return Collections.emptyMap();
        }
    }

    private static Map<String, List<String>> getSegmentedImagesFromZip(Function<String, String> indexingFieldFromName, File segmentedMIPsFile) {
        ZipFile segmentedMIPsZipFile;
        try {
            segmentedMIPsZipFile = new ZipFile(segmentedMIPsFile);
        } catch (Exception e) {
            LOG.warn("Error opening segmented mips archive {}", segmentedMIPsFile, e);
            return Collections.emptyMap();
        }
        try {
            return segmentedMIPsZipFile.stream()
                    .filter(ze -> !ze.isDirectory())
                    .map(ZipEntry::getName)
                    .filter(entryName -> StringUtils.isNotBlank(indexingFieldFromName.apply(entryName)))
                    .collect(Collectors.groupingBy(indexingFieldFromName));
        } finally {
            try {
                segmentedMIPsZipFile.close();
            } catch (IOException ignore) {
            }
        }
    }

    static List<ColorDepthMetadata> findSegmentedMIPs(ColorDepthMetadata cdmipMetadata,
                                                      String segmentedImagesBasePath,
                                                      Pair<String, Map<String, List<String>>> segmentedImages,
                                                      int segmentedImageHandling,
                                                      int segmentedImageChannelBase) {
        if (StringUtils.isBlank(segmentedImagesBasePath)) {
            return Collections.singletonList(cdmipMetadata);
        } else {
            List<ColorDepthMetadata> segmentedCDMIPs = lookupSegmentedImages(cdmipMetadata, segmentedImagesBasePath, segmentedImages.getLeft(), segmentedImages.getRight(), segmentedImageChannelBase);
            if (segmentedImageHandling == 0x1) {
                // return the original MIP but only if a segmentation exists
                return segmentedCDMIPs.isEmpty() ? Collections.emptyList() : Collections.singletonList(cdmipMetadata);
            } else if (segmentedImageHandling == 0x2) {
                // return the segmentation if it exists
                return segmentedCDMIPs;
            } else if (segmentedImageHandling == 0x4) {
                // return both the segmentation and the original
                return Stream.concat(Stream.of(cdmipMetadata), segmentedCDMIPs.stream()).collect(Collectors.toList());
            } else {
                // lookup segmented images and return them if they exist otherwise return the original
                return segmentedCDMIPs.isEmpty() ? Collections.singletonList(cdmipMetadata) : segmentedCDMIPs;
            }
        }
    }

    private static List<ColorDepthMetadata> lookupSegmentedImages(ColorDepthMetadata cdmipMetadata, String segmentedDataBasePath, String type, Map<String, List<String>> segmentedImages, int segmentedImageChannelBase) {
        String indexingField;
        Predicate<String> segmentedImageMatcher;
        if (isEmLibrary(cdmipMetadata.getLibraryName())) {
            indexingField = cdmipMetadata.getPublishedName();
            Pattern emNeuronStateRegExPattern = Pattern.compile("[0-9]+[_-]([0-9A-Z]*)_.*", Pattern.CASE_INSENSITIVE);
            segmentedImageMatcher = p -> {
                String fn = RegExUtils.replacePattern(Paths.get(p).getFileName().toString(), "\\.\\D*$", "");
                Preconditions.checkArgument(fn.contains(indexingField));
                String cmFN = RegExUtils.replacePattern(Paths.get(cdmipMetadata.filepath).getFileName().toString(), "\\.\\D*$", "");
                String fnState = extractEMNeuronStateFromName(fn, emNeuronStateRegExPattern);
                String cmFNState = extractEMNeuronStateFromName(cmFN, emNeuronStateRegExPattern);
                return StringUtils.isBlank(fnState) && StringUtils.isBlank(cmFNState) ||
                        StringUtils.isNotBlank(cmFNState) && fnState.startsWith(cmFNState); // fnState may be LV or TC which is actually the same as L or T respectivelly so for now this check should work
            };
        } else {
            indexingField = cdmipMetadata.getSlideCode();
            segmentedImageMatcher = p -> {
                String fn = Paths.get(p).getFileName().toString();
                Preconditions.checkArgument(fn.contains(indexingField));
                int channelFromMip = getColorChannel(cdmipMetadata);
                int channelFromFN = extractColorChannelFromSegmentedImageName(fn.replace(indexingField, ""), segmentedImageChannelBase);
                LOG.debug("Compare channel from {} ({}) with channel from {} ({})", cdmipMetadata.filepath, channelFromMip, fn, channelFromFN);
                String objectiveFromMip = cdmipMetadata.getObjective();
                String objectiveFromFN = extractObjectiveFromSegmentedImageName(fn.replace(indexingField, ""));
                return matchMIPChannelWithSegmentedImageChannel(channelFromMip, channelFromFN) &&
                        matchMIPObjectiveWithSegmentedImageObjective(objectiveFromMip, objectiveFromFN);
            };
        }
        if (segmentedImages.get(indexingField) == null) {
            return Collections.emptyList();
        } else {
            return segmentedImages.get(indexingField).stream()
                    .filter(segmentedImageMatcher)
                    .map(p -> {
                        String sifn = Paths.get(p).getFileName().toString();
                        int scIndex = sifn.indexOf(indexingField);
                        Preconditions.checkArgument(scIndex != -1);
                        ColorDepthMetadata segmentMIPMetadata = new ColorDepthMetadata();
                        cdmipMetadata.copyTo(segmentMIPMetadata);
                        segmentMIPMetadata.segmentedDataBasePath = segmentedDataBasePath;
                        segmentMIPMetadata.setImageType(type);
                        segmentMIPMetadata.segmentFilepath = p;
                        return segmentMIPMetadata;
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

    private static int getColorChannel(ColorDepthMetadata cdMetadata) {
        String channel = cdMetadata.getChannel();
        if (StringUtils.isNotBlank(channel)) {
            return Integer.parseInt(channel) - 1; // mip channels are 1 based so make it 0 based
        } else {
            return -1;
        }
    }

    private static int extractColorChannelFromSegmentedImageName(String imageName, int channelBase) {
        Pattern regExPattern = Pattern.compile("[_-]ch?(\\d+)[_-]", Pattern.CASE_INSENSITIVE);
        Matcher chMatcher = regExPattern.matcher(imageName);
        if (chMatcher.find()) {
            String channel = chMatcher.group(1);
            return Integer.parseInt(channel) - channelBase;
        } else {
            return -1;
        }
    }

    private static String extractObjectiveFromSegmentedImageName(String imageName) {
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
            LOG.warn("No channel info found in the mip");
            return true;
        } else if (segmentImageChannel == -1) {
            LOG.warn("No channel info found in the segmented image");
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

}
