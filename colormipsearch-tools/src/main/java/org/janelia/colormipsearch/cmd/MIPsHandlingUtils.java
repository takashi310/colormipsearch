package org.janelia.colormipsearch.cmd;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
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

import javax.ws.rs.client.Client;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MIPsHandlingUtils {
    private static final Logger LOG = LoggerFactory.getLogger(MIPsHandlingUtils.class);
    private static final int MAX_IMAGE_DATA_DEPTH = 5;

    static boolean isEmLibrary(String lname) {
        return lname != null && StringUtils.containsIgnoreCase(lname, "flyem") &&
                (StringUtils.containsIgnoreCase(lname, "hemibrain") || StringUtils.containsIgnoreCase(lname, "vnc"));
    }

    enum MIPLibraryEntryType {
        file,
        zipEntry;

        static MIPLibraryEntryType fromStringValue(String value) {
            if (StringUtils.isBlank(value)) {
                return file;
            } else {
                return valueOf(value);
            }
        }
    }

    static Pair<MIPLibraryEntryType, Map<String, List<String>>> getLibraryImageFiles(String library, String libraryPath, String nameSuffixFilter) {
        if (isEmLibrary(library)) {
            return MIPsHandlingUtils.getImageFiles(MIPsHandlingUtils.emSkeletonRegexPattern(), libraryPath, nameSuffixFilter);
        } else {
            return MIPsHandlingUtils.getImageFiles(MIPsHandlingUtils.lmSlideCodeRegexPattern(), libraryPath, nameSuffixFilter);
        }
    }

    private static Pattern emSkeletonRegexPattern() {
        return Pattern.compile("([0-9]{5,})[_-].*");
    }

    private static Pattern lmSlideCodeRegexPattern() {
        return Pattern.compile("[-_](\\d\\d\\d\\d\\d\\d\\d\\d_[a-zA-Z0-9]+_[a-zA-Z0-9]+)([-_][mf])?[-_](.+[_-])ch?(\\d+)([_-]|(\\.))", Pattern.CASE_INSENSITIVE);
    }

    private static Pair<MIPLibraryEntryType, Map<String, List<String>>> getImageFiles(Pattern indexingFieldRegExPattern, String imagesBaseDir, String nameSuffixFilter) {
        if (StringUtils.isBlank(imagesBaseDir)) {
            return ImmutablePair.of(MIPLibraryEntryType.file, Collections.emptyMap());
        } else {
            Path imagesBasePath = Paths.get(imagesBaseDir);
            Function<String, String> indexingFieldFromName = n -> {
                Matcher m = indexingFieldRegExPattern.matcher(n);
                if (m.find()) {
                    return m.group(1);
                } else {
                    LOG.warn("Indexing field could not be extracted from {} - no match found using {}", n, indexingFieldRegExPattern);
                    return null;
                }
            };

            if (Files.isDirectory(imagesBasePath)) {
                return ImmutablePair.of(MIPLibraryEntryType.file, getImageFilesFromDir(indexingFieldFromName, imagesBasePath, nameSuffixFilter));
            } else if (Files.isRegularFile(imagesBasePath)) {
                return ImmutablePair.of(MIPLibraryEntryType.zipEntry, getImageFilesFromZip(indexingFieldFromName, imagesBasePath.toFile(), nameSuffixFilter));
            } else {
                return ImmutablePair.of(MIPLibraryEntryType.file, Collections.emptyMap());
            }
        }
    }

    private static Map<String, List<String>> getImageFilesFromDir(Function<String, String> indexingFieldFromName, Path baseDir, String nameSuffixFilter) {
        try {
            return Files.find(baseDir, MAX_IMAGE_DATA_DEPTH,
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
                    .collect(Collectors.groupingBy(indexingFieldFromName));
        } catch (IOException e) {
            LOG.warn("Error scanning {} for image files", baseDir, e);
            return Collections.emptyMap();
        }
    }

    private static Map<String, List<String>> getImageFilesFromZip(Function<String, String> indexingFieldFromName, File imagesFileArchive, String nameSuffixFilter) {
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
                    .collect(Collectors.groupingBy(indexingFieldFromName));
        } finally {
            try {
                imagesZipFile.close();
            } catch (IOException ignore) {
            }
        }
    }

    static List<ColorDepthMetadata> findSegmentedMIPs(ColorDepthMetadata cdmipMetadata,
                                                      String segmentedImagesBasePath,
                                                      Pair<MIPLibraryEntryType, Map<String, List<String>>> segmentedImages,
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
                if (segmentedCDMIPs.isEmpty()) {
                    LOG.debug("No segmentation found for {}", cdmipMetadata);
                }
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

    private static List<ColorDepthMetadata> lookupSegmentedImages(ColorDepthMetadata cdmipMetadata, String segmentedDataBasePath, MIPLibraryEntryType libraryEntryType, Map<String, List<String>> segmentedImages, int segmentedImageChannelBase) {
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
                int channelFromFN = extractColorChannelFromImageName(fn.replace(indexingField, ""), segmentedImageChannelBase);
                LOG.debug("Compare channel from {} ({}) with channel from {} ({})", cdmipMetadata.filepath, channelFromMip, fn, channelFromFN);
                String objectiveFromMip = cdmipMetadata.getObjective();
                String objectiveFromFN = extractObjectiveFromImageName(fn.replace(indexingField, ""));
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
                        segmentMIPMetadata.setImageType(libraryEntryType.name());
                        segmentMIPMetadata.segmentFilepath = p;
                        return segmentMIPMetadata;
                    })
                    .collect(Collectors.toList());
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

    static int extractColorChannelFromImageName(String imageName, int channelBase) {
        Pattern regExPattern = Pattern.compile("[_-]ch?(\\d+)([_-]|(\\.))", Pattern.CASE_INSENSITIVE);
        Matcher chMatcher = regExPattern.matcher(imageName);
        if (chMatcher.find()) {
            String channel = chMatcher.group(1);
            return Integer.parseInt(channel) - channelBase;
        } else {
            return -1;
        }
    }

    static String extractObjectiveFromImageName(String imageName) {
        Pattern regExPattern = Pattern.compile("[_-]([0-9]+x)[_-]", Pattern.CASE_INSENSITIVE);
        Matcher objectiveMatcher = regExPattern.matcher(imageName);
        if (objectiveMatcher.find()) {
            return objectiveMatcher.group(1);
        } else {
            return null;
        }
    }

    static String extractGenderFromImageName(String imageName) {
        // this assumes the gender is right before the objective
        Pattern regExPattern = Pattern.compile("(m|f)[_-]([0-9]+x)[_-]", Pattern.CASE_INSENSITIVE);
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

    static Map<String, String> retrieveLibraryNameMapping(Client httpClient, String configURL) {
        Response response = httpClient.target(configURL).request(MediaType.APPLICATION_JSON).get();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new IllegalStateException("Invalid response from " + configURL + " -> " + response);
        }
        Map<String, Object> configJSON = response.readEntity(new GenericType<>(new TypeReference<Map<String, Object>>() {}.getType()));
        Object configEntry = configJSON.get("config");
        if (!(configEntry instanceof Map)) {
            LOG.error("Config entry from {} is null or it's not a map", configJSON);
            throw new IllegalStateException("Config entry not found");
        }
        Map<String, String> cdmLibraryNamesMapping = new HashMap<>();
        Map<String, Map<String, Object>> configEntryMap = (Map<String, Map<String, Object>>)configEntry;
        configEntryMap.forEach((lid, ldata) -> {
            String lname = (String) ldata.get("name");
            cdmLibraryNamesMapping.put(lid, lname);
        });
        LOG.info("Using {} for mapping library names", cdmLibraryNamesMapping);
        return cdmLibraryNamesMapping;
    }

}
