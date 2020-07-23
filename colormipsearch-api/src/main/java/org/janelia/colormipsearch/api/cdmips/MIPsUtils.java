package org.janelia.colormipsearch.api.cdmips;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.annotation.Nullable;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.api.imageprocessing.ImageArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MIPsUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MIPsUtils.class);
    private static Map<String, Set<String>> ARCHIVE_ENTRIES_CACHE = new HashMap<>();

    /**
     * Load a MIP image from its MIPInfo
     * @param mip
     * @return
     */
    @Nullable
    public static MIPImage loadMIP(@Nullable MIPMetadata mip) {
        long startTime = System.currentTimeMillis();
        if (mip == null) {
            return null;
        } else {
            LOG.trace("Load MIP {}", mip);
            InputStream inputStream;
            try {
                inputStream = openInputStream(mip);
                if (inputStream == null) {
                    return null;
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            try {
                return new MIPImage(mip, ImageArrayUtils.readImageArray(mip.getId(), mip.getImageName(), inputStream));
            } catch (Exception e) {
                LOG.error("Error loading {}", mip, e);
                throw new IllegalStateException(e);
            } finally {
                try {
                    inputStream.close();
                } catch (IOException ignore) {
                }
                LOG.trace("Loaded MIP {} in {}ms", mip, System.currentTimeMillis() - startTime);
            }
        }
    }

    public static boolean exists(MIPMetadata mip) {
        if (StringUtils.equalsIgnoreCase("zipEntry", mip.getImageType())) {
            Path archiveFilePath = Paths.get(mip.getImageArchivePath());
            if (Files.isDirectory(archiveFilePath)) {
                return checkFSDir(archiveFilePath, mip);
            } else if (Files.isRegularFile(archiveFilePath)) {
                return checkZipEntry(archiveFilePath, mip);
            } else {
                return false;
            }
        } else {
            Path imageFilePath = Paths.get(mip.getImageName());
            if (Files.exists(imageFilePath)) {
                return imageFilePath.toFile().length() > 0L;
            } else if (StringUtils.isNotBlank(mip.getImageArchivePath())) {
                Path fullImageFilePath = Paths.get(mip.getImageArchivePath()).resolve(imageFilePath);
                return Files.exists(fullImageFilePath) && fullImageFilePath.toFile().length() > 0;
            } else {
                return false;
            }
        }
    }

    private static boolean checkFSDir(Path archiveFilePath, MIPMetadata mip) {
        return Files.exists(archiveFilePath.resolve(mip.getImageName()));
    }

    private static boolean checkZipEntry(Path archiveFilePath, MIPMetadata mip) {
        ZipFile archiveFile;
        try {
            archiveFile = new ZipFile(archiveFilePath.toFile());
        } catch (IOException e) {
            return false;
        }
        try {
            if (archiveFile.getEntry(mip.getImageName()) != null) {
                return true;
            } else {
                // slightly longer test
                LOG.warn("Full archive scan for {}", mip);
                String imageFn = Paths.get(mip.getImageName()).getFileName().toString();
                return archiveFile.stream()
                        .filter(ze -> !ze.isDirectory())
                        .map(ze -> Paths.get(ze.getName()).getFileName().toString())
                        .filter(fn -> imageFn.equals(fn))
                        .findFirst()
                        .map(fn -> true)
                        .orElse(false);
            }
        } finally {
            try {
                archiveFile.close();
            } catch (IOException ignore) {
            }
        }
    }

    @Nullable
    public static InputStream openInputStream(MIPMetadata mip) throws IOException {
        if (StringUtils.equalsIgnoreCase("zipEntry", mip.getImageType())) {
            Path archiveFilePath = Paths.get(mip.getImageArchivePath());
            if (Files.isDirectory(archiveFilePath)) {
                return openFileStream(archiveFilePath, mip);
            } else if (Files.isRegularFile(archiveFilePath)) {
                return openZipEntryStream(archiveFilePath, mip);
            } else {
                return null;
            }
        } else {
            Path imageFilePath = Paths.get(mip.getImageName());
            if (Files.exists(imageFilePath)) {
                return Files.newInputStream(imageFilePath);
            } else if (StringUtils.isNotBlank(mip.getImageArchivePath())) {
                Path archiveFilePath = Paths.get(mip.getImageArchivePath());
                if (Files.exists(archiveFilePath.resolve(imageFilePath))) {
                    return openFileStream(archiveFilePath, mip);
                } else {
                    return null;
                }
            } else {
                return null;
            }
        }
    }

    private static InputStream openFileStream(Path archiveFilePath, MIPMetadata mip) throws IOException {
        return Files.newInputStream(archiveFilePath.resolve(mip.getImageName()));
    }

    private static InputStream openZipEntryStream(Path archiveFilePath, MIPMetadata mip) throws IOException {
        ZipFile archiveFile = new ZipFile(archiveFilePath.toFile());
        ZipEntry ze = archiveFile.getEntry(mip.getImageName());
        if (ze != null) {
            return archiveFile.getInputStream(ze);
        } else {
            LOG.warn("Full archive scan for {}", mip);
            String imageFn = Paths.get(mip.getImageName()).getFileName().toString();
            return archiveFile.stream()
                    .filter(aze -> !aze.isDirectory())
                    .filter(aze -> imageFn.equals(Paths.get(aze.getName()).getFileName().toString()))
                    .findFirst()
                    .map(aze -> getEntryStream(archiveFile, aze))
                    .orElseGet(() -> {
                        try {
                            archiveFile.close();
                        } catch (IOException ignore) {
                        }
                        return null;
                    });
        }
    }

    private static InputStream getEntryStream(ZipFile archiveFile, ZipEntry zipEntry) {
        try {
            return archiveFile.getInputStream(zipEntry);
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * AncillaryMIP can be the corresponding gradient image or a ZGap image that has applied the dilation already.
     * The typical pattern is that the image file name is the same but the path to it has a certain suffix
     * such as '_gradient' or '_20pxRGBMAX'
     * @param mipInfo
     * @param ancillaryMIPLocation
     * @param ancillaryMIPSuffixMapping specifies how the mapping changes from the mipInfo to the ancillary mip
     * @return
     */
    @Nullable
    public static MIPMetadata getAncillaryMIPInfo(MIPMetadata mipInfo, String ancillaryMIPLocation, Function<String, String> ancillaryMIPSuffixMapping) {
        if (StringUtils.isBlank(ancillaryMIPLocation)) {
            return null;
        } else {
            Path ancillaryMIPPath = Paths.get(ancillaryMIPLocation);
            if (Files.isDirectory(ancillaryMIPPath)) {
                return getAncillaryMIPInfoFromFilePath(ancillaryMIPPath, Paths.get(mipInfo.getImageName()), ancillaryMIPSuffixMapping);
            } else if (Files.isRegularFile(ancillaryMIPPath) && StringUtils.endsWithIgnoreCase(ancillaryMIPLocation, ".zip")) {
                return getAncillaryMIPInfoFromZipEntry(ancillaryMIPLocation, mipInfo.getImageName(), ancillaryMIPSuffixMapping);
            } else {
                return null;
            }
        }
    }

    @Nullable
    private static MIPMetadata getAncillaryMIPInfoFromFilePath(Path ancillaryMIPPath, Path mipPath, Function<String, String> ancillaryMIPSuffixMapping) {
        Path mipParentPath = mipPath.getParent();
        String mipFilenameWithoutExtension = RegExUtils.replacePattern(mipPath.getFileName().toString(), "\\..*$", "");
        List<Path> ancillaryMIPPaths;
        if (mipParentPath == null) {
            ancillaryMIPPaths = Arrays.asList(
                    ancillaryMIPPath.resolve(mipFilenameWithoutExtension + ".png"),
                    ancillaryMIPPath.resolve(mipFilenameWithoutExtension + ".tif")
            );
        } else {
            int nComponents = mipParentPath.getNameCount();
            ancillaryMIPPaths = Stream.concat(
                    IntStream.range(0, nComponents)
                            .map(i -> nComponents - i - 1)
                            .mapToObj(i -> {
                                if (i > 0)
                                    return mipParentPath.subpath(0, i).resolve(ancillaryMIPSuffixMapping.apply(mipParentPath.getName(i).toString())).toString();
                                else
                                    return ancillaryMIPSuffixMapping.apply(mipParentPath.getName(i).toString());
                            }),
                    Stream.of(""))
                    .flatMap(p -> Stream.of(
                            ancillaryMIPPath.resolve(p).resolve(mipFilenameWithoutExtension + ".png"),
                            ancillaryMIPPath.resolve(p).resolve(mipFilenameWithoutExtension + ".tif")))
                    .collect(Collectors.toList());
        }
        return ancillaryMIPPaths.stream()
                .filter(p -> Files.exists(p)).filter(p -> Files.isRegularFile(p))
                .findFirst()
                .map(Path::toString)
                .map(ancillaryMIPImagePathname -> {
                    MIPMetadata ancillaryMIP = new MIPMetadata();
                    ancillaryMIP.setCdmPath(ancillaryMIPImagePathname);
                    ancillaryMIP.setImageName(ancillaryMIPImagePathname);
                    return ancillaryMIP;
                })
                .orElse(null);
    }

    public static List<MIPMetadata> readMIPsFromLocalFiles(String mipsLocation, int offset, int length, Set<String> mipsFilter) {
        Path mipsInputPath = Paths.get(mipsLocation);
        if (Files.isDirectory(mipsInputPath)) {
            return readMIPsFromDirectory(mipsInputPath, mipsFilter, offset, length);
        } else if (Files.isRegularFile(mipsInputPath)) {
            // check if the input is an archive (right now only zip is supported)
            if (StringUtils.endsWithIgnoreCase(mipsLocation, ".zip")) {
                // read mips from zip
                return readMIPsFromZipArchive(mipsLocation, mipsFilter, offset, length);
            } else if (ImageArrayUtils.isImageFile(mipsInputPath.getFileName().toString())) {
                // treat the file as a single image file
                String fname = mipsInputPath.getFileName().toString();
                int extIndex = fname.lastIndexOf('.');
                MIPMetadata mipInfo = new MIPMetadata();
                mipInfo.setId(extIndex == -1 ? fname : fname.substring(0, extIndex));
                mipInfo.setImageName(mipsInputPath.toString());
                return Collections.singletonList(mipInfo);
            } else {
                return Collections.emptyList();
            }
        } else {
            LOG.warn("Cannot traverse links for {}", mipsLocation);
            return Collections.emptyList();
        }
    }

    public static List<MIPMetadata> readMIPsFromJSON(String mipsJSONFilename, int offset, int length, Set<String> filter, ObjectMapper mapper) {
        try {
            LOG.info("Reading {}", mipsJSONFilename);
            List<MIPMetadata> content = mapper.readValue(new File(mipsJSONFilename), new TypeReference<List<MIPMetadata>>() {
            });
            if (CollectionUtils.isEmpty(filter)) {
                int from = offset > 0 ? offset : 0;
                int to = length > 0 ? Math.min(from + length, content.size()) : content.size();
                LOG.info("Read {} mips from {} starting at {} to {}", content.size(), mipsJSONFilename, from, to);
                return content.subList(from, to);
            } else {
                LOG.info("Read {} from {} mips", filter, content.size());
                return content.stream()
                        .filter(mip -> filter.contains(mip.getPublishedName().toLowerCase()) || filter.contains(StringUtils.lowerCase(mip.getId())))
                        .collect(Collectors.toList());
            }
        } catch (IOException e) {
            LOG.error("Error reading {}", mipsJSONFilename, e);
            throw new UncheckedIOException(e);
        }
    }

    @Nullable
    private static MIPMetadata getAncillaryMIPInfoFromZipEntry(String ancillaryMIPLocation, String mipEntryName, Function<String, String> ancillaryMIPSuffixMapping) {
        String ancillaryMIPLocationName = RegExUtils.replacePattern(Paths.get(ancillaryMIPLocation).getFileName().toString(), "\\..*$", "");
        Path mipEntryPath = Paths.get(mipEntryName);
        Path mipEntryParentPath = mipEntryPath.getParent();
        String mipEntryFilenameWithoutExtension = RegExUtils.replacePattern(mipEntryPath.getFileName().toString(), "\\..*$", "");
        List<String> ancillaryMIPEntryNames;
        if (mipEntryParentPath == null) {
            ancillaryMIPEntryNames = Arrays.asList(
                    mipEntryFilenameWithoutExtension + ".png",
                    mipEntryFilenameWithoutExtension + ".tif",
                    ancillaryMIPLocationName + "/" + mipEntryFilenameWithoutExtension + ".png",
                    ancillaryMIPLocationName + "/" + mipEntryFilenameWithoutExtension + ".tif"
            );
        } else {
            int nComponents = mipEntryParentPath.getNameCount();
            ancillaryMIPEntryNames = Stream.concat(
                    Stream.of(
                            ancillaryMIPLocationName,
                            ""
                    ),
                    IntStream.range(0, nComponents)
                            .map(i -> nComponents - i - 1)
                            .mapToObj(i -> {
                                if (i > 0)
                                    return mipEntryParentPath.subpath(0, i).resolve(ancillaryMIPSuffixMapping.apply(mipEntryParentPath.getName(i).toString())).toString();
                                else
                                    return ancillaryMIPSuffixMapping.apply(mipEntryParentPath.getName(i).toString());
                            }))
                    .map(p -> Paths.get(p))
                    .flatMap(p -> Stream.of(
                            p.resolve(mipEntryFilenameWithoutExtension + ".png"),
                            p.resolve(mipEntryFilenameWithoutExtension + ".tif")))
                    .map(p -> p.toString())
                    .collect(Collectors.toList());
        }
        Set<String> ancillaryMIPEntries = getZipEntryNames(ancillaryMIPLocation);
        return ancillaryMIPEntryNames.stream()
                .filter(en -> ancillaryMIPEntries.contains(en))
                .findFirst()
                .map(en -> {
                    MIPMetadata ancillaryMIP = new MIPMetadata();
                    ancillaryMIP.setImageType("zipEntry");
                    ancillaryMIP.setImageArchivePath(ancillaryMIPLocation);
                    ancillaryMIP.setCdmPath(ancillaryMIPLocation + ":" + en);
                    ancillaryMIP.setImageName(en);
                    return ancillaryMIP;
                })
                .orElse(null);
    }

    private static Set<String> getZipEntryNames(String zipFilename) {
        if (ARCHIVE_ENTRIES_CACHE.get(zipFilename) == null) {
            return cacheZipEntryNames(zipFilename);
        } else {
            return ARCHIVE_ENTRIES_CACHE.get(zipFilename);
        }
    }

    private static Set<String> cacheZipEntryNames(String zipFilename) {
        ZipFile archiveFile;
        try {
            archiveFile = new ZipFile(zipFilename);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try {
            Set<String> zipEntryNames = archiveFile.stream()
                    .filter(ze -> !ze.isDirectory())
                    .map(ze -> ze.getName())
                    .collect(Collectors.toSet());
            ARCHIVE_ENTRIES_CACHE.put(zipFilename, zipEntryNames);
            return zipEntryNames;
        } finally {
            try {
                archiveFile.close();
            } catch (IOException ignore) {
            }
        }
    }

    private static List<MIPMetadata> readMIPsFromDirectory(Path mipsInputDirectory, Set<String> mipsFilter, int offset, int length) {
        // read mips from the specified folder
        int from = offset > 0 ? offset : 0;
        try {
            List<MIPMetadata> mips = Files.find(mipsInputDirectory, 1, (p, fa) -> fa.isRegularFile())
                    .filter(p -> ImageArrayUtils.isImageFile(p.getFileName().toString()))
                    .filter(p -> {
                        if (CollectionUtils.isEmpty(mipsFilter)) {
                            return true;
                        } else {
                            String fname = p.getFileName().toString();
                            int separatorIndex = StringUtils.indexOf(fname, '_');
                            if (separatorIndex == -1) {
                                return true;
                            } else {
                                return mipsFilter.contains(StringUtils.substring(fname, 0, separatorIndex).toLowerCase());
                            }
                        }
                    })
                    .skip(from)
                    .map(p -> {
                        String fname = p.getFileName().toString();
                        int extIndex = fname.lastIndexOf('.');
                        MIPMetadata mipInfo = new MIPMetadata();
                        mipInfo.setId(extIndex == -1 ? fname : fname.substring(0, extIndex));
                        mipInfo.setImageName(p.toString());
                        return mipInfo;
                    })
                    .collect(Collectors.toList());
            if (length > 0 && length < mips.size()) {
                return mips.subList(0, length);
            } else {
                return mips;
            }
        } catch (IOException e) {
            LOG.error("Error reading content from {}", mipsInputDirectory, e);
            return Collections.emptyList();
        }
    }

    private static List<MIPMetadata> readMIPsFromZipArchive(String mipsArchive, Set<String> mipsFilter, int offset, int length) {
        ZipFile archiveFile;
        try {
            archiveFile = new ZipFile(mipsArchive);
        } catch (IOException e) {
            LOG.error("Error opening the archive stream for {}", mipsArchive, e);
            return Collections.emptyList();
        }
        try {
            int from = offset > 0 ? offset : 0;
            List<MIPMetadata> mips = archiveFile.stream()
                    .filter(ze -> ImageArrayUtils.isImageFile(ze.getName()))
                    .filter(ze -> {
                        if (CollectionUtils.isEmpty(mipsFilter)) {
                            return true;
                        } else {
                            String fname = Paths.get(ze.getName()).getFileName().toString();
                            int separatorIndex = StringUtils.indexOf(fname, '_');
                            if (separatorIndex == -1) {
                                return true;
                            } else {
                                return mipsFilter.contains(StringUtils.substring(fname, 0, separatorIndex).toLowerCase());
                            }
                        }
                    })
                    .skip(from)
                    .map(ze -> {
                        String fname = Paths.get(ze.getName()).getFileName().toString();
                        int extIndex = fname.lastIndexOf('.');
                        MIPMetadata mipInfo = new MIPMetadata();
                        mipInfo.setId(extIndex == -1 ? fname : fname.substring(0, extIndex));
                        mipInfo.setImageType("zipEntry");
                        mipInfo.setImageArchivePath(mipsArchive);
                        mipInfo.setCdmPath(ze.getName());
                        mipInfo.setImageName(ze.getName());
                        return mipInfo;
                    })
                    .collect(Collectors.toList());
            if (length > 0 && length < mips.size()) {
                return mips.subList(0, length);
            } else {
                return mips;
            }
        } finally {
            try {
                archiveFile.close();
            } catch (IOException ignore) {
            }
        }

    }

}
