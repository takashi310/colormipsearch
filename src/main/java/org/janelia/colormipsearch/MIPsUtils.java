package org.janelia.colormipsearch;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.ZipFile;

import javax.annotation.Nullable;
import javax.imageio.ImageIO;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import ij.ImagePlus;
import ij.io.Opener;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.imageprocessing.ImageArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MIPsUtils {

    private enum ImageFormat {
        PNG,
        TIFF,
        UNKNOWN
    }

    private static final Logger LOG = LoggerFactory.getLogger(MIPsUtils.class);

    /**
     * Load a MIP image from its MIPInfo
     * @param mip
     * @return
     */
    @Nullable
    static MIPImage loadMIP(@Nullable MIPInfo mip) {
        long startTime = System.currentTimeMillis();
        if (mip == null) {
            return null;
        } else {
            LOG.trace("Load MIP {}", mip);
            InputStream inputStream;
            try {
                inputStream = mip.openInputStream();
                if (inputStream == null) {
                    return null;
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            ImagePlus ij = null;
            try {
                ij = readImagePlus(mip.getId(), getImageFormat(mip.getImagePath()), inputStream);
                return new MIPImage(mip, new ImageArray(ij));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            } finally {
                try {
                    inputStream.close();
                } catch (IOException ignore) {
                }
                if (ij != null) {
                    ij.close();
                }
                LOG.trace("Loaded MIP {} in {}ms", mip, System.currentTimeMillis() - startTime);
            }
        }
    }

    /**
     * TransformedMIP can be the corresponding gradient image or a ZGap image that has applied the dilation already.
     * The typical pattern is that the image file name is the same but the path to it has a certain suffix
     * such as '_gradient' or '_20pxRGBMAX'
     * @param mipInfo
     * @param transformedMIPLocation
     * @param transformationLookupSuffix
     * @return
     */
    @Nullable
    public static MIPInfo getTransformedMIPInfo(MIPInfo mipInfo, String transformedMIPLocation, String transformationLookupSuffix) {
        if (StringUtils.isBlank(transformedMIPLocation)) {
            return null;
        } else {
            Path transformedMIPPath = Paths.get(transformedMIPLocation);
            if (Files.isDirectory(transformedMIPPath)) {
                return getTransformedMIPInfoFromFilePath(transformedMIPPath, Paths.get(mipInfo.getImagePath()), transformationLookupSuffix);
            } else if (Files.isRegularFile(transformedMIPPath) && StringUtils.endsWithIgnoreCase(transformedMIPLocation, ".zip")) {
                return getTransformedMIPInfoFromZipEntry(transformedMIPLocation, mipInfo.getImagePath(), transformationLookupSuffix);
            } else {
                return null;
            }
        }
    }

    @Nullable
    private static MIPInfo getTransformedMIPInfoFromFilePath(Path transformedMIPPath, Path mipPath, String transformationLookupSuffix) {
        Path mipParentPath = mipPath.getParent();
        String mipFilenameWithoutExtension = StringUtils.replacePattern(mipPath.getFileName().toString(), "\\.tif(f)?$", "");
        List<Path> transformedMIPPaths;
        if (mipParentPath == null) {
            transformedMIPPaths = Arrays.asList(
                    transformedMIPPath.resolve(mipFilenameWithoutExtension + ".png"),
                    transformedMIPPath.resolve(mipFilenameWithoutExtension + ".tif")
            );
        } else {
            int nComponents = mipParentPath.getNameCount();
            transformedMIPPaths = IntStream.range(0, nComponents)
                    .map(i -> nComponents - i - 1)
                    .mapToObj(i -> mipParentPath.getName(i).toString() + transformationLookupSuffix)
                    .reduce(new ArrayList<String>(),
                            (a, e) -> {
                                if (a.isEmpty()) {
                                    a.add("");
                                    a.add(e);
                                } else {
                                    String lastElement = a.get(a.size() - 1);
                                    a.add(e + "/" + lastElement);
                                }
                                return a;
                            },
                            (a1, a2) -> {
                                a1.addAll(a2);
                                return a1;
                            })
                    .stream()
                    .flatMap(p -> Stream.of(
                            transformedMIPPath.resolve(p).resolve(mipFilenameWithoutExtension + ".png"),
                            transformedMIPPath.resolve(p).resolve(mipFilenameWithoutExtension + ".tif")))
                    .collect(Collectors.toList());
        }
        return transformedMIPPaths.stream()
                .filter(p -> Files.exists(p)).filter(p -> Files.isRegularFile(p))
                .findFirst()
                .map(Path::toString)
                .map(transformedMIPImagePathname -> {
                    MIPInfo transformedMIP = new MIPInfo();
                    transformedMIP.setCdmPath(transformedMIPImagePathname);
                    transformedMIP.setImagePath(transformedMIPImagePathname);
                    return transformedMIP;
                })
                .orElse(null);
    }

    public static List<MIPInfo> readMIPsFromLocalFiles(String mipsLocation, int offset, int length, Set<String> mipsFilter) {
        Path mipsInputPath = Paths.get(mipsLocation);
        if (Files.isDirectory(mipsInputPath)) {
            // read mips from the specified folder
            int from = offset > 0 ? offset : 0;
            try {
                List<MIPInfo> mips = Files.find(mipsInputPath, 1, (p, fa) -> fa.isRegularFile())
                        .filter(p -> isImageFile(p))
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
                            MIPInfo mipInfo = new MIPInfo();
                            mipInfo.setId(extIndex == -1 ? fname : fname.substring(0, extIndex));
                            mipInfo.setImagePath(mipsInputPath.toString());
                            return mipInfo;
                        })
                        .collect(Collectors.toList());
                if (length > 0 && length < mips.size()) {
                    return mips.subList(0, length);
                } else {
                    return mips;
                }
            } catch (IOException e) {
                LOG.error("Error reading content of {}", mipsLocation, e);
                return Collections.emptyList();
            }
        } else if (Files.isRegularFile(mipsInputPath)) {
            // check if the input is an archive (right now only zip is supported)
            if (StringUtils.endsWithIgnoreCase(mipsLocation, ".zip")) {
                // read mips from zip
                return readMIPsFromZipArchive(mipsLocation, mipsFilter, offset, length);
            } else if (isImageFile(mipsInputPath)) {
                // treat the file as a single image file
                String fname = mipsInputPath.getFileName().toString();
                int extIndex = fname.lastIndexOf('.');
                MIPInfo mipInfo = new MIPInfo();
                mipInfo.setId(extIndex == -1 ? fname : fname.substring(0, extIndex));
                mipInfo.setImagePath(mipsInputPath.toString());
                return Collections.singletonList(mipInfo);
            } else {
                return Collections.emptyList();
            }
        } else {
            LOG.warn("Cannot traverse links for {}", mipsLocation);
            return Collections.emptyList();
        }
    }

    public static List<MIPInfo> readMIPsFromJSON(String mipsJSONFilename, int offset, int length, Set<String> filter, ObjectMapper mapper) {
        try {
            LOG.info("Reading {}", mipsJSONFilename);
            List<MIPInfo> content = mapper.readValue(new File(mipsJSONFilename), new TypeReference<List<MIPInfo>>() {
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
    private static MIPInfo getTransformedMIPInfoFromZipEntry(String transformedMIPLocation, String mipEntryName, String transformationLookupSuffix) {
        String transformedMIPFilename = StringUtils.replacePattern(mipEntryName, "\\.tif(f)?$", ".png");
        Path transformedMIPEntryPath = Paths.get(transformedMIPFilename);
        int nComponents = transformedMIPEntryPath.getNameCount();
        String transformedMIPEntryName = IntStream.range(0, nComponents)
                .mapToObj(i -> i < nComponents-1 ? transformedMIPEntryPath.getName(i).toString() + transformationLookupSuffix : transformedMIPEntryPath.getName(i).toString())
                .reduce("", (p, pc) -> StringUtils.isBlank(p) ? pc : p + "/" + pc);
        MIPInfo transformedMIP = new MIPInfo();
        transformedMIP.setType("zipEntry");
        transformedMIP.setArchivePath(transformedMIPLocation);
        transformedMIP.setCdmPath(transformedMIPEntryName);
        transformedMIP.setImagePath(transformedMIPEntryName);
        return transformedMIP;
    }

    private static ImagePlus readImagePlus(String title, ImageFormat format, InputStream stream) throws Exception {
        switch (format) {
            case PNG:
                return readPngToImagePlus(title, stream);
            case TIFF:
                return readTiffToImagePlus(title, stream);
        }
        throw new IllegalArgumentException("Image must be in PNG or TIFF format");
    }

    private static ImageFormat getImageFormat(String filepath) {
        String lowerPath = filepath.toLowerCase();

        if (lowerPath.endsWith(".png")) {
            return ImageFormat.PNG;
        } else if (lowerPath.endsWith(".tiff") || lowerPath.endsWith(".tif")) {
            return ImageFormat.TIFF;
        }

        LOG.warn("Image format unknown: {}", filepath);
        return ImageFormat.UNKNOWN;
    }

    private static boolean isImageFile(Path p) {
        return isImageFile(p.getFileName().toString());
    }

    private static boolean isImageFile(String fname) {
        int extseparator = fname.lastIndexOf('.');
        if (extseparator == -1) {
            return false;
        }
        String fext = fname.substring(extseparator + 1);
        switch (fext.toLowerCase()) {
            case "jpg":
            case "jpeg":
            case "png":
            case "tif":
            case "tiff":
                return true;
            default:
                return false;
        }
    }

    private static List<MIPInfo> readMIPsFromZipArchive(String mipsArchive, Set<String> mipsFilter, int offset, int length) {
        ZipFile archiveFile;
        try {
            archiveFile = new ZipFile(mipsArchive);
        } catch (IOException e) {
            LOG.error("Error opening the archive stream for {}", mipsArchive, e);
            return Collections.emptyList();
        }
        try {
            int from = offset > 0 ? offset : 0;
            List<MIPInfo> mips = archiveFile.stream()
                    .filter(ze -> isImageFile(ze.getName()))
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
                        MIPInfo mipInfo = new MIPInfo();
                        mipInfo.setId(extIndex == -1 ? fname : fname.substring(0, extIndex));
                        mipInfo.setType("zipEntry");
                        mipInfo.setArchivePath(mipsArchive);
                        mipInfo.setCdmPath(ze.getName());
                        mipInfo.setImagePath(ze.getName());
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

    private static ImagePlus readPngToImagePlus(String title, InputStream stream) throws Exception {
        return new ImagePlus(title, ImageIO.read(stream));
    }

    private static ImagePlus readTiffToImagePlus(String title, InputStream stream) throws Exception {
        return new Opener().openTiff(stream, title);
    }

}
