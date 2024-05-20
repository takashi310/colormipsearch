package org.janelia.colormipsearch.mips;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.model.FileData;

public class FileDataUtils {

    private static final Map<Path, Map<String, List<String>>> ARCHIVE_ENTRIES_CACHE = new HashMap<>();

    public static FileData lookupVariantFileData(String cdInputName, String sourceCDMName, List<String> variantLocations, String variantSuffix, Function<String, String> variantSuffixMapping) {
        if (CollectionUtils.isEmpty(variantLocations)) {
            return null;
        } else {
            return variantLocations.stream()
                    .filter(StringUtils::isNotBlank)
                    .map(Paths::get)
                    .map(variantPath -> {
                        if (Files.isDirectory(variantPath)) {
                            return lookupVariantFileDataInDir(cdInputName, sourceCDMName, variantPath, variantSuffix, variantSuffixMapping);
                        } else if (Files.isRegularFile(variantPath)) {
                            return lookupVariantFileDataInArchive(cdInputName, variantPath, variantSuffix);
                        } else {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElse(null);
        }
    }

    private static FileData lookupVariantFileDataInDir(String cdInputName,
                                                       String sourceCDMName,
                                                       Path variantPath,
                                                       String variantSuffix,
                                                       Function<String, String> variantSuffixMapping) {
        Path cdInputPath = Paths.get(cdInputName);
        Path cdInputParentPath = cdInputPath.getParent();
        String cdInputNameWithoutExtension = RegExUtils.replacePattern(cdInputPath.getFileName().toString(), "\\..*$", "");
        List<Path> candidateVariantPaths;
        if (cdInputParentPath == null) {
            String sourceMIPNameWithoutExtension = RegExUtils.replacePattern(sourceCDMName, "\\..*$", "");
            candidateVariantPaths = Arrays.asList(
                    variantPath.resolve(createMIPName(cdInputNameWithoutExtension, variantSuffix, ".png")),
                    variantPath.resolve(createMIPName(cdInputNameWithoutExtension, variantSuffix, ".tif")),
                    variantPath.resolve(createMIPName(variantSuffixMapping.apply(sourceMIPNameWithoutExtension), variantSuffix, ".png")), // search variant based on the transformation of the original mip
                    variantPath.resolve(createMIPName(variantSuffixMapping.apply(sourceMIPNameWithoutExtension), variantSuffix, ".tiff"))
            );
        } else {
            int nComponents = cdInputParentPath.getNameCount();
            candidateVariantPaths = Stream.concat(
                            IntStream.range(0, nComponents)
                                    .map(i -> nComponents - i - 1)
                                    .mapToObj(i -> {
                                        if (i > 0)
                                            return cdInputParentPath.subpath(0, i).resolve(variantSuffixMapping.apply(cdInputParentPath.getName(i).toString())).toString();
                                        else
                                            return variantSuffixMapping.apply(cdInputParentPath.getName(i).toString());
                                    }),
                            Stream.of(""))
                    .flatMap(p -> Stream.of(
                            variantPath.resolve(p).resolve(createMIPName(cdInputNameWithoutExtension, variantSuffix, ".png")),
                            variantPath.resolve(p).resolve(createMIPName(cdInputNameWithoutExtension, variantSuffix, ".tif"))))
                    .collect(Collectors.toList());
        }
        return candidateVariantPaths.stream()
                .filter(Files::exists)
                .filter(Files::isRegularFile)
                .findFirst()
                .map(p -> {
                    try {
                        return p.toRealPath().toString();
                    } catch(IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .map(FileData::fromString)
                .orElse(null);
    }

    private static FileData lookupVariantFileDataInArchive(String cdInputName, Path variantPath, String variantSuffix) {
        Path cdInputPath = Paths.get(cdInputName);
        String cdInputWithoutExtension = RegExUtils.replacePattern(cdInputPath.getFileName().toString(), "\\..*$", "");
        String cdInputWithoutObjectNum = RegExUtils.replacePattern(cdInputWithoutExtension, "_\\d\\d*$", "");
        Map<String, List<String>> variantArchiveEntries = getZipEntryNames(variantPath);
        List<String> variantEntryNames = Arrays.asList(
                createMIPName(cdInputWithoutExtension, variantSuffix, ".png"),
                createMIPName(cdInputWithoutExtension, variantSuffix, ".tif"),
                createMIPName(cdInputWithoutObjectNum, variantSuffix, ".png"),
                createMIPName(cdInputWithoutObjectNum, variantSuffix, ".tif")
        );
        return variantEntryNames.stream()
                .filter(en -> variantArchiveEntries.containsKey(en))
                .flatMap(en -> variantArchiveEntries.get(en).stream())
                .findFirst()
                .map(en -> FileData.fromComponents(FileData.FileDataType.zipEntry, variantPath.toString(), en, true))
                .orElse(null);
    }

    private static Map<String, List<String>> getZipEntryNames(Path zipPath) {
        if (ARCHIVE_ENTRIES_CACHE.get(zipPath) == null) {
            return cacheZipEntryNames(zipPath);
        } else {
            return ARCHIVE_ENTRIES_CACHE.get(zipPath);
        }
    }

    private static Map<String, List<String>> cacheZipEntryNames(Path zipPath) {
        ZipFile archiveFile;
        try {
            archiveFile = new ZipFile(zipPath.toFile());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try {
            Map<String, List<String>> zipEntryNames = archiveFile.stream()
                    .filter(ze -> !ze.isDirectory())
                    .map(ZipEntry::getName)
                    .collect(Collectors.groupingBy(zen -> Paths.get(zen).getFileName().toString(), Collectors.toList()));
            ARCHIVE_ENTRIES_CACHE.put(zipPath, zipEntryNames);
            return zipEntryNames;
        } finally {
            try {
                archiveFile.close();
            } catch (IOException ignore) {
            }
        }
    }

    private static String createMIPName(String name, String suffix, String ext) {
        return name + StringUtils.defaultIfEmpty(suffix, "") + ext;
    }

}
