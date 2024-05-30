package org.janelia.colormipsearch.mips;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.model.FileData;

public class FileDataUtils {

    private static final Map<Path, Map<String, List<String>>> FILE_NAMES_CACHE = new HashMap<>();

    public static FileData lookupVariantFileData(List<String> variantLocations, String fastLookup, Pattern variantPattern) {
        if (CollectionUtils.isEmpty(variantLocations)) {
            return null;
        } else {
            return variantLocations.stream()
                    .filter(StringUtils::isNotBlank)
                    .map(Paths::get)
                    .map(variantPath -> {
                        if (Files.isDirectory(variantPath)) {
                            return lookupVariantFileDataInDir(variantPath, fastLookup, variantPattern);
                        } else if (Files.isRegularFile(variantPath)) {
                            return lookupVariantFileDataInArchive(variantPath, fastLookup, variantPattern);
                        } else {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElse(null);
        }
    }

    private static FileData lookupVariantFileDataInDir(Path variantPath, String fastLookup, Pattern variantPattern) {
        Map<String, List<String>> variantDirEntries = getDirEntryNames(variantPath);
        return variantDirEntries.entrySet().stream()
                .filter(e -> StringUtils.contains(e.getKey(), fastLookup))
                .filter(e -> {
                    Matcher variantMatcher = variantPattern.matcher(e.getKey());
                    return variantMatcher.find();
                })
                .flatMap(e -> e.getValue().stream())
                .findFirst()
                .map(FileData::fromString)
                .orElse(null);
    }

    private static FileData lookupVariantFileDataInArchive(Path variantPath, String fastLookup, Pattern variantPattern) {
        Map<String, List<String>> variantArchiveEntries = getZipEntryNames(variantPath);
        return variantArchiveEntries.entrySet().stream()
                .filter(e -> StringUtils.contains(e.getKey(), fastLookup))
                .filter(e -> {
                    Matcher variantMatcher = variantPattern.matcher(e.getKey());
                    return variantMatcher.find();
                })
                .flatMap(e -> e.getValue().stream())
                .findFirst()
                .map(en -> FileData.fromComponents(FileData.FileDataType.zipEntry, variantPath.toString(), en, true))
                .orElse(null);
    }

    private static Map<String, List<String>> getZipEntryNames(Path zipPath) {
        if (FILE_NAMES_CACHE.get(zipPath) == null) {
            return cacheZipEntryNames(zipPath);
        } else {
            return FILE_NAMES_CACHE.get(zipPath);
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
            FILE_NAMES_CACHE.put(zipPath, zipEntryNames);
            return zipEntryNames;
        } finally {
            try {
                archiveFile.close();
            } catch (IOException ignore) {
            }
        }
    }

    private static Map<String, List<String>> getDirEntryNames(Path dirPath) {
        if (FILE_NAMES_CACHE.get(dirPath) == null) {
            return cacheDirEntryNames(dirPath);
        } else {
            return FILE_NAMES_CACHE.get(dirPath);
        }
    }

    private static Map<String, List<String>> cacheDirEntryNames(Path dirPath) {
        try (Stream<Path> s = Files.find(dirPath, 1, (p, a) -> !a.isDirectory())) {
            Map<String, List<String>> dirEntryNames =
                    s.map(p -> {
                        try {
                            return p.toRealPath().toString();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }).collect(Collectors.groupingBy(f -> Paths.get(f).getFileName().toString(), Collectors.toList()));
            FILE_NAMES_CACHE.put(dirPath, dirEntryNames);
            return dirEntryNames;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
