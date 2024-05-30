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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

    public static FileData lookupVariantFileData(List<String> variantLocations, Pattern variantPattern, String sourceCDMName, String variantSuffix, Function<String, String> variantSuffixMapping) {
        if (CollectionUtils.isEmpty(variantLocations)) {
            return null;
        } else {
            return variantLocations.stream()
                    .filter(StringUtils::isNotBlank)
                    .map(Paths::get)
                    .map(variantPath -> {
                        if (Files.isDirectory(variantPath)) {
                            return lookupVariantFileDataInDir(variantPath, variantPattern);
                        } else if (Files.isRegularFile(variantPath)) {
                            return lookupVariantFileDataInArchive(variantPath, variantPattern);
                        } else {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElse(null);
        }
    }

    private static FileData lookupVariantFileDataInDir(Path variantPath,
                                                       Pattern variantPattern) {
        try (Stream<Path> s = Files.find(variantPath, 1, (p, a) -> {
            Matcher variantMatcher = variantPattern.matcher(p.getFileName().toString());
            return variantMatcher.find();
        })) {
            return s.map(p -> {
                        try {
                            return p.toRealPath();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .filter(Files::isRegularFile)
                    .findFirst()
                    .map(p -> FileData.fromString(p.toString()))
                    .orElse(null);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static FileData lookupVariantFileDataInArchive(Path variantPath, Pattern variantPattern) {
        Map<String, List<String>> variantArchiveEntries = getZipEntryNames(variantPath);
        return variantArchiveEntries.entrySet().stream()
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
