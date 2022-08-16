package org.janelia.colormipsearch.dataio.fileutils;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileSystem utils for creating directories, listing files, etc.
 */
public class FSUtils {
    private static final Logger LOG = LoggerFactory.getLogger(FSUtils.class);

    public static void createDirs(@Nullable Path... dirs) {
        for (Path dir : dirs) {
            if (dir != null) {
                try {
                    // create output directory
                    Files.createDirectories(dir);
                } catch (IOException e) {
                    LOG.error("Error creating directory: {}", dir, e);
                    System.exit(1);
                }
            }
        }
    }

    @Nullable
    public static Path getOutputPath(Path outputDir, File inputFile) {
        if (outputDir == null) {
            return null;
        } else {
            return outputDir.resolve(inputFile.getName());
        }
    }

    @Nullable
    public static Path getOutputPath(Path outputDir, String fname) {
        if (outputDir == null) {
            return null;
        } else {
            return outputDir.resolve(fname);
        }
    }

    public static List<String> getFiles(String location, int offsetParam, int lengthParam) {
        try {
            Path pathLocation = Paths.get(location);
            if (Files.isRegularFile(pathLocation)) {
                return Collections.singletonList(pathLocation.toString());
            } else if (Files.isDirectory(pathLocation)) {
                int from = Math.max(offsetParam, 0);
                List<String> filenamesList = Files.find(pathLocation, 1, (p, fa) -> fa.isRegularFile())
                        .skip(from)
                        .map(Path::toString)
                        .collect(Collectors.toList());
                if (lengthParam > 0 && lengthParam < filenamesList.size()) {
                    return filenamesList.subList(0, lengthParam);
                } else {
                    return filenamesList;
                }
            } else {
                return Collections.emptyList();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
