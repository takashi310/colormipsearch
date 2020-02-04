package org.janelia.colormipsearch;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImageFinder {

    private static final Logger LOG = LoggerFactory.getLogger(ImageFinder.class);

    /**
     * Load provided image libraries into memory.
     * @param imagesFiles
     */
    static Stream<String> findImages(List<String> imagesFiles) {
        final char[] wildcards = new char[] {'*', '?'};
        return imagesFiles.stream()
                .flatMap(imageFileOrDir -> {
                    try {
                        if (StringUtils.containsAny(imageFileOrDir, wildcards)) {
                            Path imageFileOrDirPath = Paths.get(imageFileOrDir);
                            Path rootDir = imageFileOrDirPath.getRoot();
                            int nPathComponents = imageFileOrDirPath.getNameCount();
                            for (int i = 0; i < nPathComponents; i++) {
                                String pathComponent = imageFileOrDirPath.getName(i).toString();
                                if (StringUtils.containsAny(pathComponent, wildcards)) {
                                    PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher("glob:" + imageFileOrDirPath.subpath(i, nPathComponents).toString() + "*");
                                    Path relStartPath = imageFileOrDirPath.subpath(0, i);
                                    Path startPath = rootDir == null ? relStartPath : rootDir.resolve(relStartPath);
                                    return Files.find(startPath, 1 + nPathComponents - i, (p, a) -> pathMatcher.matches(startPath.relativize(p)));
                                }
                            }
                            Preconditions.checkState(false, "Scanned all path components and no wildcard found even though the initial check said there was one");
                            return Stream.of();
                        } else {
                            return Files.find(Paths.get(imageFileOrDir), 1, (p, a) -> true);
                        }
                    } catch (IOException e) {
                        LOG.warn("Error traversing {}", imageFileOrDir, e);
                        return Stream.of();
                    }
                })
                .filter(fp -> Files.isRegularFile(fp))
                .map(fp -> fp.toString())
                ;
    }

}
