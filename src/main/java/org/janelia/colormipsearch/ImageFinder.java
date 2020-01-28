package org.janelia.colormipsearch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImageFinder {

    private static final Logger LOG = LoggerFactory.getLogger(ImageFinder.class);

    /**
     * Load provided image libraries into memory.
     * @param imagesFiles
     */
    static Stream<String> findImages(List<String> imagesFiles) {
        return imagesFiles.stream()
                .map(imageFileOrDir -> Paths.get(imageFileOrDir))
                .flatMap(imageFileOrDir -> {
                    try {
                        return Files.walk(imageFileOrDir, 1);
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
