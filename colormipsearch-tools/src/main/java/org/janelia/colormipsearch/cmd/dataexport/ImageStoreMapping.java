package org.janelia.colormipsearch.cmd.dataexport;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class ImageStoreMapping {
    private final String defaultImageStore;
    private final Map<String, String> imageStoreByLibrary;

    public ImageStoreMapping(String defaultImageStore, Map<String, String> imageStoreByLibrary) {
        this.defaultImageStore = defaultImageStore;
        this.imageStoreByLibrary = imageStoreByLibrary;
    }

    String getImageStore(String libraryName) {
        String imageStore = imageStoreByLibrary.get(libraryName);
        return StringUtils.defaultIfBlank(imageStore, defaultImageStore);
    }
}
