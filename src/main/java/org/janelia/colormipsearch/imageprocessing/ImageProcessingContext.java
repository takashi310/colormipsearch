package org.janelia.colormipsearch.imageprocessing;

import java.util.HashMap;
import java.util.Map;

class ImageProcessingContext {
    private Map<String, Object> ctxValues = new HashMap<>();

    void set(String name, Object value) {
        ctxValues.put(name, value);
    }

    Object get(String name) {
        return ctxValues.get(name);
    }

}
