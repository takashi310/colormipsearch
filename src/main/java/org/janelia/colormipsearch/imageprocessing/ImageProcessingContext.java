package org.janelia.colormipsearch.imageprocessing;

import java.util.HashMap;
import java.util.Map;

class ImageProcessingContext {
    private Map<Object, Object> ctxValues = new HashMap<>();

    void set(Object name, Object value) {
        ctxValues.put(name, value);
    }

    Object get(Object name) {
        return ctxValues.get(name);
    }

}
