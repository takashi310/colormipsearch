package org.janelia.colormipsearch.imageprocessing;

import java.io.Serializable;
import java.util.function.Function;

@FunctionalInterface
public interface MappingFunction<T, U> extends Function<T, U>, Serializable {
}
