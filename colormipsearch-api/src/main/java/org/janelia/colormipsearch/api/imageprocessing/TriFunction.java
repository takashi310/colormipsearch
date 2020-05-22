package org.janelia.colormipsearch.api.imageprocessing;

import java.util.function.Function;

@FunctionalInterface
public interface TriFunction<S, T, U, R> {
    R apply(S s, T t, U u);

    default <V> TriFunction<S, T, U, V> andThen(Function<? super R, ? extends V> after) {
        return (S s, T t, U u) -> after.apply(apply(s, t, u));
    }
}
