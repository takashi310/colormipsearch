package org.janelia.colormipsearch.imageprocessing;

import java.io.Serializable;
import java.util.function.Function;

/**
 * This is a 3 parameter function.
 *
 * @param <S>
 * @param <T>
 * @param <U>
 * @param <R>
 */
@FunctionalInterface
public interface TriFunction<S, T, U, R> extends Serializable {
    R apply(S s, T t, U u);

    default <V> TriFunction<S, T, U, V> andThen(Function<? super R, ? extends V> after) {
        return (S s, T t, U u) -> after.apply(apply(s, t, u));
    }
}
