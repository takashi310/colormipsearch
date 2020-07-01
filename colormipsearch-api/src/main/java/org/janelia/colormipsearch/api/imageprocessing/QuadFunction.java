package org.janelia.colormipsearch.api.imageprocessing;

import java.util.function.Function;

/**
 * This is a 4 parameter function.
 *
 * @param <P>
 * @param <S>
 * @param <T>
 * @param <U>
 * @param <R>
 */
@FunctionalInterface
public interface QuadFunction<P, S, T, U, R> {
    R apply(P p, S s, T t, U u);

    default <V> QuadFunction<P, S, T, U, V> andThen(Function<? super R, ? extends V> after) {
        return (P p, S s, T t, U u) -> after.apply(apply(p, s, t, u));
    }
}
