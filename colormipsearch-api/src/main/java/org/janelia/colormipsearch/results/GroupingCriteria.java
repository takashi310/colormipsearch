package org.janelia.colormipsearch.results;

import java.util.function.BiPredicate;
import java.util.function.Function;

public class GroupingCriteria<T, K> {

    static class WrappedKey<K1> {
        private final K1 key;
        private final BiPredicate<K1, K1> eqTest;
        private final Function<K1, Integer> keyHash;

        private WrappedKey(K1 key,
                           BiPredicate<K1, K1> eqTest,
                           Function<K1, Integer> keyHash) {
            this.key = key;
            this.eqTest = eqTest;
            this.keyHash = keyHash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            @SuppressWarnings("unchecked")
            WrappedKey<K1> that = (WrappedKey<K1>) o;

            return eqTest.test(this.key, that.key);
        }

        @Override
        public int hashCode() {
            return keyHash.apply(key);
        }
    }

    private final T item;
    private final WrappedKey<K> wrappedKey;

    public GroupingCriteria(T item,
                            Function<T, K> keySelector,
                            BiPredicate<K, K> keyEqTest,
                            Function<K, Integer> keyHash) {
        this.item = item;
        this.wrappedKey = new WrappedKey<>(keySelector.apply(item), keyEqTest, keyHash);
    }

    public T getItem() {
        return item;
    }

    public K getKey() {
        return wrappedKey.key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        GroupingCriteria<?, ?> that = (GroupingCriteria<?, ?>) o;
        return this.wrappedKey.equals(that.wrappedKey);
    }

    @Override
    public int hashCode() {
        return wrappedKey.hashCode();
    }
}
