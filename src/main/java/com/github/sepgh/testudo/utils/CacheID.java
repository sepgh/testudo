package com.github.sepgh.testudo.utils;

import java.util.Objects;

public record CacheID<K extends Comparable<K>>(int index, K key) {
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheID<?> cacheID = (CacheID<?>) o;
        return index == cacheID.index && Objects.equals(key, cacheID.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, key);
    }
}
