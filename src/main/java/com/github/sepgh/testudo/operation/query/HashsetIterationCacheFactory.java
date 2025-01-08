package com.github.sepgh.testudo.operation.query;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class HashsetIterationCacheFactory implements IterationCacheFactory {
    @Override
    public <T extends Number> IterationCache<T> create() {
        return new IterationCache<T>() {
            private final Map<Integer, HashSet<T>> cache = new HashMap<>();

            @Override
            public void add(int cacheIndex, T value) {
                if (cache.containsKey(cacheIndex)) {
                    cache.get(cacheIndex).add(value);
                } else {
                    HashSet<T> set = new HashSet<>();
                    set.add(value);
                    cache.put(cacheIndex, set);
                }
            }

            @Override
            public boolean cacheInitialized(int cacheIndex) {
                return cache.get(cacheIndex) != null;
            }

            @Override
            public boolean contains(int cacheIndex, T value) {
                if (cache.containsKey(cacheIndex)) {
                    return cache.get(cacheIndex).contains(value);
                }
                return false;
            }
        };
    }
}
