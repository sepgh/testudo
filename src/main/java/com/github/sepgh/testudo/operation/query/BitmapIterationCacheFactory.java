package com.github.sepgh.testudo.operation.query;

import com.github.sepgh.testudo.ds.Bitmap;

import java.util.HashMap;
import java.util.Map;

public class BitmapIterationCacheFactory implements IterationCacheFactory {
    @Override
    public <T extends Number> IterationCache<T> create() {
        return new IterationCache<T>() {
            private final Map<Integer, Bitmap<T>> cache = new HashMap<>();

            @Override
            public void add(int cacheIndex, T value) {
                if (cache.containsKey(cacheIndex)) {
                    cache.get(cacheIndex).on(value);
                } else {
                    cache.put(cacheIndex, Bitmap.getGenericInstance(value));
                }
            }

            @Override
            public boolean cacheInitialized(int cacheIndex) {
                return cache.get(cacheIndex) != null;
            }

            @Override
            public boolean contains(int cacheIndex, T value) {
                if (cache.containsKey(cacheIndex)) {
                    return cache.get(cacheIndex).isOn(value);
                }
                return false;
            }
        };
    }
}
