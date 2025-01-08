package com.github.sepgh.testudo.utils;

import com.github.sepgh.testudo.ds.KeyValue;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class IteratorUtils {

    public static <K extends Comparable<K>, V> Iterator<V> getNotEqualIterator(Iterator<KeyValue<K, V>> iterator, K key) {
        return new Iterator<>() {
            private V candidate = null;

            @Override
            public boolean hasNext() {
                if (candidate != null){
                    return true;
                }

                while (iterator.hasNext()) {
                    KeyValue<K, V> next = iterator.next();
                    if (!next.key().equals(key)){
                        candidate = next.value();
                        return true;
                    }
                }

                return false;
            }

            @Override
            public V next() {
                if (candidate == null){
                    throw new NoSuchElementException();
                }
                V val = candidate;
                candidate = null;
                return val;
            }
        };
    }

    public static <V> Iterator<V> getCleanIterator() {
        return new Iterator<>() {

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public V next() {
                throw new NoSuchElementException();
            }
        };
    }


    public static <V, Y> Iterator<V> modifyNext(Iterator<Y> iterator, Function<Y, V> function) {
        return new Iterator<V>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public V next() {
                return function.apply(iterator.next());
            }
        };
    }

}
