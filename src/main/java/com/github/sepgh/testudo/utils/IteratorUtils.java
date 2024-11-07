package com.github.sepgh.testudo.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class IteratorUtils {

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
