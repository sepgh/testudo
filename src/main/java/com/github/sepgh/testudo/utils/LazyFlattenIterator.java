package com.github.sepgh.testudo.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class LazyFlattenIterator<Y, X> implements Iterator<Y> {

    private final Iterator<X> xIterator;
    private final Function<X, Iterator<Y>> yIteratorFunction;
    private Iterator<Y> currentYIterator = null;

    public LazyFlattenIterator(Iterator<X> xIterator, Function<X, Iterator<Y>> yIteratorFunction) {
        this.xIterator = xIterator;
        this.yIteratorFunction = yIteratorFunction;
    }

    @Override
    public boolean hasNext() {
        // Find the next non-empty Y iterator
        while ((currentYIterator == null || !currentYIterator.hasNext()) && xIterator.hasNext()) {
            currentYIterator = yIteratorFunction.apply(xIterator.next());
        }
        // Return true if we have a non-empty currentYIterator
        return currentYIterator != null && currentYIterator.hasNext();
    }

    @Override
    public Y next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return currentYIterator.next();
    }
}
