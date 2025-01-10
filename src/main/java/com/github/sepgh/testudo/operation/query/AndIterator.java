package com.github.sepgh.testudo.operation.query;

import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class AndIterator<T extends Number> implements Iterator<T> {
    private T current;
    private final IterationCache<T> iterationCache;
    private final List<Iterator<T>> iterators;

    public AndIterator(List<Iterator<T>> iterators) {
        this(iterators, new HashsetIterationCacheFactory());
    }

    public AndIterator(List<Iterator<T>> iterators, IterationCacheFactory iterationCacheFactory) {
        Preconditions.checkArgument(iterators.size() > 1, "Need at least 2 iterators to perform AND operation");
        this.iterators = iterators;
        this.iterationCache = iterationCacheFactory.create();
    }

    @Override
    public boolean hasNext() {
        // If next() is not called yet, do not recalculate
        if (current != null) {
            return true;
        }

        // calculate current
        Iterator<T> firstIterator = iterators.get(0);
        List<Iterator<T>> otherIterators = iterators.subList(1, iterators.size());
        T candidate = null;

        while (candidate == null) {
            if (!firstIterator.hasNext()) {
                break;
            }
            candidate = firstIterator.next();

            for (int i = 0; i < otherIterators.size(); i++) {
                if (!containsCandidate(i, otherIterators.get(i), candidate)) {
                    candidate = null;
                    break;
                }
            }
        }

        if (candidate != null) {
            current = candidate;
            return true;
        }

        return false;
    }

    private boolean containsCandidate(int i, Iterator<T> tIterator, T candidate) {
        boolean cacheInitialized = iterationCache.cacheInitialized(i);

        if (cacheInitialized && iterationCache.contains(i, candidate)) {
            return true;
        }

        while (tIterator.hasNext()) {
            T next = tIterator.next();
            iterationCache.add(i, next);

            if (next.equals(candidate)) {
                return true;
            }

        }

        return false;
    }


    @Override
    public T next() {
        if (current == null) {
            throw new NoSuchElementException();
        }
        T output = current;
        current = null;
        return output;
    }
}
