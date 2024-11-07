package com.github.sepgh.testudo.operation.query;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public class IteratorSorter<T extends Comparable<T>> implements Iterator<T> {
    private final Iterator<T> sourceIterator;
    private final Set<T> targetSet = new HashSet<>();

    private T next;

    public IteratorSorter(Iterator<T> targetIterator, Iterator<T> sourceIterator) {
        this.sourceIterator = sourceIterator;

        while (targetIterator.hasNext()) {
            targetSet.add(targetIterator.next());
        }

    }

    @Override
    public boolean hasNext() {
        while (sourceIterator.hasNext()) {
            T current = sourceIterator.next();
            if (targetSet.contains(current)) {
                next = current;
                return true;
            }
        }
        next = null;
        return false;
    }

    @Override
    public T next() {
        if (next == null) {
            throw new NoSuchElementException();
        }
        return next;
    }
}
