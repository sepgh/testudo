package com.github.sepgh.testudo.operation.query;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public class SortedIterator<T extends Comparable<T>> implements Iterator<T> {
    private final Iterator<T> sourceIterator;
    private final Set<T> targetSet = new HashSet<>();

    private T next;

    public SortedIterator(Iterator<T> targetIterator, Iterator<T> sourceIterator) {
        this.sourceIterator = sourceIterator;

        while (targetIterator.hasNext()) {
            targetSet.add(targetIterator.next());
        }

    }

    @Override
    public boolean hasNext() {
        if (next != null) {
            return true;
        }
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
        T t = next;
        next = null;
        return t;
    }
}
