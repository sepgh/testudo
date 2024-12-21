package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.ds.BinaryList;

import java.util.ListIterator;
import java.util.NoSuchElementException;

public class DescendingBinaryListIterator<V extends Comparable<V>> implements ListIterator<V> {
    private final BinaryList<V> binaryList;
    private volatile int cursor;

    public DescendingBinaryListIterator(BinaryList<V> binaryList) {
        this.binaryList = binaryList;
        cursor = this.binaryList.getLastItemIndex();
    }

    @Override
    public boolean hasNext() {
        return cursor >= 0;
    }

    @Override
    public synchronized V next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        V previous = binaryList.getObjectAt(cursor);
        cursor--;
        return previous;
    }

    @Override
    public boolean hasPrevious() {
        return cursor < binaryList.getLastItemIndex();
    }

    @Override
    public synchronized V previous() {
        if (!hasPrevious()) {
            throw new NoSuchElementException();
        }

        V previous = binaryList.getObjectAt(cursor);
        cursor++;
        return previous;
    }

    @Override
    public int nextIndex() {
        return cursor - 1;
    }

    @Override
    public int previousIndex() {
        return cursor + 1;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(V v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(V v) {
        throw new UnsupportedOperationException();
    }

}
