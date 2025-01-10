package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.ds.BinaryList;
import lombok.SneakyThrows;

import java.util.ListIterator;
import java.util.NoSuchElementException;


public class AscendingBinaryListIterator<V extends Comparable<V>> implements ListIterator<V> {

    private final BinaryList<V> binaryList;
    private volatile int cursor = -1;

    public AscendingBinaryListIterator(BinaryList<V> binaryList) {
        this.binaryList = binaryList;
    }

    @Override
    public boolean hasNext() {
        return cursor + 1 <= binaryList.getLastItemIndex();
    }

    @SneakyThrows
    @Override
    public synchronized V next() {
        if (cursor + 1 <= binaryList.getLastItemIndex()) {
            V next = binaryList.getObjectAt(cursor + 1);
            cursor++;
            return next;
        }

        throw new NoSuchElementException();
    }

    @Override
    public boolean hasPrevious() {
        return cursor >= 0;
    }

    @SneakyThrows
    @Override
    public synchronized V previous() {
        if (cursor < 0) {
            throw new NoSuchElementException();
        }

        V previous = binaryList.getObjectAt(cursor);
        cursor--;
        return previous;
    }

    @Override
    public int nextIndex() {
        return cursor + 1;
    }

    @Override
    public int previousIndex() {
        return cursor - 1;
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
