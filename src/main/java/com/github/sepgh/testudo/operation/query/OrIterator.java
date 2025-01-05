package com.github.sepgh.testudo.operation.query;

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

public class OrIterator<T extends Comparable<T>> implements Iterator<T> {
    private final PriorityQueue<PeekableIterator<T>> iterators;
    private final boolean ascending;
    private T nextItem;

    OrIterator(List<Iterator<T>> iterators, Order order) {
        this.ascending = order == Order.ASC;
        this.iterators = new PriorityQueue<>((a, b) -> order.equals(Order.ASC) ?
                a.peek().compareTo(b.peek()) : b.peek().compareTo(a.peek()));

        // Initialize the priority queue with each iterator's first element
        for (Iterator<T> it : iterators) {
            if (it.hasNext()) {
                this.iterators.add(new PeekableIterator<>(it));
            }
        }
        advance();
    }

    private void advance() {
        if (iterators.isEmpty()) {
            nextItem = null;
            return;
        }

        // Find the smallest (or largest if descending) item among the iterators
        T candidate = null;
        for (PeekableIterator<T> it : iterators) {
            if (it.hasNext()) {
                T peeked = it.peek();
                if (candidate == null || (ascending ? peeked.compareTo(candidate) < 0 : peeked.compareTo(candidate) > 0)) {
                    candidate = peeked;
                }
            }
        }

        // If there's no candidate (all iterators exhausted), end
        if (candidate == null) {
            nextItem = null;
            return;
        }

        // Set nextItem to candidate and advance all iterators with this value
        nextItem = candidate;
        for (PeekableIterator<T> it : iterators) {
            if (it.hasNext() && it.peek().equals(candidate)) {
                it.next();
            }
        }

        // Remove exhausted iterators
        iterators.removeIf(it -> !it.hasNext());
    }

    @Override
    public boolean hasNext() {
        return nextItem != null;
    }

    @Override
    public T next() {
        T result = nextItem;
        advance();
        return result;
    }

    // PeekableIterator class for reference
    private static class PeekableIterator<T> implements Iterator<T> {
        private final Iterator<T> iterator;
        private T next;

        public PeekableIterator(Iterator<T> iterator) {
            this.iterator = iterator;
            if (iterator.hasNext()) next = iterator.next();
        }

        public T peek() {
            return next;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public T next() {
            T current = next;
            next = iterator.hasNext() ? iterator.next() : null;
            return current;
        }
    }

}
