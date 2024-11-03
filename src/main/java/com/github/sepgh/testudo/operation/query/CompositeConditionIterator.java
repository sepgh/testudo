package com.github.sepgh.testudo.operation.query;

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

public class CompositeConditionIterator<T extends Comparable<T>> implements Iterator<T> {
    private final CompositeCondition.CompositeOperator operator;
    private final PriorityQueue<PeekableIterator<T>> iterators;
    private T nextItem;

    CompositeConditionIterator(CompositeCondition.CompositeOperator operator, List<Iterator<T>> iterators, Order order) {
        this.operator = operator;
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
        if (operator == CompositeCondition.CompositeOperator.AND) {
            advanceForAnd();
        } else {
            advanceForOr();
        }
    }

    private void advanceForAnd() {
        while (!iterators.isEmpty()) {
            T candidate = iterators.peek().peek();
            boolean allMatch = true;

            for (PeekableIterator<T> it : iterators) {
                while (it.hasNext() && !it.peek().equals(candidate)) {
                    it.next();
                }
                if (!it.hasNext() || !it.peek().equals(candidate)) {
                    allMatch = false;
                    break;
                }
            }

            if (allMatch) {
                nextItem = candidate;
                iterators.forEach(PeekableIterator::next);
                return;
            }
        }
        nextItem = null;
    }

    private void advanceForOr() {
        if (!iterators.isEmpty()) {
            PeekableIterator<T> smallest = iterators.poll();
            nextItem = smallest.next();
            if (smallest.hasNext()) {
                iterators.add(smallest);
            }
        } else {
            nextItem = null;
        }
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
