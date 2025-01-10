package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.ds.KeyValue;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.util.Iterator;
import java.util.Optional;

public class UniqueQueryableIndexDecorator<K extends Comparable<K>, V> implements UniqueQueryableIndex<K, V> {
    protected final UniqueQueryableIndex<K, V> decorated;

    public UniqueQueryableIndexDecorator(UniqueQueryableIndex<K, V> decorated) {
        this.decorated = decorated;
    }

    public AbstractTreeNode<K> addIndex(K identifier, V value) throws InternalOperationException {
        return this.decorated.addIndex(identifier, value);
    }

    @Override
    public AbstractTreeNode<K> addOrUpdateIndex(K identifier, V value) throws InternalOperationException {
        return this.decorated.addOrUpdateIndex(identifier, value);
    }

    public Optional<V> getIndex(K identifier) throws InternalOperationException {
        return this.decorated.getIndex(identifier);
    }

    public boolean removeIndex(K identifier) throws InternalOperationException {
        return this.decorated.removeIndex(identifier);
    }

    @Override
    public int size() throws InternalOperationException {
        return this.decorated.size();
    }

    @Override
    public LockableIterator<KeyValue<K, V>> getSortedIterator(Order order) throws InternalOperationException {
        return this.decorated.getSortedIterator(order);
    }

    @Override
    public void purgeIndex() {
        this.decorated.purgeIndex();
    }

    @Override
    public int getIndexId() {
        return this.decorated.getIndexId();
    }

    @Override
    public boolean supportIncrement() {
        return this.decorated.supportIncrement();
    }

    @Override
    public K nextKey() throws InternalOperationException {
        return this.decorated.nextKey();
    }

    @Override
    public boolean isNull(V value) {
        return this.decorated.isNull(value);
    }

    @Override
    public void addNull(V value) throws InternalOperationException {
        this.decorated.addNull(value);
    }

    @Override
    public void removeNull(V value) throws InternalOperationException {
        this.decorated.removeNull(value);
    }

    @Override
    public Iterator<V> getNullIndexes(Order order) {
        return this.decorated.getNullIndexes(order);
    }

    @Override
    public Iterator<KeyValue<K, V>> getSortedKeyValueIterator(Order order) throws InternalOperationException {
        return this.decorated.getSortedKeyValueIterator(order);
    }

    @Override
    public Iterator<V> getGreaterThan(K k, Order order) throws InternalOperationException {
        return this.decorated.getGreaterThan(k, order);
    }

    @Override
    public Iterator<V> getGreaterThanEqual(K k, Order order) throws InternalOperationException {
        return this.decorated.getGreaterThanEqual(k, order);
    }

    @Override
    public Iterator<V> getLessThan(K k, Order order) throws InternalOperationException {
        return this.decorated.getLessThan(k, order);
    }

    @Override
    public Iterator<V> getLessThanEqual(K k, Order order) throws InternalOperationException {
        return this.decorated.getLessThanEqual(k, order);
    }

    @Override
    public Iterator<V> getEqual(K k, Order order) throws InternalOperationException {
        return this.decorated.getEqual(k, order);
    }

    @Override
    public Iterator<V> getNotEqual(K k, Order order) throws InternalOperationException {
        return this.decorated.getNotEqual(k, order);
    }

    @Override
    public Iterator<V> getNulls(Order order) {
        return this.decorated.getNulls(order);
    }
}
