package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.ds.KeyValue;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.IndexMissingException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.util.Iterator;
import java.util.Optional;

public class UniqueTreeIndexManagerDecorator<K extends Comparable<K>, V> implements UniqueTreeIndexManager<K, V> {
    protected final UniqueTreeIndexManager<K, V> uniqueTreeIndexManager;

    public UniqueTreeIndexManagerDecorator(UniqueTreeIndexManager<K, V> uniqueTreeIndexManager) {
        this.uniqueTreeIndexManager = uniqueTreeIndexManager;
    }

    public AbstractTreeNode<K> addIndex(K identifier, V value) throws InternalOperationException, IndexExistsException {
        return this.uniqueTreeIndexManager.addIndex(identifier, value);
    }

    @Override
    public AbstractTreeNode<K> updateIndex(K identifier, V value) throws InternalOperationException, IndexMissingException {
        return this.uniqueTreeIndexManager.updateIndex(identifier, value);
    }

    public Optional<V> getIndex(K identifier) throws InternalOperationException {
        return this.uniqueTreeIndexManager.getIndex(identifier);
    }

    public boolean removeIndex(K identifier) throws InternalOperationException {
        return this.uniqueTreeIndexManager.removeIndex(identifier);
    }

    @Override
    public int size() throws InternalOperationException {
        return this.uniqueTreeIndexManager.size();
    }

    @Override
    public LockableIterator<KeyValue<K, V>> getSortedIterator(Order order) throws InternalOperationException {
        return this.uniqueTreeIndexManager.getSortedIterator(order);
    }

    @Override
    public void purgeIndex() {
        this.uniqueTreeIndexManager.purgeIndex();
    }

    @Override
    public int getIndexId() {
        return this.uniqueTreeIndexManager.getIndexId();
    }

    @Override
    public boolean supportIncrement() {
        return this.uniqueTreeIndexManager.supportIncrement();
    }

    @Override
    public K nextKey() throws InternalOperationException {
        return this.uniqueTreeIndexManager.nextKey();
    }

    @Override
    public boolean isNull(V value) {
        return this.uniqueTreeIndexManager.isNull(value);
    }

    @Override
    public void addNull(V value) {
        this.uniqueTreeIndexManager.addNull(value);
    }

    @Override
    public void removeNull(V value) {
        this.uniqueTreeIndexManager.removeNull(value);
    }

    @Override
    public Iterator<V> getNullIndexes(Order order) {
        return this.uniqueTreeIndexManager.getNullIndexes(order);
    }
}
