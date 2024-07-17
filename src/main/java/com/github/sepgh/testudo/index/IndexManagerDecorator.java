package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.util.Optional;

public class IndexManagerDecorator<K extends Comparable<K>, V extends Comparable<V>> implements IndexManager<K, V> {
    private final IndexManager<K, V> indexManager;

    public IndexManagerDecorator(IndexManager<K, V> indexManager) {
        this.indexManager = indexManager;
    }

    public AbstractTreeNode<K> addIndex(int index, K identifier, V value) throws InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException {
        return this.indexManager.addIndex(index, identifier, value);
    }
    public Optional<V> getIndex(int index, K identifier) throws InternalOperationException {
        return this.indexManager.getIndex(index, identifier);
    }

    public boolean removeIndex(int index, K identifier) throws InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        return this.indexManager.removeIndex(index, identifier);
    }

    @Override
    public int size(int index) throws InternalOperationException {
        return this.indexManager.size(index);
    }

    @Override
    public LockableIterator<AbstractLeafTreeNode.KeyValue<K, V>> getSortedIterator(int index) throws InternalOperationException {
        return this.indexManager.getSortedIterator(index);
    }
}
