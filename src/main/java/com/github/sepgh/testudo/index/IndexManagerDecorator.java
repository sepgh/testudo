package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.util.Optional;

public class IndexManagerDecorator<K extends Comparable<K>, V extends Comparable<V>> implements IndexManager<K, V> {
    protected final IndexManager<K, V> indexManager;

    public IndexManagerDecorator(IndexManager<K, V> indexManager) {
        this.indexManager = indexManager;
    }

    public AbstractTreeNode<K> addIndex(K identifier, V value) throws InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException {
        return this.indexManager.addIndex(identifier, value);
    }
    public Optional<V> getIndex(K identifier) throws InternalOperationException {
        return this.indexManager.getIndex(identifier);
    }

    public boolean removeIndex(K identifier) throws InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        return this.indexManager.removeIndex(identifier);
    }

    @Override
    public int size() throws InternalOperationException {
        return this.indexManager.size();
    }

    @Override
    public LockableIterator<AbstractLeafTreeNode.KeyValue<K, V>> getSortedIterator() throws InternalOperationException {
        return this.indexManager.getSortedIterator();
    }

    @Override
    public void purgeIndex() {
        this.indexManager.purgeIndex();
    }

    @Override
    public int getIndexId() {
        return this.indexManager.getIndexId();
    }
}
