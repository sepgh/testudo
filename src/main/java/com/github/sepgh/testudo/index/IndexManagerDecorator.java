package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;

import java.util.Optional;

public class IndexManagerDecorator<K extends Comparable<K>, V extends Comparable<V>> implements IndexManager<K, V> {
    private final IndexManager<K, V> indexManager;

    public IndexManagerDecorator(IndexManager<K, V> indexManager) {
        this.indexManager = indexManager;
    }

    public AbstractTreeNode<K> addIndex(int table, K identifier, V value) throws InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException {
        return this.indexManager.addIndex(table, identifier, value);
    }
    public Optional<V> getIndex(int table, K identifier) throws InternalOperationException {
        return this.indexManager.getIndex(table, identifier);
    }

    public boolean removeIndex(int table, K identifier) throws InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        return this.indexManager.removeIndex(table, identifier);
    }

    @Override
    public int size(int table) throws InternalOperationException {
        return this.indexManager.size(table);
    }
}
