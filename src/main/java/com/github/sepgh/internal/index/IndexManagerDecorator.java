package com.github.sepgh.internal.index;

import com.github.sepgh.internal.index.tree.node.AbstractTreeNode;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class IndexManagerDecorator<K extends Comparable<K>, V extends Comparable<V>> implements IndexManager<K, V> {
    private final IndexManager<K, V> indexManager;

    public IndexManagerDecorator(IndexManager<K, V> indexManager) {
        this.indexManager = indexManager;
    }

    public AbstractTreeNode<K> addIndex(int table, K identifier, V value) throws ExecutionException, InterruptedException, IOException{
        return this.indexManager.addIndex(table, identifier, value);
    }
    public Optional<V> getIndex(int table, K identifier) throws ExecutionException, InterruptedException, IOException {
        return this.indexManager.getIndex(table, identifier);
    }

    public boolean removeIndex(int table, K identifier) throws ExecutionException, InterruptedException, IOException {
        return this.indexManager.removeIndex(table, identifier);
    }

    @Override
    public int size(int table) throws ExecutionException, InterruptedException {
        try {
            return this.indexManager.size(table);
        } catch (IOException e) {
            return 0;
        }
    }
}
