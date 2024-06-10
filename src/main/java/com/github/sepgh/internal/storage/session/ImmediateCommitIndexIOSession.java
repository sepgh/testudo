package com.github.sepgh.internal.storage.session;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.AbstractTreeNode;
import com.github.sepgh.internal.index.tree.node.NodeFactory;
import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.IndexTreeNodeIO;
import lombok.Getter;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class ImmediateCommitIndexIOSession<K extends Comparable<K>> implements IndexIOSession<K> {
    @Getter
    private final IndexStorageManager indexStorageManager;
    private final int table;
    private final NodeFactory<K> nodeFactory;

    public ImmediateCommitIndexIOSession(IndexStorageManager indexStorageManager, int table, NodeFactory<K> nodeFactory) {
        this.indexStorageManager = indexStorageManager;
        this.table = table;
        this.nodeFactory = nodeFactory;
    }

    @Override
    public Optional<AbstractTreeNode<K>> getRoot() throws ExecutionException, InterruptedException {
        Optional<IndexStorageManager.NodeData> optional = indexStorageManager.getRoot(table).get();
        return optional.map(nodeFactory::fromNodeData);
    }

    @Override
    public IndexStorageManager.NodeData write(AbstractTreeNode<K> node) throws IOException, ExecutionException, InterruptedException {
        return IndexTreeNodeIO.write(indexStorageManager, table, node).get();
    }

    @Override
    public AbstractTreeNode<K> read(Pointer pointer) throws ExecutionException, InterruptedException, IOException {
        return IndexTreeNodeIO.read(indexStorageManager, table, pointer, nodeFactory);
    }

    @Override
    public final void update(AbstractTreeNode<K> node) throws IOException, InterruptedException, ExecutionException {
        IndexTreeNodeIO.update(indexStorageManager, table, node);
    }

    @Override
    public void remove(AbstractTreeNode<K> node) throws ExecutionException, InterruptedException {
        IndexTreeNodeIO.remove(indexStorageManager, table, node);
    }

    @Override
    public void commit() {
        // Nothing
    }

    public static class Factory extends IndexIOSessionFactory {
        private static Factory instance;

        private Factory() {
        }

        public static synchronized Factory getInstance(){
            if (instance == null)
                instance = new Factory();
            return instance;
        }

        @Override
        public <K extends Comparable<K>> IndexIOSession<K> create(IndexStorageManager indexStorageManager, int table, NodeFactory<K> nodeFactory) {
            return new ImmediateCommitIndexIOSession<>(indexStorageManager, table, nodeFactory);
        }
    }
}
