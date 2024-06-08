package com.github.sepgh.internal.storage.session;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.cluster.BaseClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.cluster.ClusterIdentifier;
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
    private final ClusterIdentifier.Strategy<K> strategy;

    public ImmediateCommitIndexIOSession(IndexStorageManager indexStorageManager, int table, ClusterIdentifier.Strategy<K> strategy) {
        this.indexStorageManager = indexStorageManager;
        this.table = table;
        this.strategy = strategy;
    }

    @Override
    public Optional<BaseClusterTreeNode<K>> getRoot() throws ExecutionException, InterruptedException {
        Optional<IndexStorageManager.NodeData> optional = indexStorageManager.getRoot(table).get();
        return optional.map(nodeData -> BaseClusterTreeNode.fromNodeData(nodeData, strategy));
    }

    @Override
    public IndexStorageManager.NodeData write(BaseClusterTreeNode<K> node) throws IOException, ExecutionException, InterruptedException {
        return IndexTreeNodeIO.write(indexStorageManager, table, node).get();
    }

    @Override
    public BaseClusterTreeNode<K> read(Pointer pointer) throws ExecutionException, InterruptedException, IOException {
        return IndexTreeNodeIO.read(indexStorageManager, table, pointer, strategy);
    }

    @SafeVarargs
    @Override
    public final void update(BaseClusterTreeNode<K>... nodes) throws IOException, InterruptedException {
        IndexTreeNodeIO.update(indexStorageManager, table, nodes);
    }

    @Override
    public void remove(BaseClusterTreeNode<K> node) throws ExecutionException, InterruptedException {
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
        public <K extends Comparable<K>> IndexIOSession<K> create(IndexStorageManager indexStorageManager, int table, ClusterIdentifier.Strategy<K> strategy) {
            return new ImmediateCommitIndexIOSession<>(indexStorageManager, table, strategy);
        }
    }
}
