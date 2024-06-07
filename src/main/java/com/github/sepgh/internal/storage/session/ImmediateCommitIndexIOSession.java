package com.github.sepgh.internal.storage.session;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.cluster.BaseClusterTreeNode;
import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.IndexTreeNodeIO;
import lombok.Getter;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class ImmediateCommitIndexIOSession implements IndexIOSession {
    @Getter
    private final IndexStorageManager indexStorageManager;
    private final int table;

    public ImmediateCommitIndexIOSession(IndexStorageManager indexStorageManager, int table) {
        this.indexStorageManager = indexStorageManager;
        this.table = table;
    }

    @Override
    public Optional<BaseClusterTreeNode> getRoot() throws ExecutionException, InterruptedException {
        Optional<IndexStorageManager.NodeData> optional = indexStorageManager.getRoot(table).get();
        return optional.map(BaseClusterTreeNode::fromNodeData);
    }

    @Override
    public IndexStorageManager.NodeData write(BaseClusterTreeNode node) throws IOException, ExecutionException, InterruptedException {
        return IndexTreeNodeIO.write(indexStorageManager, table, node).get();
    }

    @Override
    public BaseClusterTreeNode read(Pointer pointer) throws ExecutionException, InterruptedException, IOException {
        return IndexTreeNodeIO.read(indexStorageManager, table, pointer);
    }

    @Override
    public void update(BaseClusterTreeNode... nodes) throws IOException, InterruptedException {
        IndexTreeNodeIO.update(indexStorageManager, table, nodes);
    }

    @Override
    public void remove(BaseClusterTreeNode node) throws ExecutionException, InterruptedException {
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
        public IndexIOSession create(IndexStorageManager indexStorageManager, int table) {
            return new ImmediateCommitIndexIOSession(indexStorageManager, table);
        }
    }
}
