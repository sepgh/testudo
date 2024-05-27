package com.github.sepgh.internal.storage.session;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.BaseTreeNode;
import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.IndexTreeNodeIO;
import lombok.Getter;

import java.io.IOException;
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
    public IndexStorageManager.NodeData write(BaseTreeNode node) throws IOException, ExecutionException, InterruptedException {
        return IndexTreeNodeIO.write(node, indexStorageManager, table).get();
    }

    @Override
    public BaseTreeNode read(Pointer pointer) throws ExecutionException, InterruptedException {
        return IndexTreeNodeIO.read(indexStorageManager, table, pointer);
    }

    @Override
    public void update(BaseTreeNode... nodes) throws IOException, InterruptedException {
        IndexTreeNodeIO.update(indexStorageManager, table, nodes);
    }

    @Override
    public void remove(BaseTreeNode node) throws ExecutionException, InterruptedException {
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
