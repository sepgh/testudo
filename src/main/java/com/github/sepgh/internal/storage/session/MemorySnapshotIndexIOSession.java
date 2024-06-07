package com.github.sepgh.internal.storage.session;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.cluster.BaseClusterTreeNode;
import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.IndexTreeNodeIO;
import lombok.Getter;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class MemorySnapshotIndexIOSession implements IndexIOSession {
    @Getter
    protected final IndexStorageManager indexStorageManager;
    protected final int table;
    protected final Set<Pointer> updated = new HashSet<>();
    protected final Map<Pointer, BaseClusterTreeNode> pool = new HashMap<>();
    protected final Map<Pointer, BaseClusterTreeNode> original = new HashMap<>();
    protected final List<Pointer> created = new LinkedList<>();
    protected final List<Pointer> deleted = new LinkedList<>();
    protected BaseClusterTreeNode root;

    public MemorySnapshotIndexIOSession(IndexStorageManager indexStorageManager, int table) {
        this.indexStorageManager = indexStorageManager;
        this.table = table;
    }

    @Override
    public Optional<BaseClusterTreeNode> getRoot() throws ExecutionException, InterruptedException {
        if (root == null){
            Optional<IndexStorageManager.NodeData> optional = indexStorageManager.getRoot(table).get();
            if (optional.isPresent()){
                BaseClusterTreeNode baseClusterTreeNode = BaseClusterTreeNode.fromNodeData(optional.get());
                this.root = baseClusterTreeNode;
                return Optional.of(baseClusterTreeNode);
            }
        } else {
            return Optional.of(root);
        }

        return Optional.empty();
    }

    @Override
    public IndexStorageManager.NodeData write(BaseClusterTreeNode node) throws IOException, ExecutionException, InterruptedException {
        IndexStorageManager.NodeData nodeData = IndexTreeNodeIO.write(indexStorageManager, table, node).get();
        this.created.add(nodeData.pointer());
        this.pool.put(nodeData.pointer(), node);
        if (node.isRoot())
            root = node;
        return nodeData;
    }

    @Override
    public BaseClusterTreeNode read(Pointer pointer) throws ExecutionException, InterruptedException, IOException {
        if (deleted.contains(pointer))
            return null;

        if (updated.contains(pointer))
            return pool.get(pointer);

        if (created.contains(pointer))
            return pool.get(pointer);

        BaseClusterTreeNode baseClusterTreeNode = IndexTreeNodeIO.read(indexStorageManager, table, pointer);
        pool.put(pointer, baseClusterTreeNode);

        byte[] copy = new byte[baseClusterTreeNode.getData().length];
        System.arraycopy(baseClusterTreeNode.getData(), 0, copy, 0, copy.length);
        original.put(pointer, BaseClusterTreeNode.fromNodeData(new IndexStorageManager.NodeData(pointer, copy)));
        if (baseClusterTreeNode.isRoot())
            this.root = baseClusterTreeNode;
        return baseClusterTreeNode;
    }

    @Override
    public void update(BaseClusterTreeNode... nodes) throws IOException, InterruptedException {
        for (BaseClusterTreeNode node : nodes) {
            pool.put(
                    node.getPointer(),
                    node
            );
            updated.add(node.getPointer());
            if (node.isRoot())
                root = node;
        }
    }

    @Override
    public void remove(BaseClusterTreeNode node) throws ExecutionException, InterruptedException {
        deleted.add(node.getPointer());
    }


    @Override
    public void commit() throws InterruptedException, IOException, ExecutionException {
        for (Pointer pointer : deleted) {
            try {
                IndexTreeNodeIO.remove(indexStorageManager, table, pointer);
            } catch (ExecutionException | InterruptedException e) {
                this.rollback();
            }
        }

        try {
            for (Pointer pointer : updated) {
                IndexTreeNodeIO.update(indexStorageManager, table, pool.get(pointer));
            }
        } catch (InterruptedException | IOException e) {
            this.rollback();
        }

    }

    protected void rollback() throws IOException, InterruptedException, ExecutionException {
        for (Pointer pointer : deleted) {
            IndexTreeNodeIO.update(indexStorageManager, table, original.get(pointer));
        }

        for (Pointer pointer : updated) {
            BaseClusterTreeNode baseClusterTreeNode = original.get(pointer);
            IndexTreeNodeIO.update(indexStorageManager, table, baseClusterTreeNode);
        }

        for (Pointer pointer : created) {
            IndexTreeNodeIO.remove(indexStorageManager, table, pointer);
        }
    }

    public static class Factory extends IndexIOSessionFactory {
        private static MemorySnapshotIndexIOSession.Factory instance;

        private Factory() {
        }

        public static synchronized MemorySnapshotIndexIOSession.Factory getInstance(){
            if (instance == null)
                instance = new MemorySnapshotIndexIOSession.Factory();
            return instance;
        }

        @Override
        public IndexIOSession create(IndexStorageManager indexStorageManager, int table) {
            return new MemorySnapshotIndexIOSession(indexStorageManager, table);
        }
    }

}
