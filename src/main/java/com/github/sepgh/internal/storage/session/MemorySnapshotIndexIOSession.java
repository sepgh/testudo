package com.github.sepgh.internal.storage.session;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.BaseTreeNode;
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
    protected final Map<Pointer, BaseTreeNode> pool = new HashMap<>();
    protected final Map<Pointer, BaseTreeNode> original = new HashMap<>();
    protected final List<Pointer> created = new LinkedList<>();
    protected final List<Pointer> deleted = new LinkedList<>();
    protected BaseTreeNode root;

    public MemorySnapshotIndexIOSession(IndexStorageManager indexStorageManager, int table) {
        this.indexStorageManager = indexStorageManager;
        this.table = table;
    }

    @Override
    public Optional<BaseTreeNode> getRoot() throws ExecutionException, InterruptedException {
        if (root == null){
            Optional<IndexStorageManager.NodeData> optional = indexStorageManager.getRoot(table).get();
            if (optional.isPresent()){
                BaseTreeNode baseTreeNode = BaseTreeNode.fromNodeData(optional.get());
                this.root = baseTreeNode;
                return Optional.of(baseTreeNode);
            }
        } else {
            return Optional.of(root);
        }

        return Optional.empty();
    }

    @Override
    public IndexStorageManager.NodeData write(BaseTreeNode node) throws IOException, ExecutionException, InterruptedException {
        IndexStorageManager.NodeData nodeData = IndexTreeNodeIO.write(indexStorageManager, table, node).get();
        this.created.add(nodeData.pointer());
        this.pool.put(nodeData.pointer(), node);
        if (node.isRoot())
            root = node;
        return nodeData;
    }

    @Override
    public BaseTreeNode read(Pointer pointer) throws ExecutionException, InterruptedException {
        if (deleted.contains(pointer))
            return null;

        if (updated.contains(pointer))
            return pool.get(pointer);

        if (created.contains(pointer))
            return pool.get(pointer);

        BaseTreeNode baseTreeNode = IndexTreeNodeIO.read(indexStorageManager, table, pointer);
        pool.put(pointer, baseTreeNode);

        byte[] copy = new byte[baseTreeNode.getData().length];
        System.arraycopy(baseTreeNode.getData(), 0, copy, 0, copy.length);
        original.put(pointer, BaseTreeNode.fromNodeData(new IndexStorageManager.NodeData(pointer, copy)));
        if (baseTreeNode.isRoot())
            this.root = baseTreeNode;
        return baseTreeNode;
    }

    @Override
    public void update(BaseTreeNode... nodes) throws IOException, InterruptedException {
        for (BaseTreeNode node : nodes) {
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
    public void remove(BaseTreeNode node) throws ExecutionException, InterruptedException {
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
            BaseTreeNode baseTreeNode = original.get(pointer);
            IndexTreeNodeIO.update(indexStorageManager, table, baseTreeNode);
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
