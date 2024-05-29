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
    protected final Map<Pointer, BaseTreeNode> update = new HashMap<>();
    private final List<BaseTreeNode> sortedUpdate = new LinkedList<>();
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
        Optional<BaseTreeNode> output;
        if (root == null){
            Optional<IndexStorageManager.NodeData> optional = indexStorageManager.getRoot(table).get();
            output = optional.map(BaseTreeNode::fromNodeData);
        } else {
            output = Optional.empty();
        }
        output.ifPresent(baseTreeNode -> this.root = baseTreeNode);

        return output;
    }

    @Override
    public IndexStorageManager.NodeData write(BaseTreeNode node) throws IOException, ExecutionException, InterruptedException {
        IndexStorageManager.NodeData nodeData = IndexTreeNodeIO.write(indexStorageManager, table, node).get();
        this.created.add(nodeData.pointer());
        this.original.put(nodeData.pointer(), node);
        if (node.isRoot())
            root = node;
        return nodeData;
    }

    @Override
    public BaseTreeNode read(Pointer pointer) throws ExecutionException, InterruptedException {
        if (deleted.contains(pointer))
            return null;

        if (update.containsKey(pointer))
            return update.get(pointer);

        if (created.contains(pointer))
            return original.get(pointer);

        BaseTreeNode baseTreeNode = IndexTreeNodeIO.read(indexStorageManager, table, pointer);
        original.put(pointer, baseTreeNode);
        if (baseTreeNode.isRoot())
            this.root = baseTreeNode;
        return baseTreeNode;
    }

    @Override
    public void update(BaseTreeNode... nodes) throws IOException, InterruptedException {
        for (BaseTreeNode node : nodes) {
            BaseTreeNode baseTreeNode = update.putIfAbsent(node.getPointer(), node);
            if (baseTreeNode == null){
                sortedUpdate.addLast(node);
            }
            if (node.isRoot())
                root = node;
        }
    }

    @Override
    public void remove(BaseTreeNode node) throws ExecutionException, InterruptedException {
        deleted.add(node.getPointer());
    }


    @Override
    public void commit() throws InterruptedException, IOException {
        for (Pointer pointer : deleted) {
            try {
                IndexTreeNodeIO.remove(indexStorageManager, table, pointer);
            } catch (ExecutionException | InterruptedException e) {
                this.rollback();
            }
        }

        try {
            for (BaseTreeNode baseTreeNode : sortedUpdate) {
                IndexTreeNodeIO.update(indexStorageManager, table, baseTreeNode);
            }
        } catch (InterruptedException | IOException e) {
            this.rollback();
        }

    }

    protected void rollback() throws IOException, InterruptedException {
        System.out.println("Rollback is called!!!!!");
        for (Pointer pointer : deleted) {
            IndexTreeNodeIO.update(indexStorageManager, table, original.get(pointer));
        }

        for (BaseTreeNode baseTreeNode : sortedUpdate) {
            IndexTreeNodeIO.update(indexStorageManager, table, original.get(baseTreeNode.getPointer()));
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
