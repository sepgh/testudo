package com.github.sepgh.testudo.storage.index.session;

import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.IndexTreeNodeIO;
import com.github.sepgh.testudo.utils.KVSize;
import lombok.Getter;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class MemorySnapshotIndexIOSession<K extends Comparable<K>> implements IndexIOSession<K> {
    @Getter
    protected final IndexStorageManager indexStorageManager;
    protected final int indexId;
    protected final Set<Pointer> updated = new HashSet<>();
    protected final Map<Pointer, AbstractTreeNode<K>> pool = new HashMap<>();
    protected final Map<Pointer, AbstractTreeNode<K>> original = new HashMap<>();
    protected final List<Pointer> created = new LinkedList<>();
    protected final List<Pointer> deleted = new LinkedList<>();
    protected AbstractTreeNode<K> root;
    private final NodeFactory<K> nodeFactory;
    private final KVSize kvSize;

    public MemorySnapshotIndexIOSession(IndexStorageManager indexStorageManager, int indexId, NodeFactory<K> nodeFactory, KVSize kvSize) {
        this.indexStorageManager = indexStorageManager;
        this.indexId = indexId;
        this.nodeFactory = nodeFactory;
        this.kvSize = kvSize;
    }

    @Override
    public Optional<AbstractTreeNode<K>> getRoot() throws InternalOperationException {
        if (root == null){
            Optional<IndexStorageManager.NodeData> optional = null;
            try {
                optional = indexStorageManager.getRoot(indexId, kvSize).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new InternalOperationException(e);
            }
            if (optional.isPresent()){
                AbstractTreeNode<K> baseClusterTreeNode = nodeFactory.fromNodeData(optional.get());
                this.root = baseClusterTreeNode;
                return Optional.of(baseClusterTreeNode);
            }
        } else {
            return Optional.of(root);
        }

        return Optional.empty();
    }

    @Override
    public IndexStorageManager.NodeData write(AbstractTreeNode<K> node) throws InternalOperationException {
        IndexStorageManager.NodeData nodeData = null;
        try {
            nodeData = IndexTreeNodeIO.write(indexStorageManager, indexId, node).get();
        } catch (InterruptedException | ExecutionException | IOException e) {
            throw new InternalOperationException(e);
        }
        this.created.add(nodeData.pointer());
        this.pool.put(nodeData.pointer(), node);
        if (node.isRoot())
            root = node;
        return nodeData;
    }

    @Override
    public AbstractTreeNode<K> read(Pointer pointer) throws InternalOperationException {
        if (deleted.contains(pointer))
            return null;

        if (updated.contains(pointer))
            return pool.get(pointer);

        if (created.contains(pointer))
            return pool.get(pointer);

        AbstractTreeNode<K> baseClusterTreeNode = null;
        try {
            baseClusterTreeNode = IndexTreeNodeIO.read(indexStorageManager, indexId, pointer, nodeFactory, kvSize);
        } catch (ExecutionException | InterruptedException | IOException e) {
            throw new InternalOperationException(e);
        }
        pool.put(pointer, baseClusterTreeNode);

        byte[] copy = new byte[baseClusterTreeNode.getData().length];
        System.arraycopy(baseClusterTreeNode.getData(), 0, copy, 0, copy.length);
        original.put(pointer, nodeFactory.fromNodeData(new IndexStorageManager.NodeData(pointer, copy)));
        if (baseClusterTreeNode.isRoot())
            this.root = baseClusterTreeNode;
        return baseClusterTreeNode;
    }

    public final void update(AbstractTreeNode<K> node) throws InternalOperationException {
        pool.put(
                node.getPointer(),
                node
        );
        updated.add(node.getPointer());
        if (node.isRoot())
            root = node;
    }

    @Override
    public void remove(AbstractTreeNode<K> node) throws InternalOperationException {
        deleted.add(node.getPointer());
    }


    @Override
    public void commit() throws InternalOperationException {
        for (Pointer pointer : deleted) {
            try {
                IndexTreeNodeIO.remove(indexStorageManager, indexId, pointer, kvSize);
            } catch (ExecutionException | InterruptedException e) {
                try {
                    this.rollback();
                } catch (IOException | InterruptedException | ExecutionException ex) {
                    throw new InternalOperationException(ex);
                }
            }
        }

        try {
            for (Pointer pointer : updated) {
                try {
                    IndexTreeNodeIO.update(indexStorageManager, indexId, pool.get(pointer));
                } catch (ExecutionException e) {
                    throw new InternalOperationException(e);
                }
            }
        } catch (InterruptedException | IOException e) {
            try {
                this.rollback();
            } catch (IOException | InterruptedException | ExecutionException ex) {
                throw new InternalOperationException(ex);
            }
        }

    }

    protected void rollback() throws IOException, InterruptedException, ExecutionException {
        for (Pointer pointer : deleted) {
            IndexTreeNodeIO.update(indexStorageManager, indexId, original.get(pointer));
        }

        for (Pointer pointer : updated) {
            AbstractTreeNode<K> baseClusterTreeNode = original.get(pointer);
            IndexTreeNodeIO.update(indexStorageManager, indexId, baseClusterTreeNode);
        }

        for (Pointer pointer : created) {
            IndexTreeNodeIO.remove(indexStorageManager, indexId, pointer, kvSize);
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
        public <K extends Comparable<K>> IndexIOSession<K> create(IndexStorageManager indexStorageManager, int indexId, NodeFactory<K> nodeFactory, KVSize kvSize) {
            return new MemorySnapshotIndexIOSession<>(indexStorageManager, indexId, nodeFactory, kvSize);
        }
    }

}
