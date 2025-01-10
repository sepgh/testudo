package com.github.sepgh.testudo.storage.index.session;

import com.github.sepgh.testudo.ds.KVSize;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.IndexTreeNodeIO;
import lombok.Getter;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class ImmediateCommitIndexIOSession<K extends Comparable<K>> implements IndexIOSession<K> {
    @Getter
    private final IndexStorageManager indexStorageManager;
    private final int indexId;
    private final NodeFactory<K> nodeFactory;
    private final KVSize kvSize;

    public ImmediateCommitIndexIOSession(IndexStorageManager indexStorageManager, int indexId, NodeFactory<K> nodeFactory, KVSize kvSize) {
        this.indexStorageManager = indexStorageManager;
        this.indexId = indexId;
        this.nodeFactory = nodeFactory;
        this.kvSize = kvSize;
    }

    @Override
    public Optional<AbstractTreeNode<K>> getRoot() throws InternalOperationException {
        try {
            Optional<IndexStorageManager.NodeData> optional = indexStorageManager.getRoot(indexId, kvSize).get();
            return optional.map(nodeFactory::fromNodeData);
        } catch (ExecutionException | InterruptedException e) {
            throw new InternalOperationException(e);
        }
    }

    @Override
    public IndexStorageManager.NodeData write(AbstractTreeNode<K> node) throws InternalOperationException {
        try {
            return IndexTreeNodeIO.write(indexStorageManager, indexId, node).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new InternalOperationException(e);
        }
    }

    @Override
    public AbstractTreeNode<K> read(Pointer pointer) throws InternalOperationException {
        return IndexTreeNodeIO.read(indexStorageManager, indexId, pointer, nodeFactory, kvSize);
    }

    @Override
    public final void update(AbstractTreeNode<K> node) throws InternalOperationException {
        IndexTreeNodeIO.update(indexStorageManager, indexId, node);
    }

    @Override
    public void remove(AbstractTreeNode<K> node) throws InternalOperationException {
        try {
            IndexTreeNodeIO.remove(indexStorageManager, indexId, node, kvSize);
        } catch (ExecutionException | InterruptedException e) {
            throw new InternalOperationException(e);
        }
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
        public <K extends Comparable<K>> IndexIOSession<K> create(IndexStorageManager indexStorageManager, int indexId, NodeFactory<K> nodeFactory, KVSize kvSize) {
            return new ImmediateCommitIndexIOSession<>(indexStorageManager, indexId, nodeFactory, kvSize);
        }
    }
}
