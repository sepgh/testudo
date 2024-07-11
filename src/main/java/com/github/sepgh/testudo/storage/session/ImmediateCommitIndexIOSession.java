package com.github.sepgh.testudo.storage.session;

import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.storage.IndexStorageManager;
import com.github.sepgh.testudo.storage.IndexTreeNodeIO;
import lombok.Getter;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class ImmediateCommitIndexIOSession<K extends Comparable<K>> implements IndexIOSession<K> {
    @Getter
    private final IndexStorageManager indexStorageManager;
    private final int indexId;
    private final NodeFactory<K> nodeFactory;

    public ImmediateCommitIndexIOSession(IndexStorageManager indexStorageManager, int indexId, NodeFactory<K> nodeFactory) {
        this.indexStorageManager = indexStorageManager;
        this.indexId = indexId;
        this.nodeFactory = nodeFactory;
    }

    @Override
    public Optional<AbstractTreeNode<K>> getRoot() throws InternalOperationException {
        try {
            Optional<IndexStorageManager.NodeData> optional = indexStorageManager.getRoot(indexId).get();
            return optional.map(nodeFactory::fromNodeData);
        } catch (ExecutionException | InterruptedException e) {
            throw new InternalOperationException(e);
        }
    }

    @Override
    public IndexStorageManager.NodeData write(AbstractTreeNode<K> node) throws InternalOperationException {
        try {
            return IndexTreeNodeIO.write(indexStorageManager, indexId, node).get();
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw new InternalOperationException(e);
        }
    }

    @Override
    public AbstractTreeNode<K> read(Pointer pointer) throws InternalOperationException {
        try {
            return IndexTreeNodeIO.read(indexStorageManager, indexId, pointer, nodeFactory);
        } catch (ExecutionException | InterruptedException | IOException e) {
            throw new InternalOperationException(e);
        }
    }

    @Override
    public final void update(AbstractTreeNode<K> node) throws InternalOperationException {
        try {
            IndexTreeNodeIO.update(indexStorageManager, indexId, node);
        } catch (InterruptedException | IOException | ExecutionException e) {
            throw new InternalOperationException(e);
        }
    }

    @Override
    public void remove(AbstractTreeNode<K> node) throws InternalOperationException {
        try {
            IndexTreeNodeIO.remove(indexStorageManager, indexId, node);
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
        public <K extends Comparable<K>> IndexIOSession<K> create(IndexStorageManager indexStorageManager, int indexId, NodeFactory<K> nodeFactory) {
            return new ImmediateCommitIndexIOSession<>(indexStorageManager, indexId, nodeFactory);
        }
    }
}
