package com.github.sepgh.testudo.storage.index;

import com.github.sepgh.testudo.ds.KVSize;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManager;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class IndexStorageManagerDecorator implements IndexStorageManager {
    protected final IndexStorageManager decorated;

    public IndexStorageManagerDecorator(IndexStorageManager decorated) {
        this.decorated = decorated;
    }

    public CompletableFuture<Optional<NodeData>> getRoot(int indexId, KVSize size) throws InternalOperationException {
        return this.decorated.getRoot(indexId, size);
    }

    public byte[] getEmptyNode(KVSize size) {
        return this.decorated.getEmptyNode(size);
    }

    public CompletableFuture<NodeData> readNode(int indexId, Pointer pointer, KVSize size) throws InternalOperationException {
        return this.readNode(indexId, pointer.getPosition(), pointer.getChunk(), size);
    }
    public CompletableFuture<NodeData> readNode(int indexId, long position, int chunk, KVSize size) throws InternalOperationException {
        return this.decorated.readNode(indexId, position, chunk, size);
    }

    public CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data, boolean isRoot, KVSize size) throws InternalOperationException {
        return this.decorated.writeNewNode(indexId, data, isRoot, size);
    }

    public  CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data, KVSize size) throws InternalOperationException {
        return this.writeNewNode(indexId, data, false, size);
    }
    public  CompletableFuture<Void> updateNode(int indexId, byte[] data, Pointer pointer) throws InternalOperationException {
        return this.decorated.updateNode(indexId, data, pointer, false);
    }
    public CompletableFuture<Void> updateNode(int indexId, byte[] data, Pointer pointer, boolean root) throws InternalOperationException {
        return this.decorated.updateNode(indexId, data, pointer, root);
    }

    public void close() throws InternalOperationException {
        this.decorated.close();
    }

    public CompletableFuture<Void> removeNode(int indexId, Pointer pointer, KVSize size) throws InternalOperationException {
        return this.decorated.removeNode(indexId, pointer, size);
    }

    @Override
    public boolean exists(int indexId) {
        return this.decorated.exists(indexId);
    }

    @Override
    public IndexHeaderManager getIndexHeaderManager() {
        return decorated.getIndexHeaderManager();
    }

    @Override
    public void purgeIndex(int indexId) {
        this.decorated.purgeIndex(indexId);
    }

    @Override
    public boolean supportsPurge() {
        return this.decorated.supportsPurge();
    }
}
