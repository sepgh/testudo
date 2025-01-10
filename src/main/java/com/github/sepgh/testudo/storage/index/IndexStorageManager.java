package com.github.sepgh.testudo.storage.index;

import com.github.sepgh.testudo.ds.KVSize;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManager;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface IndexStorageManager {
    CompletableFuture<Optional<NodeData>> getRoot(int indexId, KVSize kvSize) throws InternalOperationException;

    byte[] getEmptyNode(KVSize kvSize);
    default CompletableFuture<NodeData> readNode(int indexId, Pointer pointer, KVSize kvSize) throws InternalOperationException {
        return this.readNode(indexId, pointer.getPosition(), pointer.getChunk(), kvSize);
    }
    CompletableFuture<NodeData> readNode(int indexId, long position, int chunk, KVSize kvSize) throws InternalOperationException;

    CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data, boolean isRoot, KVSize size) throws InternalOperationException;
    default CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data, KVSize size) throws InternalOperationException {
        return this.writeNewNode(indexId, data, false, size);
    }
    default CompletableFuture<Void> updateNode(int indexId, byte[] data, Pointer pointer) throws InternalOperationException {
        return this.updateNode(indexId, data, pointer, false);
    }
    CompletableFuture<Void> updateNode(int indexId, byte[] data, Pointer pointer, boolean root) throws InternalOperationException;

    void close() throws InternalOperationException;

    CompletableFuture<Void> removeNode(int indexId, Pointer pointer, KVSize size) throws InternalOperationException;

    boolean exists(int indexId);

    IndexHeaderManager getIndexHeaderManager();

    default boolean supportsPurge(){return false;}
    default void purgeIndex(int indexId){}

    record NodeData(Pointer pointer, byte[] bytes){}
}
