package com.github.sepgh.testudo.storage.index;

import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.ds.KVSize;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface IndexStorageManager {
    CompletableFuture<Optional<NodeData>> getRoot(int indexId, KVSize kvSize) throws InterruptedException;

    byte[] getEmptyNode(KVSize kvSize);
    default CompletableFuture<NodeData> readNode(int indexId, Pointer pointer, KVSize kvSize) throws InterruptedException, IOException {
        return this.readNode(indexId, pointer.getPosition(), pointer.getChunk(), kvSize);
    }
    CompletableFuture<NodeData> readNode(int indexId, long position, int chunk, KVSize kvSize) throws InterruptedException, IOException;

    CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data, boolean isRoot, KVSize size) throws IOException, ExecutionException, InterruptedException;
    default CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data, KVSize size) throws IOException, ExecutionException, InterruptedException {
        return this.writeNewNode(indexId, data, false, size);
    }
    default CompletableFuture<Void> updateNode(int indexId, byte[] data, Pointer pointer) throws IOException, InterruptedException {
        return this.updateNode(indexId, data, pointer, false);
    }
    CompletableFuture<Void> updateNode(int indexId, byte[] data, Pointer pointer, boolean root) throws IOException, InterruptedException;

    void close() throws IOException;

    CompletableFuture<Void> removeNode(int indexId, Pointer pointer, KVSize size) throws InterruptedException;

    boolean exists(int indexId);

    default boolean supportsPurge(){return false;}
    default void purgeIndex(int indexId){}

    record NodeData(Pointer pointer, byte[] bytes){}
}
