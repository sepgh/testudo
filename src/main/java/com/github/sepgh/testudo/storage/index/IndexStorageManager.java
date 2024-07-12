package com.github.sepgh.testudo.storage.index;

import com.github.sepgh.testudo.index.Pointer;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface IndexStorageManager {
    CompletableFuture<Optional<NodeData>> getRoot(int indexId) throws InterruptedException;

    byte[] getEmptyNode();
    default CompletableFuture<NodeData> readNode(int indexId, Pointer pointer) throws InterruptedException, IOException {
        return this.readNode(indexId, pointer.getPosition(), pointer.getChunk());
    }
    CompletableFuture<NodeData> readNode(int indexId, long position, int chunk) throws InterruptedException, IOException;

    CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data, boolean isRoot) throws IOException, ExecutionException, InterruptedException;
    default CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data) throws IOException, ExecutionException, InterruptedException {
        return this.writeNewNode(indexId, data, false);
    }
    default CompletableFuture<Integer> updateNode(int indexId, byte[] data, Pointer pointer) throws IOException, InterruptedException {
        return this.updateNode(indexId, data, pointer, false);
    }
    CompletableFuture<Integer> updateNode(int indexId, byte[] data, Pointer pointer, boolean root) throws IOException, InterruptedException;

    void close() throws IOException;

    CompletableFuture<Integer> removeNode(int indexId, Pointer pointer) throws InterruptedException;

    boolean exists(int indexId);

    record NodeData(Pointer pointer, byte[] bytes){}
}
