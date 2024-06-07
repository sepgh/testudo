package com.github.sepgh.internal.storage;

import com.github.sepgh.internal.index.Pointer;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface IndexStorageManager {
    CompletableFuture<Optional<NodeData>> getRoot(int table) throws InterruptedException;

    byte[] getEmptyNode();
    default CompletableFuture<NodeData> readNode(int table, Pointer pointer) throws InterruptedException, IOException {
        return this.readNode(table, pointer.getPosition(), pointer.getChunk());
    }
    CompletableFuture<NodeData> readNode(int table, long position, int chunk) throws InterruptedException, IOException;

    CompletableFuture<NodeData> writeNewNode(int table, byte[] data, boolean isRoot) throws IOException, ExecutionException, InterruptedException;
    default CompletableFuture<NodeData> writeNewNode(int table, byte[] data) throws IOException, ExecutionException, InterruptedException {
        return this.writeNewNode(table, data, false);
    }
    default CompletableFuture<Integer> updateNode(int table, byte[] data, Pointer pointer) throws IOException, InterruptedException {
        return this.updateNode(table, data, pointer, false);
    }
    CompletableFuture<Integer> updateNode(int table, byte[] data, Pointer pointer, boolean root) throws IOException, InterruptedException;

    void close() throws IOException;

    CompletableFuture<Integer> removeNode(int table, Pointer pointer) throws InterruptedException;

    record NodeData(Pointer pointer, byte[] bytes){}
}
