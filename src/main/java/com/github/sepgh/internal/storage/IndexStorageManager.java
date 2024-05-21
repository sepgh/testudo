package com.github.sepgh.internal.storage;

import com.github.sepgh.internal.tree.Pointer;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface IndexStorageManager {
    CompletableFuture<NodeData> fillRoot(int table, byte[] data);

    CompletableFuture<Optional<NodeData>> getRoot(int table);

    byte[] getEmptyNode();
    default CompletableFuture<NodeData> readNode(int table, Pointer pointer) {
        return this.readNode(table, pointer.getPosition(), pointer.getChunk());
    }
    CompletableFuture<NodeData> readNode(int table, long position, int chunk);

    CompletableFuture<NodeData> writeNewNode(int table, byte[] data, boolean isRoot) throws IOException, ExecutionException, InterruptedException;
    default CompletableFuture<NodeData> writeNewNode(int table, byte[] data) throws IOException, ExecutionException, InterruptedException {
        return this.writeNewNode(table, data, false);
    }
    CompletableFuture<Integer> updateNode(int table, byte[] data, Pointer pointer);

    void close() throws IOException;

    CompletableFuture<Integer> removeNode(int table, Pointer pointer);

    record NodeData(Pointer pointer, byte[] bytes){}
}
