package com.github.sepgh.testudo.storage;

import com.github.sepgh.testudo.index.Pointer;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class IndexStorageManagerDecorator implements IndexStorageManager {
    protected final IndexStorageManager decorated;

    public IndexStorageManagerDecorator(IndexStorageManager decorated) {
        this.decorated = decorated;
    }

    public CompletableFuture<Optional<NodeData>> getRoot(int table) throws InterruptedException {
        return this.decorated.getRoot(table);
    }

    public byte[] getEmptyNode() {
        return this.decorated.getEmptyNode();
    }

    public CompletableFuture<NodeData> readNode(int table, Pointer pointer) throws InterruptedException, IOException {
        return this.readNode(table, pointer.getPosition(), pointer.getChunk());
    }
    public CompletableFuture<NodeData> readNode(int table, long position, int chunk) throws InterruptedException, IOException {
        return this.decorated.readNode(table, position, chunk);
    }

    public CompletableFuture<NodeData> writeNewNode(int table, byte[] data, boolean isRoot) throws IOException, ExecutionException, InterruptedException {
        return this.decorated.writeNewNode(table, data, isRoot);
    }

    public  CompletableFuture<NodeData> writeNewNode(int table, byte[] data) throws IOException, ExecutionException, InterruptedException {
        return this.writeNewNode(table, data, false);
    }
    public  CompletableFuture<Integer> updateNode(int table, byte[] data, Pointer pointer) throws IOException, InterruptedException {
        return this.decorated.updateNode(table, data, pointer, false);
    }
    public CompletableFuture<Integer> updateNode(int table, byte[] data, Pointer pointer, boolean root) throws IOException, InterruptedException {
        return this.decorated.updateNode(table, data, pointer, root);
    }

    public void close() throws IOException {
        this.decorated.close();
    }

    public CompletableFuture<Integer> removeNode(int table, Pointer pointer) throws InterruptedException {
        return this.decorated.removeNode(table, pointer);
    }
}
