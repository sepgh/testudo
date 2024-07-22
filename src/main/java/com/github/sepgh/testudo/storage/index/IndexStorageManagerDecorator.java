package com.github.sepgh.testudo.storage.index;

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

    public CompletableFuture<Optional<NodeData>> getRoot(int indexId) throws InterruptedException {
        return this.decorated.getRoot(indexId);
    }

    public byte[] getEmptyNode() {
        return this.decorated.getEmptyNode();
    }

    public CompletableFuture<NodeData> readNode(int indexId, Pointer pointer) throws InterruptedException, IOException {
        return this.readNode(indexId, pointer.getPosition(), pointer.getChunk());
    }
    public CompletableFuture<NodeData> readNode(int indexId, long position, int chunk) throws InterruptedException, IOException {
        return this.decorated.readNode(indexId, position, chunk);
    }

    public CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data, boolean isRoot) throws IOException, ExecutionException, InterruptedException {
        return this.decorated.writeNewNode(indexId, data, isRoot);
    }

    public  CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data) throws IOException, ExecutionException, InterruptedException {
        return this.writeNewNode(indexId, data, false);
    }
    public  CompletableFuture<Integer> updateNode(int indexId, byte[] data, Pointer pointer) throws IOException, InterruptedException {
        return this.decorated.updateNode(indexId, data, pointer, false);
    }
    public CompletableFuture<Integer> updateNode(int indexId, byte[] data, Pointer pointer, boolean root) throws IOException, InterruptedException {
        return this.decorated.updateNode(indexId, data, pointer, root);
    }

    public void close() throws IOException {
        this.decorated.close();
    }

    public CompletableFuture<Integer> removeNode(int indexId, Pointer pointer) throws InterruptedException {
        return this.decorated.removeNode(indexId, pointer);
    }

    @Override
    public boolean exists(int indexId) {
        return this.decorated.exists(indexId);
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
