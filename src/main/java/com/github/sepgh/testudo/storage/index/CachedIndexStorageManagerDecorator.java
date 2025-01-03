package com.github.sepgh.testudo.storage.index;

import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.ds.KVSize;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class CachedIndexStorageManagerDecorator extends IndexStorageManagerDecorator {
    private final Cache<IndexPointer, NodeData> cache;
    private final Map<Integer, NodeData> rootCache = new HashMap<>();

    public CachedIndexStorageManagerDecorator(IndexStorageManager decorated, int maxSize) {
        this(decorated, CacheBuilder.newBuilder().maximumSize(maxSize).initialCapacity(10).build());
    }

    public CachedIndexStorageManagerDecorator(IndexStorageManager decorated, Cache<IndexPointer, NodeData> cache) {
        super(decorated);
        this.cache = cache;
    }

    @Override
    public CompletableFuture<Optional<NodeData>> getRoot(int indexId, KVSize size) throws InterruptedException {

        synchronized (rootCache){
            NodeData nodeData = rootCache.get(indexId);
            if (nodeData != null){
                return CompletableFuture.completedFuture(Optional.of(nodeData));
            }
        }

        return super.getRoot(indexId, size).whenComplete((optionalNodeData, throwable) -> {
            synchronized (rootCache) {
                if (throwable != null && optionalNodeData.isPresent())
                    rootCache.put(indexId, optionalNodeData.get());
            }
        });
    }

    @Override
    public CompletableFuture<NodeData> readNode(int indexId, long position, int chunk, KVSize size) throws InterruptedException, IOException {
        NodeData optionalNodeData = cache.getIfPresent(new IndexPointer(indexId, new Pointer(Pointer.TYPE_NODE, position, chunk)));
        if (optionalNodeData != null){
            return CompletableFuture.completedFuture(optionalNodeData);
        }
        return super.readNode(indexId, position, chunk, size).whenComplete((nodeData, throwable) -> {
            if (throwable == null){
                cache.put(new IndexPointer(indexId, nodeData.pointer()), nodeData);
            }
        });
    }

    public CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data, boolean isRoot, KVSize size) throws IOException, ExecutionException, InterruptedException {
        return super.writeNewNode(indexId, data, isRoot, size).whenComplete((nodeData, throwable) -> {
            if (throwable == null){
                cache.put(new IndexPointer(indexId, nodeData.pointer()), nodeData);
                synchronized (rootCache) {
                    if (isRoot)
                        rootCache.put(indexId, nodeData);
                }
            }
        });
    }

    public CompletableFuture<Void> updateNode(int indexId, byte[] data, Pointer pointer, boolean root) throws IOException, InterruptedException {
        return super.updateNode(indexId, data, pointer, root).whenComplete((integer, throwable) -> {
            if (throwable == null) {
                NodeData nodeData = new NodeData(pointer, data);
                cache.put(new IndexPointer(indexId, pointer), nodeData);
                synchronized (rootCache) {
                    if (root)
                        rootCache.put(indexId, nodeData);
                }
            }
        });
    }

    public void close() throws IOException {
        this.cache.invalidateAll();
        super.close();
    }

    public CompletableFuture<Void> removeNode(int indexId, Pointer pointer, KVSize size) throws InterruptedException {
        return super.removeNode(indexId, pointer, size).whenComplete((integer, throwable) -> {
            cache.invalidate(new IndexPointer(indexId, pointer));
            synchronized (rootCache){
                if (rootCache.get(indexId).pointer().equals(pointer)){
                    rootCache.remove(indexId);
                }
            }
        });
    }

    public record IndexPointer(int indexId, Pointer pointer) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IndexPointer that = (IndexPointer) o;
            return indexId == that.indexId && Objects.equals(pointer, that.pointer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexId, pointer);
        }
    }
}
