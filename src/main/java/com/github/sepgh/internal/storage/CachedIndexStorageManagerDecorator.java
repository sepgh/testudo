package com.github.sepgh.internal.storage;

import com.github.sepgh.internal.index.Pointer;
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
    private final Cache<TablePointer, NodeData> cache;
    private final Map<Integer, NodeData> rootCache = new HashMap<>();

    public CachedIndexStorageManagerDecorator(IndexStorageManager decorated, int maxSize) {
        this(decorated, CacheBuilder.newBuilder().maximumSize(maxSize).initialCapacity(10).build());
    }

    public CachedIndexStorageManagerDecorator(IndexStorageManager decorated, Cache<TablePointer, NodeData> cache) {
        super(decorated);
        this.cache = cache;
    }

    public CompletableFuture<Optional<NodeData>> getRoot(int table) throws InterruptedException {

        synchronized (rootCache){
            NodeData nodeData = rootCache.get(table);
            if (nodeData != null){
                return CompletableFuture.completedFuture(Optional.of(nodeData));
            }
        }

        return super.getRoot(table).whenComplete((optionalNodeData, throwable) -> {
            synchronized (rootCache) {
                if (throwable != null && optionalNodeData.isPresent())
                    rootCache.put(table, optionalNodeData.get());
            }
        });
    }

    public CompletableFuture<NodeData> readNode(int table, long position, int chunk) throws InterruptedException, IOException {
        NodeData optionalNodeData = cache.getIfPresent(new TablePointer(table, new Pointer(Pointer.TYPE_NODE, position, chunk)));
        if (optionalNodeData != null){
            return CompletableFuture.completedFuture(optionalNodeData);
        }
        return super.readNode(table, position, chunk).whenComplete((nodeData, throwable) -> {
            if (throwable == null){
                cache.put(new TablePointer(table, nodeData.pointer()), nodeData);
            }
        });
    }

    public CompletableFuture<NodeData> writeNewNode(int table, byte[] data, boolean isRoot) throws IOException, ExecutionException, InterruptedException {
        return super.writeNewNode(table, data, isRoot).whenComplete((nodeData, throwable) -> {
            if (throwable == null){
                cache.put(new TablePointer(table, nodeData.pointer()), nodeData);
                synchronized (rootCache) {
                    if (isRoot)
                        rootCache.put(table, nodeData);
                }
            }
        });
    }

    public CompletableFuture<Integer> updateNode(int table, byte[] data, Pointer pointer, boolean root) throws IOException, InterruptedException {
        return super.updateNode(table, data, pointer, root).whenComplete((integer, throwable) -> {
            if (throwable == null) {
                NodeData nodeData = new NodeData(pointer, data);
                cache.put(new TablePointer(table, pointer), nodeData);
                synchronized (rootCache) {
                    if (root)
                        rootCache.put(table, nodeData);
                }
            }
        });
    }

    public void close() throws IOException {
        this.cache.invalidateAll();
        super.close();
    }

    public CompletableFuture<Integer> removeNode(int table, Pointer pointer) throws InterruptedException {
        return super.removeNode(table, pointer).whenComplete((integer, throwable) -> {
            cache.invalidate(new TablePointer(table, pointer));
            synchronized (rootCache){
                if (rootCache.get(table).pointer().equals(pointer)){
                    rootCache.remove(table);
                }
            }
        });
    }

    public record TablePointer(int table, Pointer pointer) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TablePointer that = (TablePointer) o;
            return table == that.table && Objects.equals(pointer, that.pointer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(table, pointer);
        }
    }
}
