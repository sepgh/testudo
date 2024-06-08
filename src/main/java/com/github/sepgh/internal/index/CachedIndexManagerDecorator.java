package com.github.sepgh.internal.index;

import com.github.sepgh.internal.index.tree.node.cluster.BaseClusterTreeNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class CachedIndexManagerDecorator<K extends Comparable<K>> extends IndexManagerDecorator<K> {
    private final Cache<TableIdentifier<K>, Pointer> cache;
    private final Map<Integer, Integer> sizeCache;

    public CachedIndexManagerDecorator(IndexManager<K> indexManager, int maxSize) {
        this(indexManager, CacheBuilder.newBuilder().maximumSize(maxSize).initialCapacity(10).build());
    }
    public CachedIndexManagerDecorator(IndexManager<K> indexManager, Cache<TableIdentifier<K>, Pointer> cache) {
        super(indexManager);
        this.cache = cache;
        this.sizeCache = new ConcurrentHashMap<>();
    }

    @Override
    public BaseClusterTreeNode<K> addIndex(int table, K identifier, Pointer pointer) throws ExecutionException, InterruptedException, IOException {
        BaseClusterTreeNode<K> baseClusterTreeNode = super.addIndex(table, identifier, pointer);
        cache.put(new TableIdentifier<>(table, identifier), pointer);
        sizeCache.computeIfPresent(table, (k, v) -> v + 1);
        return baseClusterTreeNode;
    }

    @Override
    public Optional<Pointer> getIndex(int table, K identifier) throws ExecutionException, InterruptedException, IOException {
        TableIdentifier<K> lookup = new TableIdentifier<>(table, identifier);
        Pointer optionalPointer = cache.getIfPresent(lookup);
        if (optionalPointer != null)
            return Optional.of(optionalPointer);
        Optional<Pointer> output = super.getIndex(table, identifier);
        output.ifPresent(pointer -> cache.put(lookup, pointer));
        return output;
    }

    @Override
    public boolean removeIndex(int table, K identifier) throws ExecutionException, InterruptedException, IOException {
        if (super.removeIndex(table, identifier)) {
            cache.invalidate(new TableIdentifier<>(table, identifier));
            sizeCache.computeIfPresent(table, (k, v) -> v - 1);
            return true;
        }
        return false;
    }

    @Override
    public int size(int table) {
        return sizeCache.computeIfAbsent(table, key -> {
            try {
                return super.size(table);
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public record TableIdentifier<K extends Comparable<K>>(int table, K identifier){
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableIdentifier<K> that = (TableIdentifier<K>) o;
            return table == that.table && identifier == that.identifier;
        }

        @Override
        public int hashCode() {
            return Objects.hash(table, identifier);
        }
    }

}
