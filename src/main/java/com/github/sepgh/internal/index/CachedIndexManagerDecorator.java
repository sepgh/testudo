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

public class CachedIndexManagerDecorator extends IndexManagerDecorator {
    private final Cache<TableIdentifier, Pointer> cache;
    private final Map<Integer, Integer> sizeCache;

    public CachedIndexManagerDecorator(IndexManager indexManager, int maxSize) {
        this(indexManager, CacheBuilder.newBuilder().maximumSize(maxSize).initialCapacity(10).build());
    }
    public CachedIndexManagerDecorator(IndexManager indexManager, Cache<TableIdentifier, Pointer> cache) {
        super(indexManager);
        this.cache = cache;
        this.sizeCache = new ConcurrentHashMap<>();
    }

    @Override
    public BaseClusterTreeNode addIndex(int table, long identifier, Pointer pointer) throws ExecutionException, InterruptedException, IOException {
        BaseClusterTreeNode baseClusterTreeNode = super.addIndex(table, identifier, pointer);
        cache.put(new TableIdentifier(table, identifier), pointer);
        sizeCache.computeIfPresent(table, (k, v) -> v + 1);
        return baseClusterTreeNode;
    }

    @Override
    public Optional<Pointer> getIndex(int table, long identifier) throws ExecutionException, InterruptedException, IOException {
        TableIdentifier lookup = new TableIdentifier(table, identifier);
        Pointer optionalPointer = cache.getIfPresent(lookup);
        if (optionalPointer != null)
            return Optional.of(optionalPointer);
        Optional<Pointer> output = super.getIndex(table, identifier);
        output.ifPresent(pointer -> cache.put(lookup, pointer));
        return output;
    }

    @Override
    public boolean removeIndex(int table, long identifier) throws ExecutionException, InterruptedException, IOException {
        if (super.removeIndex(table, identifier)) {
            cache.invalidate(new TableIdentifier(table, identifier));
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

    public record TableIdentifier(int table, long identifier){
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableIdentifier that = (TableIdentifier) o;
            return table == that.table && identifier == that.identifier;
        }

        @Override
        public int hashCode() {
            return Objects.hash(table, identifier);
        }
    }

}
