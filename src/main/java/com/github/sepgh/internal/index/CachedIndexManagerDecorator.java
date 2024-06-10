package com.github.sepgh.internal.index;

import com.github.sepgh.internal.index.tree.node.AbstractTreeNode;
import com.github.sepgh.internal.index.tree.node.data.NodeData;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class CachedIndexManagerDecorator<K extends Comparable<K>, V extends Comparable<V>> extends IndexManagerDecorator<K, V> {
    private final Cache<TableIdentifier<K>, V> cache;
    private final Map<Integer, Integer> sizeCache;

    public CachedIndexManagerDecorator(IndexManager<K, V> indexManager, int maxSize) {
        this(indexManager, CacheBuilder.newBuilder().maximumSize(maxSize).initialCapacity(10).build());
    }
    public CachedIndexManagerDecorator(IndexManager<K, V> indexManager, Cache<TableIdentifier<K>, V> cache) {
        super(indexManager);
        this.cache = cache;
        this.sizeCache = new ConcurrentHashMap<>();
    }

    @Override
    public AbstractTreeNode<K> addIndex(int table, K identifier, V value) throws ExecutionException, InterruptedException, IOException, NodeData.InvalidValueForNodeInnerObj {
        AbstractTreeNode<K> baseClusterTreeNode = super.addIndex(table, identifier, value);
        cache.put(new TableIdentifier<>(table, identifier), value);
        sizeCache.computeIfPresent(table, (k, v) -> v + 1);
        return baseClusterTreeNode;
    }

    @Override
    public Optional<V> getIndex(int table, K identifier) throws ExecutionException, InterruptedException, IOException {
        TableIdentifier<K> lookup = new TableIdentifier<>(table, identifier);
        V optionalPointer = cache.getIfPresent(lookup);
        if (optionalPointer != null)
            return Optional.of(optionalPointer);
        Optional<V> output = super.getIndex(table, identifier);
        output.ifPresent(value -> cache.put(lookup, value));
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
