package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.IndexMissingException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.data.IndexBinaryObject;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;


public class CachedIndexManagerDecorator<K extends Comparable<K>, V extends Comparable<V>> extends IndexManagerDecorator<K, V> {
    private final Cache<CacheID<K>, V> cache;
    private final AtomicInteger sizeCache = new AtomicInteger(0);

    public CachedIndexManagerDecorator(IndexManager<K, V> indexManager, int maxSize) {
        this(indexManager, CacheBuilder.newBuilder().maximumSize(maxSize).initialCapacity(10).build());
    }

    public CachedIndexManagerDecorator(IndexManager<K, V> indexManager, Cache<CacheID<K>, V> cache) {
        super(indexManager);
        this.cache = cache;
    }

    @Override
    public AbstractTreeNode<K> addIndex(K identifier, V value) throws InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject, IndexExistsException {
        AbstractTreeNode<K> baseClusterTreeNode = super.addIndex(identifier, value);
        cache.put(new CacheID<>(getIndexId(), identifier), value);
        if (sizeCache.get() > 0)
            sizeCache.incrementAndGet();
        return baseClusterTreeNode;
    }

    @Override
    public AbstractTreeNode<K> updateIndex(K identifier, V value) throws IndexExistsException, InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject, IndexMissingException {
        AbstractTreeNode<K> baseClusterTreeNode = super.addIndex(identifier, value);
        cache.put(new CacheID<>(getIndexId(), identifier), value);
        return baseClusterTreeNode;
    }

    @Override
    public Optional<V> getIndex(K identifier) throws InternalOperationException {
        V optionalPointer = cache.getIfPresent(new CacheID<>(getIndexId(), identifier));
        if (optionalPointer != null)
            return Optional.of(optionalPointer);
        Optional<V> output = super.getIndex(identifier);
        output.ifPresent(value -> cache.put(new CacheID<>(getIndexId(), identifier), value));
        return output;
    }

    @Override
    public boolean removeIndex(K identifier) throws InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject {
        if (super.removeIndex(identifier)) {
            cache.invalidate(new CacheID<>(getIndexId(), identifier));
            if (sizeCache.get() > 0) {
                sizeCache.decrementAndGet();
            }
            return true;
        }
        return false;
    }

    @Override
    public synchronized int size() throws InternalOperationException {
        int cachedSize = sizeCache.get();
        if (cachedSize > 0)
            return cachedSize;

        cachedSize = super.size();
        return cachedSize;
    }

    public record CacheID<K extends Comparable<K>>(int index, K key) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CacheID<?> cacheID = (CacheID<?>) o;
            return index == cacheID.index && Objects.equals(key, cacheID.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, key);
        }
    }

}
