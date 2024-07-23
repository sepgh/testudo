package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class CachedIndexManagerDecorator<K extends Comparable<K>, V extends Comparable<V>> extends IndexManagerDecorator<K, V> {
    private final Cache<K, V> cache;
    private final AtomicInteger sizeCache = new AtomicInteger(0);

    public CachedIndexManagerDecorator(IndexManager<K, V> indexManager, int maxSize) {
        this(indexManager, CacheBuilder.newBuilder().maximumSize(maxSize).initialCapacity(10).build());
    }
    public CachedIndexManagerDecorator(IndexManager<K, V> indexManager, Cache<K, V> cache) {
        super(indexManager);
        this.cache = cache;
    }

    @Override
    public AbstractTreeNode<K> addIndex(K identifier, V value) throws InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException {
        AbstractTreeNode<K> baseClusterTreeNode = super.addIndex(identifier, value);
        cache.put(identifier, value);
        sizeCache.incrementAndGet();
        return baseClusterTreeNode;
    }

    @Override
    public Optional<V> getIndex(K identifier) throws InternalOperationException {
        V optionalPointer = cache.getIfPresent(identifier);
        if (optionalPointer != null)
            return Optional.of(optionalPointer);
        Optional<V> output = super.getIndex(identifier);
        output.ifPresent(value -> cache.put(identifier, value));
        return output;
    }

    @Override
    public boolean removeIndex(K identifier) throws InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        if (super.removeIndex(identifier)) {
            cache.invalidate(identifier);
            sizeCache.decrementAndGet();
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

}
