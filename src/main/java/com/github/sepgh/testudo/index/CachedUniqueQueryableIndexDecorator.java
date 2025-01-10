package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.ds.CacheID;
import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


public class CachedUniqueQueryableIndexDecorator<K extends Comparable<K>, V> extends UniqueQueryableIndexDecorator<K, V> {
    private final Cache<CacheID<K>, V> cache;
    private final AtomicInteger sizeCache = new AtomicInteger(0);
    private AtomicReference<K> currentIncrementalKey = null;
    private final IndexBinaryObjectFactory<K> kIndexBinaryObjectFactory;
    private final boolean supportsNextKey;

    public CachedUniqueQueryableIndexDecorator(UniqueQueryableIndex<K, V> decorated, int maxSize) {
        this(decorated, maxSize, null);
    }

    public CachedUniqueQueryableIndexDecorator(UniqueQueryableIndex<K, V> decorated, Cache<CacheID<K>, V> cache) {
        this(decorated, cache, null);
    }

    public CachedUniqueQueryableIndexDecorator(UniqueQueryableIndex<K, V> decorated, int maxSize, @Nullable IndexBinaryObjectFactory<K> kIndexBinaryObjectFactory) {
        this(decorated, CacheBuilder.newBuilder().maximumSize(maxSize).initialCapacity(10).build(), kIndexBinaryObjectFactory);
    }

    public CachedUniqueQueryableIndexDecorator(UniqueQueryableIndex<K, V> decorated, Cache<CacheID<K>, V> cache, @Nullable IndexBinaryObjectFactory<K> kIndexBinaryObjectFactory) {
        super(decorated);
        this.cache = cache;
        this.kIndexBinaryObjectFactory = kIndexBinaryObjectFactory;
        this.supportsNextKey = kIndexBinaryObjectFactory != null;
    }

    @Override
    public AbstractTreeNode<K> addIndex(K identifier, V value) throws InternalOperationException, IndexExistsException {
        AbstractTreeNode<K> baseClusterTreeNode = super.addIndex(identifier, value);
        cache.put(new CacheID<>(getIndexId(), identifier), value);
        if (sizeCache.get() > 0)
            sizeCache.incrementAndGet();
        return baseClusterTreeNode;
    }

    @Override
    public AbstractTreeNode<K> addOrUpdateIndex(K identifier, V value) throws InternalOperationException {
        AbstractTreeNode<K> baseClusterTreeNode = super.addOrUpdateIndex(identifier, value);
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
    public boolean removeIndex(K identifier) throws InternalOperationException {
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

    @Override
    public K nextKey() throws InternalOperationException, DeserializationException {
        synchronized (this){
            if (currentIncrementalKey == null) {
                this.currentIncrementalKey = new AtomicReference<>();
                this.currentIncrementalKey.set(super.nextKey());
                return this.currentIncrementalKey.get();
            }
        }

        K next = this.kIndexBinaryObjectFactory.create(this.currentIncrementalKey.get()).getNext();
        this.currentIncrementalKey.set(next);

        return next;
    }

    @Override
    public boolean supportIncrement() {
        return this.supportsNextKey && super.supportIncrement();
    }
}
