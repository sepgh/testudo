package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

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
    public AbstractTreeNode<K> addIndex(int index, K identifier, V value) throws InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException {
        AbstractTreeNode<K> baseClusterTreeNode = super.addIndex(index, identifier, value);
        cache.put(new TableIdentifier<>(index, identifier), value);
        sizeCache.computeIfPresent(index, (k, v) -> v + 1);
        return baseClusterTreeNode;
    }

    @Override
    public Optional<V> getIndex(int index, K identifier) throws InternalOperationException {
        TableIdentifier<K> lookup = new TableIdentifier<>(index, identifier);
        V optionalPointer = cache.getIfPresent(lookup);
        if (optionalPointer != null)
            return Optional.of(optionalPointer);
        Optional<V> output = super.getIndex(index, identifier);
        output.ifPresent(value -> cache.put(lookup, value));
        return output;
    }

    @Override
    public boolean removeIndex(int index, K identifier) throws InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        if (super.removeIndex(index, identifier)) {
            cache.invalidate(new TableIdentifier<>(index, identifier));
            sizeCache.computeIfPresent(index, (k, v) -> v - 1);
            return true;
        }
        return false;
    }

    @Override
    public int size(int index) throws InternalOperationException {
        AtomicReference<InternalOperationException> atomicExceptionReference = new AtomicReference<>();
        int size = sizeCache.computeIfAbsent(index, key -> {
            try {
                return super.size(index);
            } catch (InternalOperationException e) {
                atomicExceptionReference.set(e);
                return -1;
            }
        });

        InternalOperationException exception = atomicExceptionReference.get();
        if (exception != null)
            throw exception;

        return size;
    }

    public record TableIdentifier<K extends Comparable<K>>(int index, K identifier){
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableIdentifier<K> that = (TableIdentifier<K>) o;
            return index == that.index && identifier == that.identifier;
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, identifier);
        }
    }

}
