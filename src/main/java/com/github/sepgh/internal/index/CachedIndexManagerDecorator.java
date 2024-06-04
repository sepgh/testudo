package com.github.sepgh.internal.index;

import com.github.sepgh.internal.index.tree.node.BaseTreeNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class CachedIndexManagerDecorator extends IndexManagerDecorator {
    private final Cache<TableIdentifier, Pointer> cache;

    public CachedIndexManagerDecorator(IndexManager indexManager, int maxSize) {
        this(indexManager, CacheBuilder.newBuilder().maximumSize(maxSize).initialCapacity(10).build());
    }
    public CachedIndexManagerDecorator(IndexManager indexManager, Cache<TableIdentifier, Pointer> cache) {
        super(indexManager);
        this.cache = cache;
    }

    @Override
    public BaseTreeNode addIndex(int table, long identifier, Pointer pointer) throws ExecutionException, InterruptedException, IOException {
        BaseTreeNode baseTreeNode = super.addIndex(table, identifier, pointer);
        cache.put(new TableIdentifier(table, identifier), pointer);
        return baseTreeNode;
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
            return true;
        }
        return false;
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
