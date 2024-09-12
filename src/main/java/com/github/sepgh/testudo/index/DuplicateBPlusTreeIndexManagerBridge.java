package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.BPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.data.IndexBinaryObject;
import com.github.sepgh.testudo.index.tree.node.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.util.ListIterator;
import java.util.Optional;


public class DuplicateBPlusTreeIndexManagerBridge<K extends Comparable<K>, V extends Number & Comparable<V>> implements DuplicateIndexManager<K, V> {
    private final BPlusTreeUniqueTreeIndexManager<K, Pointer> indexManager;
    private final IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory;
    private final DatabaseStorageManager databaseStorageManager;
    private final EngineConfig engineConfig;

    public DuplicateBPlusTreeIndexManagerBridge(EngineConfig engineConfig, BPlusTreeUniqueTreeIndexManager<K, Pointer> indexManager, IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory, DatabaseStorageManager databaseStorageManager) {
        this.engineConfig = engineConfig;
        this.indexManager = indexManager;
        this.valueIndexBinaryObjectFactory = valueIndexBinaryObjectFactory;
        this.databaseStorageManager = databaseStorageManager;
    }

    private Optional<BinaryListIterator<V>> getBinaryListIterator(K identifier) throws InternalOperationException {
        Optional<Pointer> pointerOptional = this.indexManager.getIndex(identifier);
        if (pointerOptional.isEmpty()) {
            return Optional.empty();
        }

        Pointer pointer = pointerOptional.get();
        Optional<DBObject> dbObjectOptional = databaseStorageManager.select(pointer);
        return dbObjectOptional.map(dbObject -> new BinaryListIterator<>(engineConfig, valueIndexBinaryObjectFactory, dbObject.getData()));
    }

    @Override
    public boolean addIndex(K identifier, V value) throws IndexExistsException, InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject {
        return false;
    }

    @Override
    public Optional<ListIterator<V>> getIndex(K identifier) throws InternalOperationException {
        Optional<BinaryListIterator<V>> binaryListIteratorOptional = this.getBinaryListIterator(identifier);
        if (binaryListIteratorOptional.isPresent()) {
            return Optional.of(binaryListIteratorOptional.get());
        }
        return Optional.empty();
    }

    @Override
    public synchronized boolean removeIndex(K identifier, V value) throws InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject {
        Optional<BinaryListIterator<V>> binaryListIteratorOptional = this.getBinaryListIterator(identifier);
        return binaryListIteratorOptional.map(vBinaryListIterator -> vBinaryListIterator.remove(value)).orElse(false);
    }

    @Override
    public int size() throws InternalOperationException {
        return this.indexManager.size();
    }

    @Override
    public LockableIterator<KeyValue<K, ListIterator<V>>> getSortedIterator() throws InternalOperationException {
        return null;
    }

    @Override
    public void purgeIndex() {
        this.indexManager.purgeIndex();
    }

    @Override
    public int getIndexId() {
        return this.indexManager.getIndexId();
    }
}
