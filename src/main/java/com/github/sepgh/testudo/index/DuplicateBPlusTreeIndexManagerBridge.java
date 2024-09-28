package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.IndexMissingException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.exception.VerificationException;
import com.github.sepgh.testudo.index.data.IndexBinaryObject;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.utils.LockableIterator;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;


public class DuplicateBPlusTreeIndexManagerBridge<K extends Comparable<K>, V extends Number & Comparable<V>> implements DuplicateIndexManager<K, V> {
    private final int collectionId;
    private final UniqueTreeIndexManager<K, Pointer> indexManager;
    private final IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory;
    private final DatabaseStorageManager databaseStorageManager;
    private final EngineConfig engineConfig;

    public DuplicateBPlusTreeIndexManagerBridge(int collectionId, EngineConfig engineConfig, UniqueTreeIndexManager<K, Pointer> indexManager, IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory, DatabaseStorageManager databaseStorageManager) {
        this.collectionId = collectionId;
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
    public boolean addIndex(K identifier, V value) throws InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject, IOException, ExecutionException, InterruptedException {
        Optional<Pointer> pointerOptional = this.indexManager.getIndex(identifier);
        if (pointerOptional.isPresent()) {
            Pointer pointer = pointerOptional.get();
            Optional<DBObject> dbObjectOptional = databaseStorageManager.select(pointer);
            if (dbObjectOptional.isEmpty()) {
                throw new RuntimeException();   // Todo: it was pointing to somewhere without data
            }
            BinaryListIterator<V> binaryListIterator = new BinaryListIterator<>(engineConfig, valueIndexBinaryObjectFactory, dbObjectOptional.get().getData());

            int prevSize = binaryListIterator.getData().length;
            binaryListIterator.addNew(value);
            int afterSize = binaryListIterator.getData().length;

            if (afterSize == prevSize) {
                databaseStorageManager.update(pointer, dbObject -> {
                    try {
                        dbObject.modifyData(binaryListIterator.getData());
                    } catch (VerificationException.InvalidDBObjectWrapper e) {
                        throw new RuntimeException(e);  // Todo
                    }
                });
            } else {
                Pointer pointerNew = databaseStorageManager.store(collectionId, -1, binaryListIterator.getData());
                try {
                    indexManager.updateIndex(identifier, pointerNew);
                } catch (IndexMissingException e) {
                    throw new RuntimeException(e);  // Todo
                }
                databaseStorageManager.remove(pointer);
            }

            return true;

        }

        // Creating new binary list iterator and add the object

        byte[] bytes = new byte[BinaryListIterator.META_SIZE + valueIndexBinaryObjectFactory.size() * 5];   // Todo: "5" here is just random! Need a better plan?
        BinaryListIterator<V> binaryListIterator = new BinaryListIterator<>(engineConfig, valueIndexBinaryObjectFactory, bytes);
        binaryListIterator.initialize();
        binaryListIterator.addNew(value);

        // Insert to DB

        Pointer pointer = databaseStorageManager.store(collectionId, -1, binaryListIterator.getData());
        try {
            this.indexManager.addIndex(identifier, pointer);
            return true;
        } catch (IndexExistsException e) {
            // Last time we ran this method there was no binary list existing! but before we can add the index, one was created
            // This normally should not happen, but if it does, it means there was a race condition which current thread lost the game
            // So let's retry
            try {
                return addIndex(identifier, value);
            } finally {
                databaseStorageManager.remove(pointer);
            }
        }

    }

    @Override
    public Optional<ListIterator<V>> getIndex(K identifier) throws InternalOperationException {
        Optional<BinaryListIterator<V>> binaryListIteratorOptional = this.getBinaryListIterator(identifier);
        if (binaryListIteratorOptional.isPresent()) {
            return Optional.of(binaryListIteratorOptional.get());
        }
        return Optional.empty();
    }

    // Todo: after removing an index, the list may be empty. We can remove the pointer from bridge index manager
    @Override
    public synchronized boolean removeIndex(K identifier, V value) throws InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject, IOException, ExecutionException, InterruptedException {
        Optional<Pointer> pointerOptional = this.indexManager.getIndex(identifier);
        if (pointerOptional.isEmpty()) {
            return false;
        }

        Pointer pointer = pointerOptional.get();
        Optional<DBObject> dbObjectOptional = databaseStorageManager.select(pointer);
        if (dbObjectOptional.isEmpty()) {
            return false;
        }
        BinaryListIterator<V> binaryListIterator = new BinaryListIterator<>(engineConfig, valueIndexBinaryObjectFactory, dbObjectOptional.get().getData());
        boolean result = binaryListIterator.remove(value);

        if (result) {
            databaseStorageManager.update(pointer, dbObject -> {
                try {
                    dbObject.modifyData(binaryListIterator.getData());
                } catch (VerificationException.InvalidDBObjectWrapper e) {
                    throw new RuntimeException(e);
                }
            });
        }

        return result;
    }

    @Override
    public int size() throws InternalOperationException {
        return this.indexManager.size();
    }

    @Override
    public LockableIterator<KeyValue<K, ListIterator<V>>> getSortedIterator() throws InternalOperationException {
        return new LockableIterator<KeyValue<K, ListIterator<V>>>() {

            private final Iterator<KeyValue<K, Pointer>> iterator = indexManager.getSortedIterator();

            @Override
            public void lock() {
            }

            @Override
            public void unlock() {

            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @SneakyThrows
            @Override
            public KeyValue<K, ListIterator<V>> next() {
                KeyValue<K, Pointer> next = iterator.next();
                Optional<BinaryListIterator<V>> binaryListIteratorOptional = getBinaryListIterator(next.key());
                if (binaryListIteratorOptional.isPresent()) {
                    return new KeyValue<>(next.key(), binaryListIteratorOptional.get());
                }
                throw new NoSuchElementException();
            }
        };
    }

    // Todo: purging the bridge index manager is not enough, the list is stored in DB storage
    @Override
    public void purgeIndex() {
        this.indexManager.purgeIndex();
    }

    @Override
    public int getIndexId() {
        return this.indexManager.getIndexId();
    }
}
