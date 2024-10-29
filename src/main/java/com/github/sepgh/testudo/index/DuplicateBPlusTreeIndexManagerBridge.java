package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.IndexMissingException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.exception.VerificationException;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.utils.LazyFlattenIterator;
import com.github.sepgh.testudo.utils.LockableIterator;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;


public class DuplicateBPlusTreeIndexManagerBridge<K extends Comparable<K>, V extends Number & Comparable<V>> implements DuplicateQueryableIndex<K, V> {
    private final int collectionId;
    private final UniqueQueryableIndex<K, Pointer> indexManager;
    private final IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory;
    private final DatabaseStorageManager databaseStorageManager;
    private final EngineConfig engineConfig;

    public DuplicateBPlusTreeIndexManagerBridge(int collectionId, EngineConfig engineConfig, UniqueQueryableIndex<K, Pointer> indexManager, IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory, DatabaseStorageManager databaseStorageManager) {
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
    public boolean addIndex(K identifier, V value) throws InternalOperationException, IOException, ExecutionException, InterruptedException {
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

    @Override
    public synchronized boolean removeIndex(K identifier, V value) throws InternalOperationException, IOException, ExecutionException, InterruptedException {
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

        // the list is empty, remove the object from index and DB storage
        this.indexManager.removeIndex(identifier);
        binaryListIterator.resetCursor();
        if (!binaryListIterator.hasNext())
            databaseStorageManager.remove(pointer);

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

    @Override
    public void purgeIndex() {
        try {
            LockableIterator<KeyValue<K, Pointer>> lockableIterator = this.indexManager.getSortedIterator();

            try {
                lockableIterator.lock();
                while (lockableIterator.hasNext()) {
                    Pointer pointer = lockableIterator.next().value();
                    databaseStorageManager.remove(pointer);
                }
            } catch (IOException | ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                lockableIterator.unlock();
            }


            this.indexManager.purgeIndex();
        } catch (InternalOperationException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public int getIndexId() {
        return this.indexManager.getIndexId();
    }

    private Function<Pointer, Iterator<V>> getListIteratorFunction() {
        return pointer -> {
            Optional<DBObject> dbObjectOptional = databaseStorageManager.select(pointer);
            if (dbObjectOptional.isPresent()) {
                DBObject dbObject = dbObjectOptional.get();
                return new BinaryListIterator<>(engineConfig, valueIndexBinaryObjectFactory, dbObject.getData());
            }
            return null;
        };
    }

    @Override
    public Iterator<V> getGreaterThan(K k) throws InternalOperationException {
        return new LazyFlattenIterator<>(
                this.indexManager.getGreaterThan(k),
                getListIteratorFunction()
        );
    }

    @Override
    public Iterator<V> getGreaterThanEqual(K k) throws InternalOperationException {
        return new LazyFlattenIterator<>(
                this.indexManager.getGreaterThanEqual(k),
                getListIteratorFunction()
        );
    }

    @Override
    public Iterator<V> getLessThan(K k) throws InternalOperationException {
        return new LazyFlattenIterator<>(
                this.indexManager.getLessThan(k),
                getListIteratorFunction()
        );
    }

    @Override
    public Iterator<V> getLessThanEqual(K k) throws InternalOperationException {
        return new LazyFlattenIterator<>(
                this.indexManager.getLessThanEqual(k),
                getListIteratorFunction()
        );
    }

    @Override
    public Optional<Iterator<V>> getEqual(K k) throws InternalOperationException {
        Optional<ListIterator<V>> optional = this.getIndex(k);
        if (optional.isPresent()) {
            return Optional.of(optional.get());
        }
        return Optional.empty();
    }
}
