package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.ds.Bitmap;
import com.github.sepgh.testudo.ds.KeyValue;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.IndexMissingException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.exception.VerificationException;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.utils.IteratorUtils;
import com.github.sepgh.testudo.utils.LazyFlattenIterator;
import com.github.sepgh.testudo.utils.LockableIterator;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class DuplicateBitmapIndexManager<K extends Comparable<K>, V extends Number & Comparable<V>> implements DuplicateQueryableIndex<K, V> {
    private final int collectionId;
    private final UniqueQueryableIndex<K, Pointer> indexManager;
    private final IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory;
    private final DatabaseStorageManager databaseStorageManager;
    private static final int SCHEME_ID = -1;

    public DuplicateBitmapIndexManager(int collectionId, UniqueQueryableIndex<K, Pointer> indexManager, IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory, DatabaseStorageManager databaseStorageManager) {
        this.collectionId = collectionId;
        this.indexManager = indexManager;
        this.valueIndexBinaryObjectFactory = valueIndexBinaryObjectFactory;
        this.databaseStorageManager = databaseStorageManager;
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

            DBObject dbObject = dbObjectOptional.get();
            int initialSize = dbObject.getDataSize();

            Bitmap<V> vBitmap = new Bitmap<>(valueIndexBinaryObjectFactory.getType(), dbObject.getData());
            boolean result = vBitmap.on(value);

            if (result) {
                if (initialSize >= vBitmap.getData().length) {
                    databaseStorageManager.update(pointer, dbObject1 -> {
                        try {
                            dbObject1.modifyData(vBitmap.getData());
                        } catch (VerificationException.InvalidDBObjectWrapper e) {
                            throw new RuntimeException(e);
                        }
                    });
                } else {
                    Pointer pointer1 = databaseStorageManager.store(SCHEME_ID, collectionId, 1, vBitmap.getData());
                    try {
                        this.indexManager.updateIndex(identifier, pointer1);
                        databaseStorageManager.remove(pointer);
                    } catch (IndexMissingException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            return result;
        } else {
            Bitmap<V> vBitmap = new Bitmap<>(valueIndexBinaryObjectFactory.getType(), new byte[1]);
            vBitmap.on(value);
            Pointer pointer = databaseStorageManager.store(SCHEME_ID, collectionId, 1, vBitmap.getData());
            try {
                this.indexManager.addIndex(identifier, pointer);
                return true;
            } catch (IndexExistsException e) {
                // Another thread stored index? shouldn't happen, but retry
                return this.addIndex(identifier, value);
            }
        }

    }

    @Override
    public Optional<ListIterator<V>> getIndex(K identifier) throws InternalOperationException {
        return this.getIndex(identifier, Order.DEFAULT);
    }

    @Override
    public Optional<ListIterator<V>> getIndex(K identifier, Order order) throws InternalOperationException {
        Optional<Pointer> pointerOptional = this.indexManager.getIndex(identifier);
        if (pointerOptional.isPresent()) {
            Pointer pointer = pointerOptional.get();
            Optional<DBObject> dbObjectOptional = databaseStorageManager.select(pointer);
            if (dbObjectOptional.isPresent()) {
                DBObject dbObject = dbObjectOptional.get();
                Bitmap<V> vBitmap = new Bitmap<>(valueIndexBinaryObjectFactory.getType(), dbObject.getData());
                return Optional.of(vBitmap.getOnIterator(order));
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean removeIndex(K identifier, V value) throws InternalOperationException, IOException, ExecutionException, InterruptedException {
        Optional<Pointer> pointerOptional = this.indexManager.getIndex(identifier);
        if (pointerOptional.isPresent()) {
            Pointer pointer = pointerOptional.get();
            Optional<DBObject> dbObjectOptional = databaseStorageManager.select(pointer);
            if (dbObjectOptional.isPresent()) {
                DBObject dbObject = dbObjectOptional.get();
                Bitmap<V> vBitmap = new Bitmap<>(valueIndexBinaryObjectFactory.getType(), dbObject.getData());
                boolean result = vBitmap.off(value);

                if (result) {
                    databaseStorageManager.update(pointer, dbObject1 -> {
                        try {
                            dbObject1.modifyData(vBitmap.getData());
                        } catch (VerificationException.InvalidDBObjectWrapper e) {
                            throw new RuntimeException(e);
                        }
                    });
                }

                return result;
            }
        }

        return false;
    }

    @Override
    public int size() throws InternalOperationException {
        return this.indexManager.size();
    }

    @Override
    public LockableIterator<KeyValue<K, ListIterator<V>>> getSortedIterator(Order order) throws InternalOperationException {
        LockableIterator<KeyValue<K, Pointer>> lockableIterator = this.indexManager.getSortedIterator(order);
        return new LockableIterator<>() {

            @Override
            public boolean hasNext() {
                return lockableIterator.hasNext();
            }

            @Override
            public KeyValue<K, ListIterator<V>> next() {
                KeyValue<K, Pointer> next = lockableIterator.next();
                try {
                    return new KeyValue<>(next.key(), getIndex(next.key(), order).orElseGet(() -> new ListIterator<V>() {
                        @Override
                        public boolean hasNext() {
                            return false;
                        }

                        @Override
                        public V next() {
                            return null;
                        }

                        @Override
                        public boolean hasPrevious() {
                            return false;
                        }

                        @Override
                        public V previous() {
                            return null;
                        }

                        @Override
                        public int nextIndex() {
                            return 0;
                        }

                        @Override
                        public int previousIndex() {
                            return 0;
                        }

                        @Override
                        public void remove() {

                        }

                        @Override
                        public void set(V v) {

                        }

                        @Override
                        public void add(V v) {

                        }
                    }));
                } catch (InternalOperationException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void lock() {
                lockableIterator.lock();
            }

            @Override
            public void unlock() {
                lockableIterator.unlock();
            }
        };
    }

    @SneakyThrows
    @Override
    public void purgeIndex() {
        LockableIterator<KeyValue<K, Pointer>> lockableIterator = this.indexManager.getSortedIterator(Order.DEFAULT);
        while (lockableIterator.hasNext()){
            KeyValue<K, Pointer> next = lockableIterator.next();
            databaseStorageManager.remove(next.value());
        }

        this.indexManager.purgeIndex();
    }

    @Override
    public int getIndexId() {
        return this.indexManager.getIndexId();
    }

    private Function<Pointer, Iterator<V>> getBitmapIteratorFunction(Order order) {
        return pointer -> {
            Optional<DBObject> dbObjectOptional = databaseStorageManager.select(pointer);
            if (dbObjectOptional.isPresent()) {
                DBObject dbObject = dbObjectOptional.get();
                return new Bitmap<>(valueIndexBinaryObjectFactory.getType(), dbObject.getData()).getOnIterator(order);
            }
            return null;
        };
    }

    private Function<KeyValue<K, Pointer>, Iterator<KeyValue<K, V>>> getKPBitmapIteratorFunction(Order order) {
        return kPointer -> {
            Optional<DBObject> dbObjectOptional = databaseStorageManager.select(kPointer.value());
            if (dbObjectOptional.isPresent()) {
                DBObject dbObject = dbObjectOptional.get();
                return IteratorUtils.modifyNext(
                        new Bitmap<>(valueIndexBinaryObjectFactory.getType(), dbObject.getData()).getOnIterator(order),
                        v -> new KeyValue<>(kPointer.key(), v)
                );
            }
            return null;
        };
    }

    @Override
    public Iterator<KeyValue<K, V>> getSortedKeyValueIterator(Order order) throws InternalOperationException {
        return new LazyFlattenIterator<>(
                this.indexManager.getSortedKeyValueIterator(order),
                getKPBitmapIteratorFunction(order)
        );
    }

    @Override
    public Iterator<V> getGreaterThan(K k, Order order) throws InternalOperationException {
        return new LazyFlattenIterator<>(
                this.indexManager.getGreaterThan(k, order),
                getBitmapIteratorFunction(order)
        );
    }

    @Override
    public Iterator<V> getGreaterThanEqual(K k, Order order) throws InternalOperationException {
        return new LazyFlattenIterator<>(
                this.indexManager.getGreaterThanEqual(k, order),
                getBitmapIteratorFunction(order)
        );
    }

    @Override
    public Iterator<V> getLessThan(K k, Order order) throws InternalOperationException {
        return new LazyFlattenIterator<>(
                this.indexManager.getLessThan(k, order),
                getBitmapIteratorFunction(order)
        );
    }

    @Override
    public Iterator<V> getLessThanEqual(K k, Order order) throws InternalOperationException {
        return new LazyFlattenIterator<>(
                this.indexManager.getLessThanEqual(k, order),
                getBitmapIteratorFunction(order)
        );
    }

    @Override
    public Iterator<V> getEqual(K k, Order order) throws InternalOperationException {
        Optional<ListIterator<V>> optional = this.getIndex(k, order);
        if (optional.isPresent()) {
            return optional.get();
        }
        return IteratorUtils.getCleanIterator();
    }

    @Override
    public Iterator<V> getNulls(Order order) {
        return this.getNullIndexes(order);
    }

    @Override
    public UniqueTreeIndexManager<K, Pointer> getInnerIndexManager() {
        return this.indexManager;
    }
}
