package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.operation.query.Query;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.serialization.ModelDeserializer;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.utils.IteratorUtils;
import com.github.sepgh.testudo.utils.LockableIterator;
import com.github.sepgh.testudo.utils.ReaderWriterLock;
import lombok.Getter;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class DefaultCollectionSelectOperation<T extends Number & Comparable<T>> implements CollectionSelectOperation<T> {

    private final Scheme.Collection collection;
    private final ReaderWriterLock readerWriterLock;
    private final CollectionIndexProvider collectionIndexProvider;
    private final DatabaseStorageManager storageManager;
    private final UniqueTreeIndexManager<T, Pointer> clusterIndexManager;
    @Getter
    private Query query;
    private List<String> fields;

    @SuppressWarnings("unchecked")
    public DefaultCollectionSelectOperation(Scheme.Collection collection, ReaderWriterLock readerWriterLock, CollectionIndexProvider collectionIndexProvider, DatabaseStorageManager storageManager) {
        this.collection = collection;
        this.readerWriterLock = readerWriterLock;
        this.collectionIndexProvider = collectionIndexProvider;
        this.storageManager = storageManager;
        this.clusterIndexManager = (UniqueTreeIndexManager<T, Pointer>) this.collectionIndexProvider.getClusterIndexManager();
    }

    // Todo: implement the actual field limitations
    @Override
    public CollectionSelectOperation<T> fields(String... fields) {
        this.fields = Arrays.asList(fields);
        return this;
    }

    @Override
    public CollectionSelectOperation<T> query(Query query) {
        this.query = query;
        return this;
    }

    protected <V extends Number & Comparable<V>> Iterator<V> getExecutedQuery() {
        if (query == null) {
            this.query = new Query();
        }
        return query.execute(this.collectionIndexProvider);
    }

    @Override
    public LockableIterator<DBObject> execute() {
        Iterator<T> executedQuery = getExecutedQuery();

        return LockableIterator.wrapReader(IteratorUtils.modifyNext(
                executedQuery,
                i -> {
                    try {
                        Optional<Pointer> optionalPointer = clusterIndexManager.getIndex(i);
                        if (optionalPointer.isPresent()) {
                            Optional<DBObject> dbObjectOptional = this.storageManager.select(optionalPointer.get());
                            if (dbObjectOptional.isPresent()) {
                                return dbObjectOptional.get();
                            }
                        }
                        throw new RuntimeException("Could not read object");  // Todo
                    } catch (InternalOperationException e) {
                        throw new RuntimeException(e); // Todo
                    }
                }
        ), readerWriterLock);

    }

    @Override
    public <V> LockableIterator<V> execute(Class<V> clazz) {
        ModelDeserializer<V> modelDeserializer = new ModelDeserializer<>(clazz);
        return LockableIterator.wrapReader(IteratorUtils.modifyNext(execute(), dbObject -> {
            try {
                return modelDeserializer.deserialize(dbObject.getData());
            } catch (SerializationException | DeserializationException e) {
                throw new RuntimeException(e); // Todo
            }
        }), readerWriterLock);
    }

    @Override
    public <V> List<V> asList(Class<V> clazz) {
        return this.execute(clazz).asList();
    }

    @Override
    public long count() {
        try {
            readerWriterLock.getReadLock().lock();
            long count = 0;
            Iterator<?> executedQuery = getExecutedQuery();
            while (executedQuery.hasNext()) {
                count++;
                executedQuery.next();
            }
            return count;
        } finally {
            readerWriterLock.getReadLock().unlock();
        }

    }

    @Override
    public boolean exists() {
        if (query != null) {
            query.limit(1);
        } else {
            query = new Query().limit(1);
        }
        return this.execute().hasNext();
    }
}
