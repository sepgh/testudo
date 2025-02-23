package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.DuplicateQueryableIndex;
import com.github.sepgh.testudo.index.UniqueQueryableIndex;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.operation.query.Query;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.serialization.CollectionSerializationUtil;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.utils.ReaderWriterLock;
import lombok.Getter;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultCollectionDeleteOperation<T extends Number & Comparable<T>> implements CollectionDeleteOperation<T> {

    private final Scheme.Collection collection;
    private final ReaderWriterLock readerWriterLock;
    private final CollectionIndexProvider collectionIndexProvider;
    private final DatabaseStorageManager storageManager;
    private final UniqueTreeIndexManager<T, Pointer> clusterIndexManager;
    @Getter
    private Query query;

    @SuppressWarnings("unchecked")
    public DefaultCollectionDeleteOperation(Scheme.Collection collection, ReaderWriterLock readerWriterLock, CollectionIndexProvider collectionIndexProvider, DatabaseStorageManager storageManager) {
        this.collection = collection;
        this.readerWriterLock = readerWriterLock;
        this.collectionIndexProvider = collectionIndexProvider;
        this.storageManager = storageManager;
        this.clusterIndexManager = (UniqueTreeIndexManager<T, Pointer>) this.collectionIndexProvider.getClusterIndexManager();
    }

    @Override
    public CollectionDeleteOperation<T> query(Query query) {
        this.query = query;
        return this;
    }

    protected Iterator<T> getExecutedQuery() {
        if (query == null) {
            this.query = new Query();
        }
        return query.execute(this.collectionIndexProvider);
    }

    @Override
    public long execute() throws InternalOperationException, DeserializationException {
        AtomicLong counter = new AtomicLong();
        try {
            this.readerWriterLock.getWriteLock().lock();
            Iterator<T> executedQuery = getExecutedQuery();

            while (executedQuery.hasNext()) {
                T clusterId = executedQuery.next();
                Optional<Pointer> optionalPointer = clusterIndexManager.getIndex(clusterId);

                if (optionalPointer.isEmpty()) {
                    throw new InternalOperationException("No pointer found for clusterId: " + clusterId);
                }

                Pointer pointer = optionalPointer.get();
                Optional<DBObject> optionalDBObject = this.storageManager.select(pointer);

                if (optionalDBObject.isEmpty()) {
                    throw new InternalOperationException("No object present at: " + pointer);
                }


                this.removeFieldIndexes(clusterId, optionalDBObject.get().getData());

                this.storageManager.remove(pointer);
                this.clusterIndexManager.removeIndex(clusterId);

                counter.incrementAndGet();
            }
        } finally {
            this.readerWriterLock.getWriteLock().unlock();
        }
        return counter.get();
    }

    private void removeFieldIndexes(T clusterId, byte[] bytes) throws DeserializationException, InternalOperationException {
        for (Scheme.Field field : collection.getFields().stream().filter(Scheme.Field::isIndexed).toList()) {
            if (field.getIndex().isUnique()) {
                UniqueQueryableIndex<?, T> uniqueIndexManager = (UniqueQueryableIndex<?, T>) collectionIndexProvider.getUniqueIndexManager(field);
                uniqueIndexManager.removeIndex(
                        CollectionSerializationUtil.getValueOfFieldAsObject(
                                collection,
                                field,
                                bytes
                        )
                );

            } else {
                DuplicateQueryableIndex<?, T> duplicateIndexManager = (DuplicateQueryableIndex<?, T>) collectionIndexProvider.getDuplicateIndexManager(field);

                duplicateIndexManager.removeIndex(
                        CollectionSerializationUtil.getValueOfFieldAsObject(
                                collection,
                                field,
                                bytes
                        ),
                        clusterId
                );
            }
        }
    }

}
