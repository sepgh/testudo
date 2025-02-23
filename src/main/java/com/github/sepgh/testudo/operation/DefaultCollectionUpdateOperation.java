package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.ds.Bitmap;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.*;
import com.github.sepgh.testudo.functional.CheckedFunction;
import com.github.sepgh.testudo.index.DuplicateQueryableIndex;
import com.github.sepgh.testudo.index.UniqueQueryableIndex;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.operation.query.Query;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.serialization.CollectionSerializationUtil;
import com.github.sepgh.testudo.serialization.ModelDeserializer;
import com.github.sepgh.testudo.serialization.ModelSerializer;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.utils.CachedFieldValueReader;
import com.github.sepgh.testudo.utils.ReaderWriterLock;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Slf4j
public class DefaultCollectionUpdateOperation<T extends Number & Comparable<T>> implements CollectionUpdateOperation<T> {
    private final Scheme.Collection collection;
    private final ReaderWriterLock readerWriterLock;
    private final CollectionIndexProvider collectionIndexProvider;
    private final DatabaseStorageManager storageManager;
    private final UniqueTreeIndexManager<T, Pointer> clusterIndexManager;
    private Query query;

    @SuppressWarnings("unchecked")
    public DefaultCollectionUpdateOperation(Scheme.Collection collection, ReaderWriterLock readerWriterLock, CollectionIndexProvider collectionIndexProvider, DatabaseStorageManager storageManager) {
        this.collection = collection;
        this.readerWriterLock = readerWriterLock;
        this.collectionIndexProvider = collectionIndexProvider;
        this.storageManager = storageManager;
        this.clusterIndexManager = (UniqueTreeIndexManager<T, Pointer>) this.collectionIndexProvider.getClusterIndexManager();
    }

    protected Iterator<T> getExecutedQuery() {
        if (query == null) {
            this.query = new Query();
        }
        return query.execute(this.collectionIndexProvider);
    }

    @Override
    public CollectionUpdateOperation<T> query(Query query) {
        this.query = query;
        return this;
    }

    @Override
    public Query getQuery() {
        return this.query;
    }

    @SneakyThrows
    private byte[] hash(byte[] bytes) {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        return md.digest(bytes);
    }

    protected long handleExecution(CheckedFunction<DBObject, byte[], BaseSerializationException> dbObjectConsumer) throws InternalOperationException, BaseSerializationException {
        AtomicLong atomicLong = new AtomicLong();

        try {
            this.readerWriterLock.getWriteLock().lock();
            Iterator<T> executedQuery = getExecutedQuery();

            while (executedQuery.hasNext()) {
                T clusterId = executedQuery.next();
                Optional<Pointer> optionalPointer = clusterIndexManager.getIndex(clusterId);

                if (optionalPointer.isEmpty()) {
                    log.error("cluster id {} has no pointer!", clusterId);
                    continue;
                }

                Pointer pointer = optionalPointer.get();

                Optional<DBObject> dbObjectOptional = this.storageManager.select(pointer);
                if (dbObjectOptional.isEmpty()) {
                    log.error("Object not found for cluster id {} and pointer {}", clusterId, pointer);
                    continue;
                }

                DBObject dbObject = dbObjectOptional.get();
                byte[] preHash = hash(dbObject.getData());
                byte[] updatedData = dbObjectConsumer.apply(dbObject);
                byte[] postHash = hash(updatedData);

                if (Arrays.equals(preHash, postHash))
                    continue;

                this.update(pointer, updatedData, dbObject.getData(), clusterId);
                atomicLong.incrementAndGet();

            }

        } finally {
            this.readerWriterLock.getWriteLock().unlock();
        }

        return atomicLong.get();
    }

    @SuppressWarnings("unchecked")
    private <F extends Comparable<F>> void update(Pointer pointer, byte[] update, byte[] old, T clusterId) throws DeserializationException, InternalOperationException {
        CachedFieldValueReader updatedCachedFieldsReader = new CachedFieldValueReader(collection, update);
        CachedFieldValueReader oldCachedFieldsReader = new CachedFieldValueReader(collection, old);

        Bitmap<Integer> newNulls = CollectionSerializationUtil.getNullsBitmap(collection, update);
        Bitmap<Integer> oldNulls = CollectionSerializationUtil.getNullsBitmap(collection, old);

        this.verify(oldCachedFieldsReader, updatedCachedFieldsReader, oldNulls, newNulls);

        this.storageManager.update(pointer, update);

        int fieldIndex = -1;

        for (Scheme.Field field : collection.getSortedFields()) {
            fieldIndex++;

            if (!field.isIndexed())
                continue;

            Object oldObject = CollectionSerializationUtil.getValueOfFieldAsObject(
                    collection,
                    field,
                    old
            );
            Object newObject = CollectionSerializationUtil.getValueOfFieldAsObject(
                    collection,
                    field,
                    update
            );

            if ((oldObject).equals(newObject)) {
                continue;
            }

            if (field.getIndex().isPrimary() && field.getIndex().isAutoIncrement()) {
                continue; // Auto Increment field doesnt need to get updated
            }

            if (field.getIndex().isPrimary() && newNulls.isOn(fieldIndex)){
                throw new VerificationException("The %s field is a primary index and cant be null".formatted(field.getName()));
            }

            if (field.getIndex().isUnique()) {
                UniqueQueryableIndex<F, T> uniqueIndexManager = (UniqueQueryableIndex<F, T>) collectionIndexProvider.getUniqueIndexManager(field);
                if (oldNulls.isOn(fieldIndex) && !newNulls.isOn(fieldIndex)) {
                    uniqueIndexManager.removeNull(clusterId);
                } else {
                    uniqueIndexManager.removeIndex((F) oldObject);
                }
                if (newNulls.isOn(fieldIndex)) {
                    uniqueIndexManager.addNull(clusterId);
                } else {
                    uniqueIndexManager.addIndex((F) newObject, clusterId);
                }
            } else {
                DuplicateQueryableIndex<F, T> duplicateIndexManager = (DuplicateQueryableIndex<F, T>) collectionIndexProvider.getDuplicateIndexManager(field);
                if (oldNulls.isOn(fieldIndex) && !newNulls.isOn(fieldIndex)) {
                    duplicateIndexManager.removeNull(clusterId);
                } else {
                    duplicateIndexManager.removeIndex((F) oldObject, clusterId);
                }
                if (newNulls.isOn(fieldIndex)) {
                    duplicateIndexManager.addNull(clusterId);
                } else {
                    duplicateIndexManager.addIndex((F) newObject, clusterId);
                }
            }
        }

    }

    @SuppressWarnings("unchecked")
    private <K extends Comparable<K>> void verify(CachedFieldValueReader oldCachedFieldsReader, CachedFieldValueReader updatedCachedFieldsReader, Bitmap<Integer> oldNulls, Bitmap<Integer> newNulls) throws DeserializationException, InternalOperationException {
        int index = -1;
        for (Scheme.Field field : collection.getSortedFields()) {
            index++;

            if (newNulls.isOn(index))
                continue;

            if (!field.isIndexed())
                continue;

            if (!field.getIndex().isUnique())
                continue;

            Object value = updatedCachedFieldsReader.get(field);
            if (oldCachedFieldsReader.get(field).equals(value))
                continue;

            UniqueQueryableIndex<K, T> uniqueIndexManager = (UniqueQueryableIndex<K, T>) collectionIndexProvider.getUniqueIndexManager(field);
            Optional<T> optional = uniqueIndexManager.getIndex((K) value);

            if (optional.isPresent()) {
                throw new IndexExistsException("An index for field %s with value %s already exists".formatted(field.getName(), value));
            }

        }
    }

    @Override
    public <M> long execute(Class<M> mClass, Consumer<M> mConsumer) throws InternalOperationException, BaseSerializationException {
        ModelDeserializer<M> modelDeserializer = new ModelDeserializer<>(mClass);
        ModelSerializer modelSerializer = new ModelSerializer();

        return this.handleExecution(dbObject -> {
            M model = modelDeserializer.deserialize(dbObject.getData());
            mConsumer.accept(model);
            return modelSerializer.reset(model).serialize();
        });
    }

    @Override
    public long execute(Consumer<byte[]> byteArrayConsumer) throws InternalOperationException, BaseSerializationException {
        return this.handleExecution(dbObject -> {
            byte[] data = dbObject.getData();
            byteArrayConsumer.accept(data);
            return data;
        });
    }

}
