package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.exception.*;
import com.github.sepgh.testudo.index.DuplicateQueryableIndex;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.UniqueQueryableIndex;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.operation.query.Query;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.serialization.CollectionSerializationUtil;
import com.github.sepgh.testudo.serialization.ModelDeserializer;
import com.github.sepgh.testudo.serialization.ModelSerializer;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.utils.ReaderWriterLock;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

public class DefaultCollectionUpdateOperation<T extends Number & Comparable<T>> implements CollectionUpdateOperation<T> {
    private final Scheme.Collection collection;
    private final ReaderWriterLock readerWriterLock;
    private final CollectionIndexProvider collectionIndexProvider;
    private final DatabaseStorageManager storageManager;
    private final UniqueTreeIndexManager<T, Pointer> clusterIndexManager;
    @Getter
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

    protected long handleExecution(Function<DBObject, byte[]> dbObjectConsumer) {
        AtomicLong atomicLong = new AtomicLong();

        try {
            this.readerWriterLock.getWriteLock().lock();
            Iterator<T> executedQuery = getExecutedQuery();

            while (executedQuery.hasNext()) {
                T clusterId = executedQuery.next();
                Optional<Pointer> optionalPointer = clusterIndexManager.getIndex(clusterId);

                if (optionalPointer.isEmpty()) {
                    // Todo: can just use `continue` as well
                    throw new RuntimeException("Pointer not found: " + clusterId);
                }

                Pointer pointer = optionalPointer.get();

                Optional<DBObject> dbObjectOptional = this.storageManager.select(pointer);
                if (dbObjectOptional.isEmpty()) {
                    // Todo
                    throw new RuntimeException("Object not found: " + pointer);
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

        } catch (InternalOperationException | VerificationException.InvalidDBObjectWrapper | IOException |
                 ExecutionException | InterruptedException | IndexExistsException | DeserializationException e) {
            // Todo
            throw new RuntimeException(e);
        } finally {
            this.readerWriterLock.getWriteLock().unlock();
        }

        return atomicLong.get();
    }

    @SuppressWarnings("unchecked")
    private <F extends Comparable<F>> void update(Pointer pointer, byte[] update, byte[] old, T clusterId) throws IOException, ExecutionException, InterruptedException, VerificationException.InvalidDBObjectWrapper, DeserializationException, InternalOperationException, IndexExistsException {
        // Todo: if a field has been updated, we better first check if its unique, and it already existed or not.  read README.md

        this.storageManager.update(pointer, update);

        for (Scheme.Field field : collection.getFields()) {
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

            // Todo: primary fields cant get updated. Verify this at a valid point
            if (field.getIndex().isUnique()) {
                UniqueQueryableIndex<F, T> uniqueIndexManager = (UniqueQueryableIndex<F, T>) collectionIndexProvider.getUniqueIndexManager(field);
                uniqueIndexManager.removeIndex((F) oldObject);
                uniqueIndexManager.addIndex((F) newObject, clusterId);
            } else {
                DuplicateQueryableIndex<F, T> duplicateIndexManager = (DuplicateQueryableIndex<F, T>) collectionIndexProvider.getDuplicateIndexManager(field);
                duplicateIndexManager.removeIndex((F) oldObject, clusterId);
                duplicateIndexManager.addIndex((F) newObject, clusterId);
            }
        }

    }

    @Override
    public <M> long execute(Consumer<M> mConsumer, Class<M> mClass) {
        ModelDeserializer<M> modelDeserializer = new ModelDeserializer<>(mClass);
        ModelSerializer modelSerializer = new ModelSerializer();

        return this.handleExecution(dbObject -> {
            M model;
            try {
                model = modelDeserializer.deserialize(dbObject.getData());
            } catch (SerializationException | DeserializationException e) {
                // todo: + how to know which err happened really?  (https://stackoverflow.com/questions/18198176)
                throw new RuntimeException(e);
            }


            mConsumer.accept(model);
            try {
                return modelSerializer.reset(model).serialize();
            } catch (SerializationException e) {
                // todo: + how to know which err happened really?  (https://stackoverflow.com/questions/18198176)
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public long execute(Consumer<byte[]> byteArrayConsumer) {
        return this.handleExecution(dbObject -> {
            byte[] data = dbObject.getData();
            byteArrayConsumer.accept(data);
            return data;
        });
    }

}
