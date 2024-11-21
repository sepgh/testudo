package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.index.DuplicateQueryableIndex;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.UniqueQueryableIndex;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.serialization.CollectionSerializationUtil;
import com.github.sepgh.testudo.serialization.ModelSerializer;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class DefaultCollectionInsertOperation implements CollectionInsertOperation {
    private final Scheme scheme;
    private final Scheme.Collection collection;
    private final CollectionIndexProvider collectionIndexProvider;
    private final DatabaseStorageManager storageManager;

    public DefaultCollectionInsertOperation(Scheme scheme, Scheme.Collection collection, CollectionIndexProviderFactory collectionIndexProviderFactory, DatabaseStorageManager storageManager) {
        this.scheme = scheme;
        this.collection = collection;
        this.collectionIndexProvider = collectionIndexProviderFactory.create(collection);
        this.storageManager = storageManager;
    }

    @Override
    public <V> void insert(V v) throws SerializationException {
        ModelSerializer modelSerializer = new ModelSerializer(v);
        this.insert(modelSerializer.serialize());
    }

    @Override
    public <T extends Number & Comparable<T>> void insert(byte[] bytes) {
        Pointer pointer;

        try {
            pointer = storageManager.store(this.collection.getId(), scheme.getVersion(), bytes);
        } catch (IOException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);  // Todo
        }

        UniqueTreeIndexManager<T, Pointer> clusterIndexManager = (UniqueTreeIndexManager<T, Pointer>) collectionIndexProvider.getClusterIndexManager();
        T key = null;

        boolean stored = false;
        while (!stored) {
            try {
                key = clusterIndexManager.nextKey();
                clusterIndexManager.addIndex(key, pointer);
                stored = true;
            } catch (IndexExistsException ignored){}
            catch (InternalOperationException e) {
                throw new RuntimeException(e); // Todo
            }
        }

        T finalKey = key;
        for (Scheme.Field field : collection.getFields().stream().filter(field -> field.isIndex() || field.isPrimary()).toList()) {
            try {
                // Todo: add support for auto increment
                if (field.isIndexUnique()) {
                    UniqueQueryableIndex<?, T> uniqueIndexManager = (UniqueQueryableIndex<?, T>) collectionIndexProvider.getUniqueIndexManager(field);

                        uniqueIndexManager.addIndex(
                                CollectionSerializationUtil.getValueOfFieldAsObject(
                                        collection,
                                        field,
                                        bytes
                                ),
                                finalKey
                        );

                } else {
                    DuplicateQueryableIndex<?, T> duplicateIndexManager = (DuplicateQueryableIndex<?, T>) collectionIndexProvider.getDuplicateIndexManager(field);
                    duplicateIndexManager.addIndex(
                            CollectionSerializationUtil.getValueOfFieldAsObject(
                                    collection,
                                    field,
                                    bytes
                            ),
                            key
                    );
                }
            } catch (IndexExistsException | InternalOperationException | DeserializationException e) {
                try {
                    clusterIndexManager.removeIndex(key);
                } catch (InternalOperationException ex) {
                    // Todo: log e and ex
                    throw new RuntimeException("Failed to store index, but also failed to rollback cluster key: " + e.getMessage(), ex);
                }

                try {
                    storageManager.remove(pointer);
                } catch (IOException | ExecutionException | InterruptedException ep) {
                    // Todo: log e and ep
                }

                throw new RuntimeException(e); // Todo
            } catch (IOException | ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);   // Todo: hopefully these would be handled like above?
            }
        }
    }
}
