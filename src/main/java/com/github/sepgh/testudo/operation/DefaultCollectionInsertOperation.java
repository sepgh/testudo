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
import com.github.sepgh.testudo.utils.ReaderWriterLock;

import java.io.IOException;
import java.util.concurrent.ExecutionException;


// Todo: verification (read README.md)
public class DefaultCollectionInsertOperation<T extends Number & Comparable<T>> implements CollectionInsertOperation<T> {
    private final Scheme scheme;
    private final Scheme.Collection collection;
    private final ReaderWriterLock readerWriterLock;
    private final CollectionIndexProvider collectionIndexProvider;
    private final DatabaseStorageManager storageManager;
    private final UniqueTreeIndexManager<T, Pointer> clusterIndexManager;

    @SuppressWarnings("unchecked")
    public DefaultCollectionInsertOperation(Scheme scheme, Scheme.Collection collection, ReaderWriterLock readerWriterLock, CollectionIndexProviderFactory collectionIndexProviderFactory, DatabaseStorageManager storageManager) {
        this.scheme = scheme;
        this.collection = collection;
        this.readerWriterLock = readerWriterLock;
        this.collectionIndexProvider = collectionIndexProviderFactory.create(collection);
        this.storageManager = storageManager;
        this.clusterIndexManager = (UniqueTreeIndexManager<T, Pointer>) collectionIndexProvider.getClusterIndexManager();
    }

    @Override
    public <V> void insert(V v) throws SerializationException {
        ModelSerializer modelSerializer = new ModelSerializer(v);
        this.insert(modelSerializer.serialize());
    }

    protected Pointer storeBytes(byte[] bytes) throws IOException, InterruptedException, ExecutionException {
        return storageManager.store(this.collection.getId(), scheme.getVersion(), bytes);
    }

    // Todo: good idea to have this method as public interface? read README.md
    @Override
    public void insert(byte[] bytes) {
        Pointer pointer;

        try {
            pointer = this.storeBytes(bytes);
        } catch (IOException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);  // Todo
        }

        try {
            readerWriterLock.getWriteLock().lock();
            final T key = this.storeClusterIndex(pointer);
            this.storeFieldIndexes(bytes, key, pointer);
        } finally {
            readerWriterLock.getWriteLock().unlock();
        }

    }

    @SuppressWarnings("unchecked")
    private void storeFieldIndexes(byte[] bytes, T key, Pointer pointer) {
        for (Scheme.Field field : collection.getFields().stream().filter(field -> field.isIndexed()).toList()) {
            try {
                // Todo: add support for auto increment
                if (field.getIndex().isUnique()) {
                    UniqueQueryableIndex<?, T> uniqueIndexManager = (UniqueQueryableIndex<?, T>) collectionIndexProvider.getUniqueIndexManager(field);

                    uniqueIndexManager.addIndex(
                            CollectionSerializationUtil.getValueOfFieldAsObject(
                                    collection,
                                    field,
                                    bytes
                            ),
                            key
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

    private T storeClusterIndex(Pointer pointer) {
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
        return key;
    }
}
