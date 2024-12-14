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
import lombok.Getter;

import java.io.IOException;
import java.util.concurrent.ExecutionException;


// Todo: verification (read README.md)
@Getter
public class DefaultCollectionInsertOperation<T extends Number & Comparable<T>> implements CollectionInsertOperation<T> {
    private final Scheme scheme;
    private final Scheme.Collection collection;
    private final ReaderWriterLock readerWriterLock;
    private final CollectionIndexProvider collectionIndexProvider;
    private final DatabaseStorageManager storageManager;
    private final UniqueTreeIndexManager<T, Pointer> clusterIndexManager;

    @SuppressWarnings("unchecked")
    public DefaultCollectionInsertOperation(Scheme scheme, Scheme.Collection collection, ReaderWriterLock readerWriterLock, CollectionIndexProvider collectionIndexProvider, DatabaseStorageManager storageManager) {
        this.scheme = scheme;
        this.collection = collection;
        this.readerWriterLock = readerWriterLock;
        this.collectionIndexProvider = collectionIndexProvider;
        this.storageManager = storageManager;
        this.clusterIndexManager = (UniqueTreeIndexManager<T, Pointer>) collectionIndexProvider.getClusterIndexManager();
    }

    @Override
    public <V> void execute(V v) throws SerializationException {
        ModelSerializer modelSerializer = new ModelSerializer(v);
        this.execute(modelSerializer.serialize());
    }

    protected Pointer storeBytes(byte[] bytes) throws IOException, InterruptedException, ExecutionException {
        return storageManager.store(this.scheme.getId(), this.collection.getId(), scheme.getVersion(), bytes);
    }

    // Todo: good idea to have this method as public interface? read README.md
    @Override
    public void execute(byte[] bytes) {
        try {
            readerWriterLock.getWriteLock().lock();
            // Todo: we better check for unique field values first, before any insertions  read README.md
            Pointer pointer = this.storeBytes(bytes);
            final T key = this.storeClusterIndex(pointer);
            this.storeFieldIndexes(bytes, key, pointer);
        } catch (IOException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);  // Todo
        } finally {
            readerWriterLock.getWriteLock().unlock();
        }

    }

    @SuppressWarnings("unchecked")
    private void storeFieldIndexes(byte[] bytes, T clusterId, Pointer pointer) {
        for (Scheme.Field field : collection.getFields().stream().filter(Scheme.Field::isIndexed).toList()) {
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
                            clusterId
                    );

                } else {
                    DuplicateQueryableIndex<?, T> duplicateIndexManager = (DuplicateQueryableIndex<?, T>) collectionIndexProvider.getDuplicateIndexManager(field);
                    duplicateIndexManager.addIndex(
                            CollectionSerializationUtil.getValueOfFieldAsObject(
                                    collection,
                                    field,
                                    bytes
                            ),
                            clusterId
                    );
                }
            } catch (IndexExistsException | InternalOperationException | DeserializationException e) {
                try {
                    clusterIndexManager.removeIndex(clusterId);
                } catch (InternalOperationException ex) {
                    // Todo: log e and ex
                    throw new RuntimeException("Failed to store index, but also failed to rollback cluster id: " + e.getMessage(), ex);
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
