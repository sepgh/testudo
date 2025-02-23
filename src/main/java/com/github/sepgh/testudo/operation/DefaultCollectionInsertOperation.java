package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.ds.Bitmap;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.*;
import com.github.sepgh.testudo.index.DuplicateQueryableIndex;
import com.github.sepgh.testudo.index.UniqueQueryableIndex;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.serialization.*;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.utils.CachedFieldValueReader;
import com.github.sepgh.testudo.utils.ReaderWriterLock;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;


@Getter
public class DefaultCollectionInsertOperation<T extends Number & Comparable<T>> implements CollectionInsertOperation<T> {
    private static final Logger logger = LoggerFactory.getLogger(DefaultCollectionInsertOperation.class);

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

    @SuppressWarnings("unchecked")
    @Override
    public <V> V execute(V v) throws SerializationException, InternalOperationException, DeserializationException {
        ModelSerializer modelSerializer = new ModelSerializer(v);
        byte[] serialized = modelSerializer.serialize();
        byte[] bytes = this.execute(serialized);
        return (V) new ModelDeserializer<>(v.getClass()).deserialize(bytes);
    }

    protected Pointer storeBytes(CachedFieldValueReader cachedFieldValueReader) throws InternalOperationException {
        return storageManager.store(this.scheme.getId(), this.collection.getId(), scheme.getVersion(), cachedFieldValueReader.getBytes());
    }

    @Override
    public byte[] execute(byte[] bytes) throws InternalOperationException, DeserializationException, SerializationException {
        try {
            readerWriterLock.getWriteLock().lock();
            CachedFieldValueReader cachedFieldValueReader = new CachedFieldValueReader(collection, bytes);
            this.verify(cachedFieldValueReader);
            this.handleAutoGeneratedPrimaryKey(cachedFieldValueReader);
            Pointer pointer = this.storeBytes(cachedFieldValueReader);
            final T key = this.storeClusterIndex(pointer);
            this.storeFieldIndexes(cachedFieldValueReader, key, pointer);
            return bytes;
        } finally {
            readerWriterLock.getWriteLock().unlock();
        }

    }

    protected <K extends Comparable<K>> void verify(CachedFieldValueReader cachedFieldValueReader) throws InternalOperationException, DeserializationException {
        Bitmap<Integer> nullsBitmap = CollectionSerializationUtil.getNullsBitmap(collection, cachedFieldValueReader.getBytes());

        List<Scheme.Field> fields = collection.getFields();
        fields.sort(Comparator.comparing(Scheme.Field::getId));

        int index = -1;
        for (Scheme.Field field : fields) {
            index++;

            if (nullsBitmap.isOn(index) && !field.isNullable()) {
                throw new VerificationException("%s can't be null".formatted(field.getName()));
            }

            if (field.isIndexed() && (field.getIndex().isUnique() || field.getIndex().isPrimary()) && !field.getIndex().isAutoIncrement()) {
                @SuppressWarnings("unchecked")
                UniqueQueryableIndex<K, T> uniqueIndexManager = (UniqueQueryableIndex<K, T>) collectionIndexProvider.getUniqueIndexManager(field);
                K value = cachedFieldValueReader.get(field);
                Optional<T> optional = uniqueIndexManager.getIndex(value);
                if (optional.isPresent()) {
                    throw new IndexExistsException("field %s is unique but the provided value is not: %s".formatted(field.getName(), value));
                }
            }
        }

    }

    protected <K extends Comparable<K>> void handleAutoGeneratedPrimaryKey(CachedFieldValueReader cachedFieldValueReader) throws InternalOperationException, DeserializationException, SerializationException {
        List<Scheme.Field> sortedFields = collection.getSortedFields();
        Optional<Scheme.Field> primaryKeyOptional = sortedFields.stream().filter(field -> field.isIndexed() && field.getIndex().isPrimary()).findFirst();

        if (primaryKeyOptional.isEmpty())
            return;

        Scheme.Field field = primaryKeyOptional.get();

        if (!field.getIndex().isAutoIncrement())
            return;

        Bitmap<Integer> nullsBitmap = CollectionSerializationUtil.getNullsBitmap(collection, cachedFieldValueReader.getBytes());
        nullsBitmap.off(sortedFields.indexOf(field));
        CollectionSerializationUtil.setNullsBitmap(
                collection,
                cachedFieldValueReader.getBytes(),
                nullsBitmap.getData()
        );

        @SuppressWarnings("unchecked")
        UniqueQueryableIndex<K, T> uniqueIndexManager = (UniqueQueryableIndex<K, T>) collectionIndexProvider.getUniqueIndexManager(field);
        K pk = uniqueIndexManager.nextKey();

        @SuppressWarnings("unchecked")
        Serializer<K> serializer = (Serializer<K>) SerializerRegistry.getInstance().getSerializer(field.getType());
        CollectionSerializationUtil.setValueOfField(
                collection,
                field,
                cachedFieldValueReader.getBytes(),
                serializer.serialize(pk)
        );
    }


    @SuppressWarnings("unchecked")
    private <K extends Comparable<K>> void storeFieldIndexes(CachedFieldValueReader cachedFieldValueReader, T clusterId, Pointer pointer) throws InternalOperationException, DeserializationException, SerializationException {
        Bitmap<Integer> nullsBitmap = CollectionSerializationUtil.getNullsBitmap(collection, cachedFieldValueReader.getBytes());

        int fieldIndex = -1;
        List<Scheme.Field> fields = collection.getFields();
        fields.sort(Comparator.comparingInt(Scheme.Field::getId));

        for (Scheme.Field field : fields) {
            fieldIndex++;
            if (!field.isIndexed())
                continue;

            try {
                if (field.getIndex().isUnique() || field.getIndex().isPrimary()) {
                    UniqueQueryableIndex<K, T> uniqueIndexManager = (UniqueQueryableIndex<K, T>) collectionIndexProvider.getUniqueIndexManager(field);
                    if (field.isNullable() && nullsBitmap.isOn(fieldIndex)) {
                        uniqueIndexManager.addNull(clusterId);
                    } else {
                        uniqueIndexManager.addIndex(
                                cachedFieldValueReader.get(field),
                                clusterId
                        );
                    }
                } else {
                    DuplicateQueryableIndex<?, T> duplicateIndexManager = (DuplicateQueryableIndex<?, T>) collectionIndexProvider.getDuplicateIndexManager(field);

                    if (field.isNullable() && nullsBitmap.isOn(fieldIndex)) {
                        duplicateIndexManager.addNull(clusterId);
                    } else {
                        duplicateIndexManager.addIndex(
                                cachedFieldValueReader.get(field),
                                clusterId
                        );
                    }
                }
            } catch (InternalOperationException | DeserializationException e) {
                try {
                    clusterIndexManager.removeIndex(clusterId);
                } catch (InternalOperationException ex) {
                    logger.error("Failed to store index, but also failed to rollback cluster id. Root error: {}, rollback error: {}", e.getMessage(), ex.getMessage());
                    throw e;
                }

                try {
                    storageManager.remove(pointer);
                } catch (InternalOperationException ep) {
                    logger.error("Failed to store index, but also failed to remove object from storage manager. Root error {}, storage manager removal error: {}", e.getMessage(), ep.getMessage());
                }

                throw e;
            }
        }
    }

    /*
    *   Frankly, the while loop below should only get executed only once in any scenario!
    *   Not going to perform same approach for primary key indexes
    */
    private T storeClusterIndex(Pointer pointer) throws InternalOperationException {
        T key = null;

        boolean stored = false;
        while (!stored) {
            try {
                key = clusterIndexManager.nextKey();
                clusterIndexManager.addIndex(key, pointer);
                stored = true;
            } catch (IndexExistsException | DeserializationException ignored){}
        }
        return key;
    }
}
