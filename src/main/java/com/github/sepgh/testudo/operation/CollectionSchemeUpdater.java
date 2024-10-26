package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.exception.*;
import com.github.sepgh.testudo.index.DuplicateIndexManager;
import com.github.sepgh.testudo.index.KeyValue;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.scheme.SchemeManager;
import com.github.sepgh.testudo.serialization.CollectionSerializationUtil;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.utils.LockableIterator;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;


public class CollectionSchemeUpdater {
    private SchemeManager.CollectionFieldsUpdate collectionFieldsUpdate;
    private DatabaseStorageManager databaseStorageManager;
    private SchemeManager schemeManager;

    public CollectionSchemeUpdater(
            DatabaseStorageManager databaseStorageManager,
            SchemeManager schemeManager
    ) {
        this.databaseStorageManager = databaseStorageManager;
        this.schemeManager = schemeManager;
    }

    public void reset(
            SchemeManager.CollectionFieldsUpdate collectionFieldsUpdate
    ) {
        this.collectionFieldsUpdate = collectionFieldsUpdate;
    }

    /*  Update process
    *   Grab all rows/objects using cluster IndexManager and loop over them
    *   Keep it the same or Compare and update the object
    *   Update indexes
    */
    @SneakyThrows
    public <K extends Number & Comparable<K>> void update() {
        assert this.collectionFieldsUpdate != null;

        this.validateTypeCompatibilities();

        UniqueTreeIndexManager<K, Pointer> clusterIndexManager = (UniqueTreeIndexManager<K, Pointer>) this.schemeManager.getClusterIndexManager(this.collectionFieldsUpdate.getBefore());

        LockableIterator<? extends KeyValue<K, Pointer>> lockableIterator = clusterIndexManager.getSortedIterator();

        List<K> removedObjects = new ArrayList<>();

        lockableIterator.forEachRemaining(keyValue -> {
            Pointer pointer = keyValue.value();
            try {
                Optional<DBObject> dbObjectOptional = databaseStorageManager.select(pointer);
                if (dbObjectOptional.isEmpty() || !dbObjectOptional.get().isAlive()) {
                    removedObjects.add(keyValue.key());
                    return;
                }

                DBObject dbObject = dbObjectOptional.get();

                if (collectionFieldsUpdate.getVersion() <= dbObject.getVersion())
                    return;

                dbObject.setVersion(collectionFieldsUpdate.getVersion());

                byte[] bytes;

                if (hasSpace(dbObject, this.collectionFieldsUpdate.getAfter())){
                    try {
                        update(dbObject);
                    } catch (SerializationException e) {
                        throw new RuntimeException(e);
                    }

                    bytes = dbObject.getData();
                    this.databaseStorageManager.update(pointer, dbObject1 -> {
                        dbObject1.setVersion(collectionFieldsUpdate.getVersion());
                        try {
                            dbObject1.modifyData(bytes);
                        } catch (VerificationException.InvalidDBObjectWrapper e) {
                            throw new RuntimeException(e);
                        }
                    });

                } else {
                    dbObject.deactivate();
                    try {
                        bytes = createNew(dbObject);
                        Pointer newPointer = this.databaseStorageManager.store(
                                this.collectionFieldsUpdate.getAfter().getId(),
                                this.collectionFieldsUpdate.getVersion(),
                                bytes
                        );
                        clusterIndexManager.updateIndex(keyValue.key(), newPointer);
                        this.databaseStorageManager.remove(pointer);
                    } catch (IOException | ExecutionException | InterruptedException | IndexExistsException | InternalOperationException | IndexMissingException | SerializationException  e) {
                        throw new RuntimeException(e);
                    }
                }

                updateIndexes(bytes, keyValue.key());
            } catch (DeserializationException | IndexExistsException | InternalOperationException | IOException | ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        purgeIndexesOfRemovedFields();

        removedObjects.forEach(k -> {
            try {
                clusterIndexManager.removeIndex(k);
            } catch (InternalOperationException e) {
                throw new RuntimeException(e);
            }
        });

    }

    private void purgeIndexesOfRemovedFields() {
        for (Scheme.Field field : collectionFieldsUpdate.getRemovedFields()) {
            if (field.isIndex()){
                if (field.isIndexUnique()){
                    UniqueTreeIndexManager<?, ?> uniqueTreeIndexManager = this.schemeManager.getCollectionIndexProviderFactory().create(collectionFieldsUpdate.getBefore()).getUniqueIndexManager(field);
                    uniqueTreeIndexManager.purgeIndex();
                } else {
                    DuplicateIndexManager<?, ?> duplicateIndexManager = this.schemeManager.getCollectionIndexProviderFactory().create(collectionFieldsUpdate.getBefore()).getDuplicateIndexManager(field);
                    duplicateIndexManager.purgeIndex();
                }
            }
        }
    }

    private <K extends Comparable<K>, V extends Number & Comparable<V>> void updateIndexes(byte[] obj, V clusterId) throws DeserializationException, IndexExistsException, InternalOperationException, IOException, ExecutionException, InterruptedException {
        for (Scheme.Field field : collectionFieldsUpdate.getNewFields()) {
            if (!field.isIndex()){
                return;
            }

            K key = CollectionSerializationUtil.getValueOfFieldAsObject(collectionFieldsUpdate.getAfter(), field, obj);
            if (field.isIndexUnique()){
                UniqueTreeIndexManager<K, V> uniqueTreeIndexManager = (UniqueTreeIndexManager<K, V>) this.schemeManager.getCollectionIndexProviderFactory().create(collectionFieldsUpdate.getAfter()).getUniqueIndexManager(field);
                uniqueTreeIndexManager.addIndex(key, clusterId);
            } else {
                DuplicateIndexManager<K, V> duplicateIndexManager = (DuplicateIndexManager<K, V>) this.schemeManager.getCollectionIndexProviderFactory().create(collectionFieldsUpdate.getBefore()).getDuplicateIndexManager(field);
                duplicateIndexManager.addIndex(key, clusterId);
            }
        }

    }

    private <K extends Comparable<K>> byte[] createNew(DBObject dbObject) throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException, IndexMissingException, SerializationException {
        Map<Integer, byte[]> valueMap = new HashMap<>();

        collectionFieldsUpdate.getBefore().getFields().forEach(field -> {
            valueMap.put(
                    field.getId(),
                    CollectionSerializationUtil.getValueOfField(
                            collectionFieldsUpdate.getBefore(),
                            field,
                            dbObject
                    )
            );
        });

        byte[] newObj = new byte[CollectionSerializationUtil.getSizeOfCollection(collectionFieldsUpdate.getAfter())];
        for (Scheme.Field field : collectionFieldsUpdate.getAfter().getFields()) {
            CollectionSerializationUtil.setValueOfField(
                    collectionFieldsUpdate.getAfter(),
                    field,
                    newObj,
                    valueMap.get(field.getId())
            );
        }

        return newObj;
    }

    private boolean hasSpace(DBObject dbObject, Scheme.Collection newCollection) {
        return dbObject.getDataSize() >= CollectionSerializationUtil.getSizeOfCollection(newCollection);
    }

    private void update(
            DBObject dbObject
    ) throws SerializationException {
        Map<Integer, byte[]> valueMap = new HashMap<>();

        collectionFieldsUpdate.getBefore().getFields().forEach(field -> {
            valueMap.put(
                    field.getId(),
                    CollectionSerializationUtil.getValueOfField(
                        collectionFieldsUpdate.getBefore(),
                        field,
                        dbObject
                    )
            );
        });

        // Setting position of each remaining field
        byte[] update = new byte[dbObject.getDataSize()];
        for (Scheme.Field field : collectionFieldsUpdate.getAfter().getFields()) {
            CollectionSerializationUtil.setValueOfField(
                    collectionFieldsUpdate.getAfter(),
                    field,
                    update,
                    valueMap.get(field.getId())
            );
        }

        dbObject.modifyData(0, update);
    }


    private void validateTypeCompatibilities(){
        List<SchemeManager.CollectionFieldsUpdate.UpdateField> updatedFields =
                this.collectionFieldsUpdate.getUpdatedFields();

        for (SchemeManager.CollectionFieldsUpdate.UpdateField updatedField : updatedFields) {
            if (!CollectionSerializationUtil.areTypesCompatible(
                    updatedField.beforeField().getType(),
                    updatedField.afterField().getType())
            ) {
                // Todo: err
            }

        }
    }

}
