package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.exception.*;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.data.IndexBinaryObject;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.scheme.SchemeManager;
import com.github.sepgh.testudo.serialization.CollectionSerializationUtil;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.utils.LockableIterator;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;


// Todo:
// Index updating: Removed fields needs to drop indexes
//                 An index itself may have been removed as well
//                 Newly added indexes should also be handled
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

    @SneakyThrows
    public <K extends Comparable<K>> void update() {
        assert this.collectionFieldsUpdate != null;

        this.validateTypeCompatibilities();

        IndexManager<K, Pointer> pkIndexManager = (IndexManager<K, Pointer>) this.schemeManager.getPKIndexManager(this.collectionFieldsUpdate.getBefore());

        LockableIterator<? extends AbstractLeafTreeNode.KeyValue<K, Pointer>> lockableIterator = pkIndexManager.getSortedIterator();

        lockableIterator.forEachRemaining(keyValue -> {
            Pointer pointer = keyValue.value();
            try {
                databaseStorageManager.update(
                        pointer,
                        dbObject -> {
                            if (collectionFieldsUpdate.getVersion() <= dbObject.getVersion())
                                return;

                            byte[] bytes;

                            if (hasSpace(dbObject, this.collectionFieldsUpdate.getAfter())){
                                try {
                                    update(dbObject);
                                    bytes = dbObject.getData();
                                } catch (SerializationException e) {
                                    throw new RuntimeException(e);
                                }
                            } else {
                                dbObject.deactivate();
                                try {
                                    bytes = createNew(dbObject, pkIndexManager, keyValue);
                                } catch (IOException | ExecutionException | InterruptedException |
                                         IndexExistsException | InternalOperationException |
                                         IndexBinaryObject.InvalidIndexBinaryObject |
                                         IndexMissingException | SerializationException e) {
                                    throw new RuntimeException(e);
                                }
                            }

                            try {
                                Comparable fieldAsObject = CollectionSerializationUtil.getValueOfFieldAsObject(
                                        collectionFieldsUpdate.getAfter(),
                                        collectionFieldsUpdate.getAfter().getPrimaryField().get(),
                                        bytes
                                );
                                updateIndexes(bytes, fieldAsObject);
                            } catch (DeserializationException | IndexExistsException | InternalOperationException |
                                     IndexBinaryObject.InvalidIndexBinaryObject e) {
                                throw new RuntimeException(e);
                            }

                        }
                );
            } catch (IOException | ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

    }

    private <K extends Comparable<K>, V extends Comparable<V>> void updateIndexes(byte[] obj, V pk) throws DeserializationException, IndexExistsException, InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject {
        for (Scheme.Field field : collectionFieldsUpdate.getRemovedFields()) {
            if (field.isIndex()){
                IndexManager<?, ?> indexManager = this.schemeManager.getFieldIndexManagerProvider().getIndexManager(collectionFieldsUpdate.getBefore(), field);
                indexManager.purgeIndex();
            }
        }

        for (Scheme.Field field : collectionFieldsUpdate.getNewFields()) {
            if (!field.isIndex()){
                return;
            }

            IndexManager<K, V> indexManager = (IndexManager<K, V>) this.schemeManager.getFieldIndexManagerProvider().getIndexManager(collectionFieldsUpdate.getAfter(), field);
            K value = CollectionSerializationUtil.getValueOfFieldAsObject(collectionFieldsUpdate.getAfter(), field, obj);
            indexManager.addIndex(value, pk);
        }

    }

    private <K extends Comparable<K>> byte[] createNew(DBObject dbObject, IndexManager<K, Pointer> pkIndexManager, AbstractLeafTreeNode.KeyValue<K, Pointer> keyValue) throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject, IndexMissingException, SerializationException {
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

        Pointer pointer = databaseStorageManager.store(
                collectionFieldsUpdate.getAfter().getId(),
                schemeManager.getScheme().getVersion(),
                newObj
        );

        pkIndexManager.updateIndex(keyValue.key(), pointer);
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
