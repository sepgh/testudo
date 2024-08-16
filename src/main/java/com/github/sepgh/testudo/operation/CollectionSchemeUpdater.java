package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.IndexMissingException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
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
                            if (hasSpace(dbObject, this.collectionFieldsUpdate.getAfter())){
                                try {
                                    update(dbObject, this.collectionFieldsUpdate);
                                } catch (SerializationException e) {
                                    throw new RuntimeException(e);
                                }
                            } else {
                                dbObject.deactivate();
                                try {
                                    createNew(dbObject, pkIndexManager, keyValue, this.collectionFieldsUpdate);
                                } catch (IOException | ExecutionException | InterruptedException |
                                         IndexExistsException | InternalOperationException |
                                         ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue |
                                         IndexMissingException | SerializationException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                            updateIndexes(collectionFieldsUpdate);
                        }
                );
            } catch (IOException | ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

    }

    private void updateIndexes(SchemeManager.CollectionFieldsUpdate collectionFieldsUpdate) {
        collectionFieldsUpdate.getRemovedFields().forEach(field -> {
            if (field.isIndex()){
                IndexManager<?, ?> indexManager = this.schemeManager.getFieldIndexManagerProvider().getIndexManager(collectionFieldsUpdate.getBefore(), field);
                indexManager.purgeIndex();
            }
        });
    }

    private <K extends Comparable<K>> void createNew(DBObject dbObject, IndexManager<K, Pointer> pkIndexManager, AbstractLeafTreeNode.KeyValue<K, Pointer> keyValue, SchemeManager.CollectionFieldsUpdate collectionFieldsUpdate) throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexMissingException, SerializationException {
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
    }

    private boolean hasSpace(DBObject dbObject, Scheme.Collection newCollection) {
        return dbObject.getDataSize() >= CollectionSerializationUtil.getSizeOfCollection(newCollection);
    }

    private void update(
            DBObject dbObject,
            SchemeManager.CollectionFieldsUpdate collectionFieldsUpdate
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
