package com.github.sepgh.testudo.scheme;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.operation.FieldIndexManagerProvider;
import com.github.sepgh.testudo.operation.CollectionSchemeUpdater;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.utils.LockableIterator;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;



// Todo: index updating
public class SchemeManager implements SchemeComparator.SchemeComparisonListener {

    private final Scheme scheme;
    private Scheme oldScheme = null;
    private final EngineConfig engineConfig;
    private final Gson gson = new GsonBuilder().serializeNulls().create();
    private final SchemeUpdateConfig schemeUpdateConfig;
    private final FieldIndexManagerProvider fieldIndexManagerProvider;
    private final DatabaseStorageManager databaseStorageManager;
    private final List<CollectionFieldsUpdate> collectionFieldsUpdateQueue = new LinkedList<>();
    private final Map<Integer, CollectionFieldsUpdate> collectionFieldTypeUpdateMap = new HashMap<>();

    public SchemeManager(EngineConfig engineConfig, Scheme scheme, SchemeUpdateConfig schemeUpdateConfig, FieldIndexManagerProvider fieldIndexManagerProvider, DatabaseStorageManager databaseStorageManager) {
        this.scheme = scheme;
        this.engineConfig = engineConfig;
        this.schemeUpdateConfig = schemeUpdateConfig;
        this.fieldIndexManagerProvider = fieldIndexManagerProvider;
        this.databaseStorageManager = databaseStorageManager;
        init();
    }

    private String getSchemePath() {
        return Path.of(this.engineConfig.getBaseDBPath(), "scheme.json").toString();
    }

    private void loadOldScheme() {
        try {
            FileReader fileReader = new FileReader(getSchemePath());
            JsonReader jsonReader = new JsonReader(fileReader);
            this.oldScheme = gson.fromJson(jsonReader, Scheme.class);
            fileReader.close();
        } catch (FileNotFoundException ignored) {} catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void init() {
        SchemeValidator.validate(this.scheme);
        if (this.oldScheme != null) {
            this.compareSchemes();
        }
    }

    public void update() throws IOException {
        if (!collectionFieldsUpdateQueue.isEmpty()) {
            CollectionSchemeUpdater collectionSchemeUpdater = new CollectionSchemeUpdater(databaseStorageManager, this);

            CollectionFieldsUpdate collectionFieldsUpdate = collectionFieldsUpdateQueue.removeLast();
            while (collectionFieldsUpdate != null) {
                collectionSchemeUpdater.reset(collectionFieldsUpdate);
                collectionSchemeUpdater.update();
                collectionFieldsUpdate = collectionFieldsUpdateQueue.removeLast();
            }
        }

        this.store();
    }

    private void store() throws IOException {
        FileWriter fileWriter = new FileWriter(this.getSchemePath());
        gson.toJson(this.scheme, fileWriter);
        fileWriter.close();

    }

    private void compareSchemes() {
        SchemeComparator schemeComparator = new SchemeComparator(this.oldScheme, this.scheme);
        schemeComparator.compare(this);
    }

    @Override
    public void onChange(SchemeComparator.DifferenceReason differenceReason, Scheme.Collection oldCollection, Scheme.Collection newCollection, @Nullable Scheme.Field oldField, @Nullable Scheme.Field newField) {
        switch (differenceReason) {
            case NEW_FIELD -> this.addNewField(oldCollection, newCollection, newField);
            case FIELD_META_CHANGED -> this.metaUpdated(oldCollection, newCollection, oldField, newField);
            case FIELD_TYPE_CHANGED -> this.fieldTypeChanged(oldCollection, newCollection, oldField, newField);
            case FIELD_INDEX_CHANGED -> this.indexChanged(oldCollection, oldField, newField);
            case FIELD_REMOVED -> this.fieldRemoved(oldCollection, newCollection, oldField);
            case NEW_COLLECTION -> this.newCollection(newCollection);
            case COLLECTION_REMOVED -> this.collectionRemoved(oldCollection);
        }
    }

    /*  TODO  */

    @SneakyThrows
    private void removeDBObject(AbstractLeafTreeNode.KeyValue<?, ?> keyValue) {
        Pointer pointer = (Pointer) keyValue.value();
        databaseStorageManager.remove(pointer);
    }

    public IndexManager<?, ?> getPKIndexManager(Scheme.Collection collection) {
        Optional<Scheme.Field> optionalField = collection.getFields().stream().filter(Scheme.Field::isPrimary).findFirst();
        Scheme.Field primaryField = optionalField.get();
        return this.fieldIndexManagerProvider.getIndexManager(collection, primaryField);
    }

    @SneakyThrows
    public LockableIterator<? extends AbstractLeafTreeNode.KeyValue<?, ?>> getPKIterator(Scheme.Collection collection) {
        Optional<Scheme.Field> optionalField = collection.getFields().stream().filter(Scheme.Field::isPrimary).findFirst();
        Scheme.Field primaryField = optionalField.get();
        IndexManager<?, ?> indexManager = this.fieldIndexManagerProvider.getIndexManager(collection, primaryField);
        return indexManager.getSortedIterator();
    }

    @SneakyThrows
    private void collectionRemoved(Scheme.Collection collection) {
        LockableIterator<? extends AbstractLeafTreeNode.KeyValue<?, ?>> lockableIterator = getPKIterator(collection);
        try {
            lockableIterator.lock();
            lockableIterator.forEachRemaining(this::removeDBObject);
        } finally {
            lockableIterator.unlock();
        }

        collection.getFields().forEach(field -> {
            if (field.isIndex()) {
                IndexManager<?, ?> indexManager = this.fieldIndexManagerProvider.getIndexManager(collection, field);
                indexManager.purgeIndex();
            }
        });
    }

    private void newCollection(Scheme.Collection collection) {
    }

    private void fieldRemoved(Scheme.Collection collection, Scheme.Collection newCollection, Scheme.Field oldField) {
        CollectionFieldsUpdate collectionFieldsUpdate = this.collectionFieldTypeUpdateMap.get(collection.getId());
        if (collectionFieldsUpdate == null) {
            collectionFieldsUpdate = new CollectionFieldsUpdate(this.scheme.getVersion(), collection, newCollection);
            this.collectionFieldTypeUpdateMap.put(collection.getId(), collectionFieldsUpdate);
            this.collectionFieldsUpdateQueue.add(collectionFieldsUpdate);
        }

        collectionFieldsUpdate.addRemovedField(oldField);
    }

    private void indexChanged(Scheme.Collection collection, Scheme.Field oldField, Scheme.Field newField) {
        // TODO
    }

    private void fieldTypeChanged(Scheme.Collection collection, Scheme.Collection newCollection, Scheme.Field oldField, Scheme.Field newField) {
        CollectionFieldsUpdate collectionFieldsUpdate = this.collectionFieldTypeUpdateMap.get(collection.getId());
        if (collectionFieldsUpdate == null) {
            collectionFieldsUpdate = new CollectionFieldsUpdate(this.scheme.getVersion(), collection, newCollection);
            this.collectionFieldTypeUpdateMap.put(collection.getId(), collectionFieldsUpdate);
            this.collectionFieldsUpdateQueue.add(collectionFieldsUpdate);
        }

        collectionFieldsUpdate.addUpdatedField(
                new CollectionFieldsUpdate.UpdateField(
                        oldField,
                        newField
                )
        );

    }

    private void metaUpdated(Scheme.Collection oldCollection, Scheme.Collection newCollection, Scheme.Field oldField, Scheme.Field newField) {
    }

    private void addNewField(Scheme.Collection collection, Scheme.Collection newCollection, Scheme.Field newField) {
        CollectionFieldsUpdate collectionFieldsUpdate = this.collectionFieldTypeUpdateMap.get(collection.getId());
        if (collectionFieldsUpdate == null) {
            collectionFieldsUpdate = new CollectionFieldsUpdate(this.scheme.getVersion(), collection, newCollection);
            this.collectionFieldTypeUpdateMap.put(collection.getId(), collectionFieldsUpdate);
            this.collectionFieldsUpdateQueue.add(collectionFieldsUpdate);
        }

        collectionFieldsUpdate.addNewField(newField);
    }


    @Data
    public static class CollectionFieldsUpdate {
        private final int version;
        private final Scheme.Collection before;
        private final Scheme.Collection after;
        private final List<Scheme.Field> removedFields = new ArrayList<>();
        private final List<UpdateField> updatedFields = new ArrayList<>();
        private final List<Scheme.Field> newFields = new ArrayList<>();

        private CollectionFieldsUpdate(int version, Scheme.Collection before, Scheme.Collection after) {
            this.version = version;
            this.before = before;
            this.after = after;
        }

        public void addRemovedField(Scheme.Field removedField) {
            this.removedFields.add(removedField);
        }

        public void addUpdatedField(UpdateField updatedField) {
            this.updatedFields.add(updatedField);
        }

        public void addNewField(Scheme.Field newField) {
            this.newFields.add(newField);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CollectionFieldsUpdate that = (CollectionFieldsUpdate) o;
            return Objects.equals(getBefore(), that.getBefore());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getBefore());
        }

        public record UpdateField(Scheme.Field beforeField, Scheme.Field afterField) {}
    }

    @Getter
    @Builder
    public static class SchemeUpdateConfig {
        @Builder.Default
        private boolean commitCollectionRemovals = false;
        @Builder.Default
        private boolean errorForMetaValidationOfExistingData = false;
    }
}
