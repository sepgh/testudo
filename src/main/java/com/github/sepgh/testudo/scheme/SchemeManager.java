package com.github.sepgh.testudo.scheme;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.operation.CollectionIndexManagerProvider;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.utils.LockableIterator;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class SchemeManager implements SchemeComparator.SchemeComparisonListener {

    private final Scheme scheme;
    private Scheme oldScheme = null;
    private final EngineConfig engineConfig;
    private final Gson gson = new GsonBuilder().serializeNulls().create();
    private final Config config;
    private final CollectionIndexManagerProvider collectionIndexManagerProvider;
    private final DatabaseStorageManager databaseStorageManager;

    public SchemeManager(EngineConfig engineConfig, Scheme scheme, Config config, CollectionIndexManagerProvider collectionIndexManagerProvider, DatabaseStorageManager databaseStorageManager) {
        this.scheme = scheme;
        this.engineConfig = engineConfig;
        this.config = config;
        this.collectionIndexManagerProvider = collectionIndexManagerProvider;
        this.databaseStorageManager = databaseStorageManager;
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

    private void compareSchemes() {
        SchemeComparator schemeComparator = new SchemeComparator(this.oldScheme, this.scheme);
        schemeComparator.compare(this);
    }


    @Override
    public void onChange(SchemeComparator.DifferenceReason differenceReason, Scheme.Collection collection, @Nullable Scheme.Field oldField, @Nullable Scheme.Field newField) {
        switch (differenceReason) {
            case NEW_FIELD -> {
                this.addNewField(collection, newField);
            }
            case FIELD_META_CHANGED -> {
                this.metaUpdated(collection, oldField, newField);
            }
            case FIELD_TYPE_CHANGED -> {
                this.fieldTypeChanged(collection, oldField, newField);
            }
            case FIELD_INDEX_CHANGED -> {
                this.indexChanged(collection, oldField, newField);
            }
            case FIELD_REMOVED -> {
                this.fieldRemoved(collection, oldField);
            }
            case NEW_COLLECTION -> {
                this.newCollection(collection);
            }
            case COLLECTION_REMOVED -> {
                this.collectionRemoved(collection);
            }
        }
    }

    /*  TODO  */

    @SneakyThrows
    private void removeDBObject(AbstractLeafTreeNode.KeyValue<?, ?> keyValue) {
        Pointer pointer = (Pointer) keyValue.value();
        databaseStorageManager.remove(pointer);
    }

    // Todo
    @SneakyThrows
    private LockableIterator<? extends AbstractLeafTreeNode.KeyValue<?, ?>> getPKIterator(Scheme.Collection collection) {
        Optional<Scheme.Field> optionalField = collection.getFields().stream().filter(Scheme.Field::isPrimary).findFirst();
        Scheme.Field primaryField = optionalField.get();
        int primaryFieldId = primaryField.getId();

        IndexManager<?, ?> indexManager = this.collectionIndexManagerProvider.getIndexManager(collection.getId());
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
        IndexManager<?, ?> indexManager = this.collectionIndexManagerProvider.getIndexManager(collection.getId());
        collection.getFields().forEach(field -> {
            if (field.isIndex())
                indexManager.purgeIndex();
        });
    }

    private void newCollection(Scheme.Collection collection) {
    }

    private void fieldRemoved(Scheme.Collection collection, Scheme.Field oldField) {
        LockableIterator<? extends AbstractLeafTreeNode.KeyValue<?, ?>> lockableIterator = getPKIterator(collection);
        lockableIterator.forEachRemaining(keyValue -> {
            Pointer pointer = (Pointer) keyValue.value();
            try {
                databaseStorageManager.update(pointer, dbObjectWrapper -> {
                    // Todo: find position of field in obj, replace bytes with 0, move next fields back
                });
            } catch (IOException | ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void indexChanged(Scheme.Collection collection, Scheme.Field oldField, Scheme.Field newField) {
        
    }

    private void fieldTypeChanged(Scheme.Collection collection, Scheme.Field oldField, Scheme.Field newField) {

    }

    private void metaUpdated(Scheme.Collection collection, Scheme.Field oldField, Scheme.Field newField) {
    }

    private void addNewField(Scheme.Collection collection, Scheme.Field newField) {

    }


    @Getter
    @Builder
    public static class Config {
        @Builder.Default
        private boolean commitCollectionRemovals = false;
        @Builder.Default
        private boolean immediateAllocationForNewFields = false;
        @Builder.Default
        private boolean errorForMetaValidationOfExistingData = false;
    }
}
