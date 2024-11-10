package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.index.*;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.index.data.PointerIndexBinaryObject;
import com.github.sepgh.testudo.index.tree.BPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.operation.query.Queryable;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.serialization.Serializer;
import com.github.sepgh.testudo.serialization.SerializerRegistry;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.storage.index.IndexStorageManagerFactory;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DefaultCollectionIndexProviderFactory implements CollectionIndexProviderFactory {
    protected final Map<Scheme.Collection, CollectionIndexProvider> providers = new HashMap<>();
    protected final Map<String, UniqueQueryableIndex<?, ? extends Number>> uniqueTreeIndexManagers = new HashMap<>();
    protected final Map<String, UniqueTreeIndexManager<?, Pointer>> clusterIndexManagers = new HashMap<>();
    protected final Map<String, DuplicateQueryableIndex<?, ? extends Number>> duplicateIndexManagers = new HashMap<>();
    protected final EngineConfig engineConfig;
    protected final IndexStorageManagerFactory indexStorageManagerFactory;
    protected final DatabaseStorageManager databaseStorageManager;

    public DefaultCollectionIndexProviderFactory(EngineConfig engineConfig, IndexStorageManagerFactory indexStorageManagerFactory, DatabaseStorageManager databaseStorageManager) {
        this.engineConfig = engineConfig;
        this.indexStorageManagerFactory = indexStorageManagerFactory;
        this.databaseStorageManager = databaseStorageManager;
    }

    @Override
    public CollectionIndexProvider create(Scheme.Collection collection) {
        return providers.computeIfAbsent(collection, this::getProvider);
    }

    protected Scheme.Field getClusterField() {
        return Scheme.Field.builder()
                .id(-1)
                .name("__CLUSTER_ID__")
                .type(engineConfig.getClusterKeyType().getTypeName())
                .meta(Scheme.Meta.builder().build())
                .build();
    }

    protected String getIndexId(Scheme.Collection collection, Scheme.Field field){
        return "%d_%d".formatted(collection.getId(), field.getId());
    }

    protected UniqueQueryableIndex<?, ? extends Number> buildUniqueIndexManager(Scheme.Collection collection, Scheme.Field field) {
        Preconditions.checkArgument(field.isPrimary() || field.isIndexUnique(), "Field should either be primary or unique to build a UniqueIndexManager");

        // Raw use of field.id as indexId would force the scheme designer to use unique field ids per whole DB
        // However, using hash code of pool id (which is combination of collection.id and field.id) forces the scheme designer
        //          to only use unique field ids per collection
        int indexId = getIndexId(collection, field).hashCode();

        Serializer<?> serializer = SerializerRegistry.getInstance().getSerializer(field.getType());
        Serializer<?> clusterSerializer = SerializerRegistry.getInstance().getSerializer(engineConfig.getClusterKeyType().getTypeName());

        return (UniqueQueryableIndex<?, ? extends Number>) new BPlusTreeUniqueTreeIndexManager<>(
                indexId,
                engineConfig.getBTreeDegree(),
                indexStorageManagerFactory.create(collection, field),
                serializer.getIndexBinaryObjectFactory(field),
                clusterSerializer.getIndexBinaryObjectFactory(getClusterField())
        );

    }

    protected UniqueTreeIndexManager<?, Pointer> buildClusterIndexManager(Scheme.Collection collection) {
        Scheme.Field field = getClusterField();
        int indexId = getIndexId(collection, field).hashCode();
        Serializer<?> serializer = SerializerRegistry.getInstance().getSerializer(field.getType());

        return new ClusterBPlusTreeUniqueTreeIndexManager<>(
                indexId,
                engineConfig.getBTreeDegree(),
                indexStorageManagerFactory.create(collection, field),
                serializer.getIndexBinaryObjectFactory(field)
        );
    }

    protected <K extends Comparable<K>, V extends Number & Comparable<V>> DuplicateQueryableIndex<K, V> buildDuplicateIndexManager(Scheme.Collection collection, Scheme.Field field) {
        int indexId = getIndexId(collection, field).hashCode();

        Serializer<?> fieldSerializer = SerializerRegistry.getInstance().getSerializer(field.getType());
        BPlusTreeUniqueTreeIndexManager<?, Pointer> uniqueTreeIndexManager = new BPlusTreeUniqueTreeIndexManager<>(
                indexId,
                engineConfig.getBTreeDegree(),
                indexStorageManagerFactory.create(collection, field),
                fieldSerializer.getIndexBinaryObjectFactory(field),
                new PointerIndexBinaryObject.Factory()
        );

        Serializer<?> clusterSerializer = SerializerRegistry.getInstance().getSerializer(engineConfig.getClusterKeyType().getTypeName());

        // Bitmap or B+Tree?
        if (field.isLowCardinality()){
            return new DuplicateBitmapIndexManager<>(
                    collection.getId(),
                    (UniqueQueryableIndex<K, Pointer>) uniqueTreeIndexManager,
                    (IndexBinaryObjectFactory<V>) clusterSerializer.getIndexBinaryObjectFactory(getClusterField()),
                    databaseStorageManager
            );
        }
        return new DuplicateBPlusTreeIndexManagerBridge<>(
                collection.getId(),
                engineConfig,
                (UniqueQueryableIndex<K, Pointer>) uniqueTreeIndexManager,
                (IndexBinaryObjectFactory<V>) clusterSerializer.getIndexBinaryObjectFactory(getClusterField()),
                databaseStorageManager
        );
    }

    protected CollectionIndexProvider getProvider(Scheme.Collection collection) {

        return new CollectionIndexProvider() {
            @Override
            public UniqueQueryableIndex<?, ? extends Number> getUniqueIndexManager(Scheme.Field field) {
                Preconditions.checkNotNull(field);
                Preconditions.checkArgument(field.isIndex() || field.isPrimary(), "Index Manager can only be requested for indexed fields");
                Preconditions.checkArgument(field.isIndexUnique() || field.isPrimary(), "Unique index manager can only be created for fields with index set as unique or primary");
                return uniqueTreeIndexManagers.computeIfAbsent(getIndexId(collection, field), key -> buildUniqueIndexManager(collection, field));
            }

            @Override
            public DuplicateQueryableIndex<?, ?> getDuplicateIndexManager(Scheme.Field field) {
                return duplicateIndexManagers.computeIfAbsent(getIndexId(collection, field), key -> buildDuplicateIndexManager(collection, field));
            }

            @Override
            public UniqueTreeIndexManager<?, Pointer> getClusterIndexManager() {
                return clusterIndexManagers.computeIfAbsent(getIndexId(collection, getClusterField()), key -> buildClusterIndexManager(collection));
            }

            @Override
            public UniqueQueryableIndex<?, ? extends Number> getUniqueIndexManager(String fieldName) {
                Optional<Scheme.Field> fieldOptional = collection.getFields().stream().filter(field -> field.getName().equals(fieldName)).findFirst();
                if (fieldOptional.isEmpty()) {
                    throw new IllegalArgumentException("Field " + fieldName + " not found");
                }
                return getUniqueIndexManager(fieldOptional.get());
            }

            @Override
            public DuplicateQueryableIndex<?, ? extends Number> getDuplicateIndexManager(String fieldName) {
                Optional<Scheme.Field> fieldOptional = collection.getFields().stream().filter(field -> field.getName().equals(fieldName)).findFirst();
                if (fieldOptional.isEmpty()) {
                    throw new IllegalArgumentException("Field " + fieldName + " not found");
                }
                return getDuplicateIndexManager(fieldOptional.get());
            }

            @Override
            public Queryable<?, ? extends Number> getQueryableIndex(Scheme.Field field) {
                return field.isIndexUnique() ? getUniqueIndexManager(field) : getDuplicateIndexManager(field);
            }

            @Override
            public Queryable<?, ? extends Number> getQueryableIndex(String fieldName) {
                Optional<Scheme.Field> fieldOptional = collection.getFields().stream().filter(field -> field.getName().equals(fieldName)).findFirst();
                if (fieldOptional.isEmpty()) {
                    throw new IllegalArgumentException("Field " + fieldName + " not found");
                }
                return getQueryableIndex(fieldOptional.get());
            }
        };
    }
}
