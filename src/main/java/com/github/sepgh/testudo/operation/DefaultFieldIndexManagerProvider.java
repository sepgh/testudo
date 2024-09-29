package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.index.DuplicateBPlusTreeIndexManagerBridge;
import com.github.sepgh.testudo.index.DuplicateIndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.index.data.PointerIndexBinaryObject;
import com.github.sepgh.testudo.index.tree.BPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.serialization.Serializer;
import com.github.sepgh.testudo.serialization.SerializerRegistry;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.storage.index.IndexStorageManagerFactory;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

public class DefaultFieldIndexManagerProvider extends FieldIndexManagerProvider {
    private final Map<String, UniqueTreeIndexManager<?, ?>> uniqueIndexManagers = new HashMap<>();
    private final Map<String, DuplicateIndexManager<?, ?>> duplicateIndexManagers = new HashMap<>();
    private final Map<Integer, UniqueTreeIndexManager<?, Pointer>> clusterIndexManagers = new HashMap<>();
    private final DatabaseStorageManager databaseStorageManager;
    private final Scheme.Field clusterField;

    public DefaultFieldIndexManagerProvider(EngineConfig engineConfig, IndexStorageManagerFactory indexStorageManagerFactory, DatabaseStorageManager databaseStorageManager) {
        super(engineConfig, indexStorageManagerFactory);
        this.databaseStorageManager = databaseStorageManager;
        clusterField = Scheme.Field.builder()
                .id(-1)
                .type(engineConfig.getClusterKeyType().getTypeName())
                .meta(Scheme.Meta.builder().min(1).build())
                .build();
    }

    protected String getPoolId(Scheme.Collection collection, Scheme.Field field){
        return "%d_%d".formatted(collection.getId(), field.getId());
    }

    protected UniqueTreeIndexManager<?, ?> buildUniqueIndexManager(Scheme.Collection collection, Scheme.Field field) {
        Preconditions.checkArgument(field.isPrimary() || field.isIndexUnique(), "Field should either be primary or unique to build a UniqueIndexManager");

        // Raw use of field.id as indexId would force the scheme designer to use unique field ids per whole DB
        // However, using hash code of pool id (which is combination of collection.id and field.id) forces the scheme designer
        //          to only use unique field ids per collection
        int indexId = getPoolId(collection, field).hashCode();

        Serializer<?> serializer = SerializerRegistry.getInstance().getSerializer(field.getType());
        Serializer<?> clusterSerializer = SerializerRegistry.getInstance().getSerializer(engineConfig.getClusterKeyType().getTypeName());

        return new BPlusTreeUniqueTreeIndexManager<>(
                indexId,
                engineConfig.getBTreeDegree(),
                indexStorageManagerFactory.create(collection, field),
                serializer.getIndexBinaryObjectFactory(field),
                clusterSerializer.getIndexBinaryObjectFactory(getClusterField())
        );

    }

    protected final Scheme.Field getClusterField() {
        return clusterField;
    }

    protected UniqueTreeIndexManager<?, Pointer> buildClusterIndexManager(Scheme.Collection collection) {
        Scheme.Field field = getClusterField();
        int indexId = getPoolId(collection, field).hashCode();
        Serializer<?> serializer = SerializerRegistry.getInstance().getSerializer(field.getType());

        return new ClusterBPlusTreeUniqueTreeIndexManager<>(
                indexId,
                engineConfig.getBTreeDegree(),
                indexStorageManagerFactory.create(collection, field),
                serializer.getIndexBinaryObjectFactory(field)
        );

    }

    protected <K extends Comparable<K>, V extends Number & Comparable<V>> DuplicateIndexManager<K, V> buildDuplicateIndexManager(Scheme.Collection collection, Scheme.Field field) {
        int indexId = getPoolId(collection, field).hashCode();

        UniqueTreeIndexManager<?, ?> uniqueTreeIndexManager = uniqueIndexManagers.computeIfAbsent(getPoolId(collection, field), key -> {
            Serializer<?> fieldSerializer = SerializerRegistry.getInstance().getSerializer(field.getType());

            return new BPlusTreeUniqueTreeIndexManager<>(
                    indexId,
                    engineConfig.getBTreeDegree(),
                    indexStorageManagerFactory.create(collection, field),
                    fieldSerializer.getIndexBinaryObjectFactory(field),
                    new PointerIndexBinaryObject.Factory()
            );
        });

        Serializer<?> clusterSerializer = SerializerRegistry.getInstance().getSerializer(engineConfig.getClusterKeyType().getTypeName());

        return new DuplicateBPlusTreeIndexManagerBridge<>(
                collection.getId(),
                engineConfig,
                (UniqueTreeIndexManager<K, Pointer>) uniqueTreeIndexManager,
                (IndexBinaryObjectFactory<V>) clusterSerializer.getIndexBinaryObjectFactory(getClusterField()),
                databaseStorageManager
        );
    }

    @Override
    public synchronized UniqueTreeIndexManager<?, Pointer> getClusterIndexManager(Scheme.Collection collection) {
        Preconditions.checkNotNull(collection);
        return clusterIndexManagers.computeIfAbsent(collection.getId(), key -> buildClusterIndexManager(collection));
    }

    @Override
    public synchronized UniqueTreeIndexManager<?, ?> getUniqueIndexManager(Scheme.Collection collection, Scheme.Field field) {
        Preconditions.checkNotNull(collection);
        Preconditions.checkNotNull(field);
        Preconditions.checkArgument(field.isIndex() || field.isPrimary(), "Index Manager can only be requested for indexed fields");
        Preconditions.checkArgument(field.isIndexUnique() || field.isPrimary(), "Unique index manager can only be created for fields with index set as unique or primary");

        return uniqueIndexManagers.computeIfAbsent(getPoolId(collection, field), key -> buildUniqueIndexManager(collection, field));
    }

    @Override
    public synchronized DuplicateIndexManager<?, ?> getDuplicateIndexManager(Scheme.Collection collection, Scheme.Field field) {
        Preconditions.checkNotNull(collection);
        Preconditions.checkNotNull(field);
        Preconditions.checkArgument(field.isIndex(), "Index Manager can only be requested for indexed fields");
        Preconditions.checkArgument(!field.isIndexUnique(), "Duplicate index manager can only be created for fields with index NOT set as unique");

        return duplicateIndexManagers.computeIfAbsent(getPoolId(collection, field), key -> buildDuplicateIndexManager(collection, field));
    }

    @Override
    public void clearIndexManager(Scheme.Collection collection, Scheme.Field field) {
        uniqueIndexManagers.remove(getPoolId(collection, field));
    }

}
