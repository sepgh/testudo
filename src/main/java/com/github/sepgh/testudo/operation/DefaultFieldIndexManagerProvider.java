package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.data.*;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.serialization.FieldType;
import com.github.sepgh.testudo.storage.index.IndexStorageManagerFactory;

import java.util.HashMap;
import java.util.Map;

public class DefaultFieldIndexManagerProvider extends FieldIndexManagerProvider {
    private final Map<String, IndexManager<?, ?>> indexManagers = new HashMap<>();
    private final Map<String, IndexBinaryObjectFactory<?>> typeToImmutableBinaryObjectWrappers = new HashMap<>();

    public DefaultFieldIndexManagerProvider(EngineConfig engineConfig, IndexStorageManagerFactory indexStorageManagerFactory) {
        super(engineConfig, indexStorageManagerFactory);
        this.registerTypeToImmutableBinaryObjectWrapper(
                FieldType.INT.getName(), engineConfig.isSupportZeroInClusterKeys() ? new IntegerIndexBinaryObject.Factory() : new NoZeroIntegerIndexBinaryObject.Factory()
        );
        this.registerTypeToImmutableBinaryObjectWrapper(
                FieldType.LONG.getName(), engineConfig.isSupportZeroInClusterKeys() ? new LongIndexBinaryObject.Factory() : new NoZeroLongIndexBinaryObject.Factory()
        );
    }

    private String getPoolId(Scheme.Collection collection, Scheme.Field field){
        return "%d_%d".formatted(collection.getId(), field.getId());
    }

    private IndexManager<?, ?> buildIndexManager(Scheme.Collection collection, Scheme.Field field) {
        IndexManager<?, ?> indexManager = null;
        if (field.isPrimary()){
            indexManager = new ClusterBPlusTreeIndexManager<>(
                    field.getId(),
                    engineConfig.getBTreeDegree(),
                    indexStorageManagerFactory.create(collection, field),
                    this.typeToImmutableBinaryObjectWrappers.get(field.getType())
            );
        } else {
            // Todo: this isn't cluster index
            indexManager = new ClusterBPlusTreeIndexManager<>(
                    field.getId(),
                    engineConfig.getBTreeDegree(),
                    indexStorageManagerFactory.create(collection, field),
                    this.typeToImmutableBinaryObjectWrappers.get(field.getType())
            );
        }

        return indexManager;
    }

    @Override
    public synchronized IndexManager<?, ?> getIndexManager(Scheme.Collection collection, Scheme.Field field) {
        return indexManagers.computeIfAbsent(getPoolId(collection, field), key -> buildIndexManager(collection, field));
    }

    @Override
    public void clearIndexManager(Scheme.Collection collection, Scheme.Field field) {
        indexManagers.remove(getPoolId(collection, field));
    }

    public void registerTypeToImmutableBinaryObjectWrapper(String type, IndexBinaryObjectFactory<?> wrapper) {
        this.typeToImmutableBinaryObjectWrappers.put(type, wrapper);
    }
}
