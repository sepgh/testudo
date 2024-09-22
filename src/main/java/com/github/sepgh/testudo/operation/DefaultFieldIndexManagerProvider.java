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
import com.github.sepgh.testudo.storage.index.IndexStorageManagerFactory;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DefaultFieldIndexManagerProvider extends FieldIndexManagerProvider {
    private final Map<String, UniqueTreeIndexManager<?, ?>> uniqueIndexManagers = new HashMap<>();
    private final Map<String, DuplicateIndexManager<?, ?>> duplicateIndexManagers = new HashMap<>();

    public DefaultFieldIndexManagerProvider(EngineConfig engineConfig, IndexStorageManagerFactory indexStorageManagerFactory) {
        super(engineConfig, indexStorageManagerFactory);
    }

    protected String getPoolId(Scheme.Collection collection, Scheme.Field field){
        return "%d_%d".formatted(collection.getId(), field.getId());
    }

    // Todo: the current implementation ignores that the cluster index is not the primary field index
    protected UniqueTreeIndexManager<?, ?> buildUniqueIndexManager(Scheme.Collection collection, Scheme.Field field) {
        // Raw use of field.id as indexId would force the scheme designer to use unique field ids per whole DB
        // However, using hash code of pool id (which is combination of collection.id and field.id) forces the scheme designer
        //          to only use unique field ids per collection
        int indexId = getPoolId(collection, field).hashCode();

        Serializer<?> serializer = SerializerRegistry.getInstance().getSerializer(field.getType());

        UniqueTreeIndexManager<?, ?> uniqueTreeIndexManager;
        if (field.isPrimary()){
            uniqueTreeIndexManager = new ClusterBPlusTreeUniqueTreeIndexManager<>(
                    indexId,
                    engineConfig.getBTreeDegree(),
                    indexStorageManagerFactory.create(collection, field),
                    serializer.getIndexBinaryObjectFactory(field)
            );
        } else {
            Optional<Scheme.Field> optionalField = collection.getPrimaryField();

            if (optionalField.isEmpty()){
                // Todo: err  (unless above todo is over)
            }

            Scheme.Field primaryField = optionalField.get();
            Serializer<?> primarySerializer = SerializerRegistry.getInstance().getSerializer(primaryField.getType());

            uniqueTreeIndexManager = new BPlusTreeUniqueTreeIndexManager<>(
                    indexId,
                    engineConfig.getBTreeDegree(),
                    indexStorageManagerFactory.create(collection, field),
                    serializer.getIndexBinaryObjectFactory(field),
                    primarySerializer.getIndexBinaryObjectFactory(primaryField)
            );
        }

        return uniqueTreeIndexManager;
    }

    // Todo: the current implementation ignores that the cluster index is not the primary field index
    protected <K extends Comparable<K>, V extends Number & Comparable<V>> DuplicateIndexManager<K, V> buildDuplicateIndexManager(Scheme.Collection collection, Scheme.Field field) {
        int indexId = getPoolId(collection, field).hashCode();

        Optional<Scheme.Field> optionalField = collection.getPrimaryField();
        if (optionalField.isEmpty()){
            // Todo: err  (unless above todo is over, then we dont need primary field)
        }


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

        Scheme.Field primaryField = optionalField.get();
        Serializer<?> primarySerializer = SerializerRegistry.getInstance().getSerializer(primaryField.getType());

        DuplicateIndexManager<K, V> duplicateIndexManager = new DuplicateBPlusTreeIndexManagerBridge<>(
                collection.getId(),
                engineConfig,
                (UniqueTreeIndexManager<K, Pointer>) uniqueTreeIndexManager,
                (IndexBinaryObjectFactory<V>) primarySerializer.getIndexBinaryObjectFactory(primaryField),
                null
        );
        return duplicateIndexManager;
    }

    @Override
    public synchronized UniqueTreeIndexManager<?, ?> getUniqueIndexManager(Scheme.Collection collection, Scheme.Field field) {
        return uniqueIndexManagers.computeIfAbsent(getPoolId(collection, field), key -> buildUniqueIndexManager(collection, field));
    }

    @Override
    public synchronized DuplicateIndexManager<?, ?> getDuplicateIndexManager(Scheme.Collection collection, Scheme.Field field) {
        Preconditions.checkNotNull(collection);
        Preconditions.checkNotNull(field);
        Preconditions.checkArgument(field.isIndex(), "Index Manager can only be requested for indexed fields");
        Preconditions.checkArgument(!field.isIndexUnique(), "Duplicate index manager can only be created for fields with index set as not unique");

        return duplicateIndexManagers.computeIfAbsent(getPoolId(collection, field), key -> buildDuplicateIndexManager(collection, field));
    }

    @Override
    public void clearIndexManager(Scheme.Collection collection, Scheme.Field field) {
        uniqueIndexManagers.remove(getPoolId(collection, field));
    }

}
