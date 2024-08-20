package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.index.tree.BPlusTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.data.*;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.serialization.FieldType;
import com.github.sepgh.testudo.serialization.Serializer;
import com.github.sepgh.testudo.serialization.SerializerRegistry;
import com.github.sepgh.testudo.storage.index.IndexStorageManagerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DefaultFieldIndexManagerProvider extends FieldIndexManagerProvider {
    private final Map<String, IndexManager<?, ?>> indexManagers = new HashMap<>();

    public DefaultFieldIndexManagerProvider(EngineConfig engineConfig, IndexStorageManagerFactory indexStorageManagerFactory) {
        super(engineConfig, indexStorageManagerFactory);
    }

    private String getPoolId(Scheme.Collection collection, Scheme.Field field){
        return "%d_%d".formatted(collection.getId(), field.getId());
    }

    private IndexManager<?, ?> buildIndexManager(Scheme.Collection collection, Scheme.Field field) {
        // Raw use of field.id as indexId would force the scheme designer to use unique field ids per whole DB
        // However, using hash code of pool id (which is combination of collection.id and field.id) forces the scheme designer
        //          to only use unique field ids per collection
        int indexId = getPoolId(collection, field).hashCode();

        Serializer<?> serializer = SerializerRegistry.getInstance().getSerializer(field.getType());

        IndexManager<?, ?> indexManager;
        if (field.isPrimary()){
            indexManager = new ClusterBPlusTreeIndexManager<>(
                    indexId,
                    engineConfig.getBTreeDegree(),
                    indexStorageManagerFactory.create(collection, field),
                    serializer.getIndexBinaryObjectFactory(field)
            );
        } else {
            Optional<Scheme.Field> optionalField = collection.getPrimaryField();

            if (optionalField.isEmpty()){
                // Todo: err
            }

            Scheme.Field primaryField = optionalField.get();
            Serializer<?> primarySerializer = SerializerRegistry.getInstance().getSerializer(primaryField.getType());

            indexManager = new BPlusTreeIndexManager<>(
                    indexId,
                    engineConfig.getBTreeDegree(),
                    indexStorageManagerFactory.create(collection, field),
                    serializer.getIndexBinaryObjectFactory(field),
                    primarySerializer.getIndexBinaryObjectFactory(primaryField)
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

}
