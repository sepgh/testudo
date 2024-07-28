package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.storage.index.IndexStorageManagerFactory;

public abstract class FieldIndexManagerProvider {
    protected final EngineConfig engineConfig;
    protected final IndexStorageManagerFactory indexStorageManagerFactory;

    public FieldIndexManagerProvider(EngineConfig engineConfig, IndexStorageManagerFactory indexStorageManagerFactory) {
        this.engineConfig = engineConfig;
        this.indexStorageManagerFactory = indexStorageManagerFactory;
    }

    public abstract IndexManager<?, ?> getIndexManager(Scheme.Collection collection, Scheme.Field field);
    public abstract void clearIndexManager(Scheme.Collection collection, Scheme.Field field);
}
