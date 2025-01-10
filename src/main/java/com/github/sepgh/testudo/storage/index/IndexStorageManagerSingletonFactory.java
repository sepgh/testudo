package com.github.sepgh.testudo.storage.index;

import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManagerSingletonFactory;

public abstract class IndexStorageManagerSingletonFactory {
    protected final EngineConfig engineConfig;
    protected final IndexHeaderManagerSingletonFactory indexHeaderManagerSingletonFactory;

    protected IndexStorageManagerSingletonFactory(EngineConfig engineConfig, IndexHeaderManagerSingletonFactory indexHeaderManagerSingletonFactory) {
        this.engineConfig = engineConfig;
        this.indexHeaderManagerSingletonFactory = indexHeaderManagerSingletonFactory;
    }

    public abstract IndexStorageManager create(Scheme scheme, Scheme.Collection collection);
}
