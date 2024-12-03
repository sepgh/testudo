package com.github.sepgh.testudo.storage.index;

import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManagerFactory;

public abstract class IndexStorageManagerFactory {
    protected final EngineConfig engineConfig;
    protected final IndexHeaderManagerFactory indexHeaderManagerFactory;

    protected IndexStorageManagerFactory(EngineConfig engineConfig, IndexHeaderManagerFactory indexHeaderManagerFactory) {
        this.engineConfig = engineConfig;
        this.indexHeaderManagerFactory = indexHeaderManagerFactory;
    }

    public abstract IndexStorageManager create(Scheme scheme, Scheme.Collection collection);
}
