package com.github.sepgh.testudo.context;

import com.github.sepgh.testudo.operation.*;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.DefaultIndexStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.IndexStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManagerFactory;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandlerPoolFactory;

import java.util.concurrent.TimeUnit;

public abstract class DatabaseContextConfigurator {
    private FileHandlerPoolFactory fileHandlerPoolFactory;
    private IndexHeaderManagerFactory indexHeaderManagerFactory;
    private EngineConfig engineConfig;
    private IndexStorageManagerFactory indexStorageManagerFactory;
    private DatabaseStorageManagerFactory databaseStorageManagerFactory;
    private CollectionIndexProviderFactory collectionIndexProviderFactory;
    private Scheme scheme;
    private DatabaseContext databaseContext;

    public EngineConfig engineConfig() {
        return EngineConfig.builder().build();
    }

    protected EngineConfig getEngineConfig() {
        if (engineConfig == null) {
            this.engineConfig = engineConfig();
        }
        return engineConfig;
    }

    public abstract Scheme scheme();

    protected Scheme getScheme() {
        if (scheme == null) {
            this.scheme = scheme();
        }
        return scheme;
    }

    public IndexHeaderManagerFactory indexHeaderManagerFactory() {
        return new JsonIndexHeaderManager.Factory();
    }

    protected IndexHeaderManagerFactory getIndexHeaderManagerFactory() {
        if (indexHeaderManagerFactory == null) {
            indexHeaderManagerFactory = indexHeaderManagerFactory();
        }
        return indexHeaderManagerFactory;
    }

    public FileHandlerPoolFactory fileHandlerPoolFactory() {
        return new FileHandlerPoolFactory.DefaultFileHandlerPoolFactory(getEngineConfig());
    }

    protected FileHandlerPoolFactory getFileHandlerPoolFactory() {
        if (fileHandlerPoolFactory == null) {
            this.fileHandlerPoolFactory = fileHandlerPoolFactory();
        }
        return fileHandlerPoolFactory;
    }

    public IndexStorageManagerFactory indexStorageManagerFactory() {
        return new DefaultIndexStorageManagerFactory(
                getEngineConfig(),
                getIndexHeaderManagerFactory(),
                getFileHandlerPoolFactory(),
                getDatabaseStorageManagerFactory()
        );
    }

    protected IndexStorageManagerFactory getIndexStorageManagerFactory() {
        if (indexStorageManagerFactory == null) {
            this.indexStorageManagerFactory = indexStorageManagerFactory();
        }
        return indexStorageManagerFactory;
    }

    public DatabaseStorageManagerFactory databaseStorageManagerFactory() {
        return new DatabaseStorageManagerFactory.DiskPageDatabaseStorageManagerFactory(getEngineConfig(), getFileHandlerPoolFactory());
    }

    protected DatabaseStorageManagerFactory getDatabaseStorageManagerFactory() {
        if (databaseStorageManagerFactory == null) {
            this.databaseStorageManagerFactory = databaseStorageManagerFactory();
        }
        return databaseStorageManagerFactory;
    }

    public CollectionIndexProviderFactory collectionIndexProviderFactory() {
        return new DefaultCollectionIndexProviderFactory(this.getScheme(), this.getEngineConfig(), this.getIndexStorageManagerFactory(), this.getDatabaseStorageManagerFactory().getInstance());
    }

    public CollectionIndexProviderFactory getCollectionIndexProviderFactory() {
        if (this.collectionIndexProviderFactory == null) {
            this.collectionIndexProviderFactory = collectionIndexProviderFactory();
        }
        return collectionIndexProviderFactory;
    }

    public CollectionOperationFactory collectionOperationFactory(Scheme.Collection collection) {
        return new CollectionOperationFactory(getScheme(), collection, getCollectionIndexProviderFactory(), getDatabaseStorageManagerFactory());
    }

    public DatabaseContext databaseContext() {
        if (databaseContext == null)
            this.databaseContext = new DefaultDatabaseContext(getScheme());
        return this.databaseContext;
    }

    public class DefaultDatabaseContext implements DatabaseContext {

        private final Scheme scheme;

        public DefaultDatabaseContext(Scheme scheme) {
            this.scheme = scheme;
        }

        @Override
        public Scheme getScheme() {
            return scheme;
        }

        @Override
        public CollectionOperation getOperation(Scheme.Collection collection) {
            return collectionOperationFactory(collection).create();
        }

        @Override
        public CollectionOperation getOperation(String collection) {
            return collectionOperationFactory(
                    // Todo: err instead of orElse
                    getScheme().getCollection(collection).orElse(null)
            ).create();
        }

        @Override
        public void shutdownGracefully() {
            for (Scheme.Collection collection : getScheme().getCollections()) {
                ReaderWriterLockPool.getInstance().getReaderWriterLock(getScheme(), collection).getWriteLock().lock();
            }
            getFileHandlerPoolFactory().getInstance().closeAll(1, TimeUnit.DAYS);  // Todo
            getDatabaseStorageManagerFactory().getInstance().close();
        }
    }

}
