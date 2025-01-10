package com.github.sepgh.testudo.context;

import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.operation.*;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManagerSingletonFactory;
import com.github.sepgh.testudo.storage.index.DefaultIndexStorageManagerSingletonFactory;
import com.github.sepgh.testudo.storage.index.IndexStorageManagerSingletonFactory;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManagerSingletonFactory;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandlerPoolSingletonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public abstract class DatabaseContextConfigurator {
    private FileHandlerPoolSingletonFactory fileHandlerPoolSingletonFactory;
    private IndexHeaderManagerSingletonFactory indexHeaderManagerSingletonFactory;
    private EngineConfig engineConfig;
    private IndexStorageManagerSingletonFactory indexStorageManagerSingletonFactory;
    private DatabaseStorageManagerSingletonFactory databaseStorageManagerSingletonFactory;
    private CollectionIndexProviderSingletonFactory collectionIndexProviderSingletonFactory;
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

    protected IndexHeaderManagerSingletonFactory indexHeaderManagerFactory() {
        return new JsonIndexHeaderManager.SingletonFactory();
    }

    public IndexHeaderManagerSingletonFactory getIndexHeaderManagerFactory() {
        if (indexHeaderManagerSingletonFactory == null) {
            indexHeaderManagerSingletonFactory = indexHeaderManagerFactory();
        }
        return indexHeaderManagerSingletonFactory;
    }

    protected FileHandlerPoolSingletonFactory fileHandlerPoolFactory() {
        return new FileHandlerPoolSingletonFactory.DefaultFileHandlerPoolSingletonFactory(getEngineConfig());
    }

    public FileHandlerPoolSingletonFactory getFileHandlerPoolFactory() {
        if (fileHandlerPoolSingletonFactory == null) {
            this.fileHandlerPoolSingletonFactory = fileHandlerPoolFactory();
        }
        return fileHandlerPoolSingletonFactory;
    }

    protected IndexStorageManagerSingletonFactory indexStorageManagerFactory() {
        return new DefaultIndexStorageManagerSingletonFactory(
                getEngineConfig(),
                getIndexHeaderManagerFactory(),
                getFileHandlerPoolFactory(),
                getDatabaseStorageManagerFactory()
        );
    }

    public IndexStorageManagerSingletonFactory getIndexStorageManagerFactory() {
        if (indexStorageManagerSingletonFactory == null) {
            this.indexStorageManagerSingletonFactory = indexStorageManagerFactory();
        }
        return indexStorageManagerSingletonFactory;
    }

    protected DatabaseStorageManagerSingletonFactory databaseStorageManagerFactory() {
        return new DatabaseStorageManagerSingletonFactory.DiskPageDatabaseStorageManagerSingletonFactory(getEngineConfig(), getFileHandlerPoolFactory());
    }

    public DatabaseStorageManagerSingletonFactory getDatabaseStorageManagerFactory() {
        if (databaseStorageManagerSingletonFactory == null) {
            this.databaseStorageManagerSingletonFactory = databaseStorageManagerFactory();
        }
        return databaseStorageManagerSingletonFactory;
    }

    protected CollectionIndexProviderSingletonFactory collectionIndexProviderFactory() {
        return new DefaultCollectionIndexProviderSingletonFactory(this.getScheme(), this.getEngineConfig(), this.getIndexStorageManagerFactory(), this.getDatabaseStorageManagerFactory().getInstance());
    }

    public CollectionIndexProviderSingletonFactory getCollectionIndexProviderFactory() {
        if (this.collectionIndexProviderSingletonFactory == null) {
            this.collectionIndexProviderSingletonFactory = collectionIndexProviderFactory();
        }
        return collectionIndexProviderSingletonFactory;
    }

    protected CollectionOperationFactory collectionOperationFactory(Scheme.Collection collection) {
        return new CollectionOperationFactory(getScheme(), collection, getCollectionIndexProviderFactory(), getDatabaseStorageManagerFactory());
    }

    public DatabaseContext databaseContext() {
        if (databaseContext == null)
            this.databaseContext = new DefaultDatabaseContext(getScheme());
        return this.databaseContext;
    }

    public class DefaultDatabaseContext implements DatabaseContext {

        private final Scheme scheme;
        private static final Logger logger = LoggerFactory.getLogger(DefaultDatabaseContext.class);


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
            try {
                getFileHandlerPoolFactory().getInstance().closeAll(1, TimeUnit.DAYS);  // Todo
            } catch (InternalOperationException e) {
                logger.error("failed to close file handler instance", e);
            }
            getDatabaseStorageManagerFactory().getInstance().close();
        }
    }

}
