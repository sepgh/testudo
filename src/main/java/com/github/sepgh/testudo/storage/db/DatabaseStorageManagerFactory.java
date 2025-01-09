package com.github.sepgh.testudo.storage.db;

import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.storage.pool.FileHandlerPoolFactory;

public abstract class DatabaseStorageManagerFactory {
    protected final EngineConfig engineConfig;

    protected DatabaseStorageManagerFactory(EngineConfig engineConfig) {
        this.engineConfig = engineConfig;
    }

    public abstract DatabaseStorageManager getInstance();

    public static class DiskPageDatabaseStorageManagerFactory extends DatabaseStorageManagerFactory {
        private final FileHandlerPoolFactory fileHandlerPoolFactory;
        private DatabaseStorageManager databaseStorageManager;

        public DiskPageDatabaseStorageManagerFactory(EngineConfig engineConfig, FileHandlerPoolFactory fileHandlerPoolFactory) {
            super(engineConfig);
            this.fileHandlerPoolFactory = fileHandlerPoolFactory;
        }

        @Override
        public synchronized DatabaseStorageManager getInstance() {
            if (databaseStorageManager != null)
                return databaseStorageManager;

            if (engineConfig.getRemovedObjectTrackingStrategy().equals(EngineConfig.RemovedObjectTrackingStrategy.IN_MEMORY)) {
                this.databaseStorageManager = new DiskPageDatabaseStorageManager(
                        engineConfig,
                        fileHandlerPoolFactory.getInstance(),
                        new RemovedObjectsTracer.InMemoryRemovedObjectsTracer(engineConfig.getIMROTMinLengthToSplit())
                );
            } else {
                // Note: no difference than above line for now, does the same thing! Todo: update if new implementations of Removed Object Tracker has been added
                this.databaseStorageManager = new DiskPageDatabaseStorageManager(
                        engineConfig,
                        fileHandlerPoolFactory.getInstance()
                );
            }


            return this.databaseStorageManager;
        }
    }
}
