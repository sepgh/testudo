package com.github.sepgh.testudo.storage.db;

import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.storage.pool.FileHandlerPoolFactory;

public abstract class DatabaseStorageManagerFactory {
    protected final EngineConfig engineConfig;

    protected DatabaseStorageManagerFactory(EngineConfig engineConfig) {
        this.engineConfig = engineConfig;
    }

    public abstract DatabaseStorageManager create();

    public static class DiskPageDatabaseStorageManagerFactory extends DatabaseStorageManagerFactory {
        private final FileHandlerPoolFactory fileHandlerPoolFactory;
        private DatabaseStorageManager databaseStorageManager;

        public DiskPageDatabaseStorageManagerFactory(EngineConfig engineConfig, FileHandlerPoolFactory fileHandlerPoolFactory) {
            super(engineConfig);
            this.fileHandlerPoolFactory = fileHandlerPoolFactory;
        }

        @Override
        public synchronized DatabaseStorageManager create() {
            if (databaseStorageManager != null)
                return databaseStorageManager;

            if (engineConfig.getRemovedObjectTrackingStrategy().equals(EngineConfig.RemovedObjectTrackingStrategy.IN_MEMORY)) {
                this.databaseStorageManager = new DiskPageDatabaseStorageManager(
                        engineConfig,
                        fileHandlerPoolFactory.create(),
                        new RemovedObjectsTracer.InMemoryRemovedObjectsTracer(engineConfig.getIMROTMinLengthToSplit())
                );
            } else {
                // Note: no difference than above line for now, does the same thing! Todo: update if new implementations of Removed Object Tracker has been added
                this.databaseStorageManager = new DiskPageDatabaseStorageManager(
                        engineConfig,
                        fileHandlerPoolFactory.create()
                );
            }


            return this.databaseStorageManager;
        }
    }
}
