package com.github.sepgh.testudo.storage.db;

import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.storage.pool.FileHandlerPoolSingletonFactory;

public abstract class DatabaseStorageManagerSingletonFactory {
    protected final EngineConfig engineConfig;
    private DatabaseStorageManager databaseStorageManager;

    protected DatabaseStorageManagerSingletonFactory(EngineConfig engineConfig) {
        this.engineConfig = engineConfig;
    }

    public final synchronized DatabaseStorageManager getInstance() {
        if (databaseStorageManager == null) {
            databaseStorageManager = create();
        }
        return databaseStorageManager;
    }

    protected abstract DatabaseStorageManager create();

    public static class DiskPageDatabaseStorageManagerSingletonFactory extends DatabaseStorageManagerSingletonFactory {
        private final FileHandlerPoolSingletonFactory fileHandlerPoolSingletonFactory;

        public DiskPageDatabaseStorageManagerSingletonFactory(EngineConfig engineConfig, FileHandlerPoolSingletonFactory fileHandlerPoolSingletonFactory) {
            super(engineConfig);
            this.fileHandlerPoolSingletonFactory = fileHandlerPoolSingletonFactory;
        }

        @Override
        public DatabaseStorageManager create() {
            if (engineConfig.getRemovedObjectTrackingStrategy().equals(EngineConfig.RemovedObjectTrackingStrategy.IN_MEMORY)) {
                return new DiskPageDatabaseStorageManager(
                        engineConfig,
                        fileHandlerPoolSingletonFactory.getInstance(),
                        new RemovedObjectsTracer.InMemoryRemovedObjectsTracer(engineConfig.getIMROTMinLengthToSplit())
                );
            } else {
                // Note: no difference than above line for now, does the same thing! Todo: update if new implementations of Removed Object Tracker has been added
                return new DiskPageDatabaseStorageManager(
                        engineConfig,
                        fileHandlerPoolSingletonFactory.getInstance()
                );
            }

        }
    }
}
