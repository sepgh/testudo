package com.github.sepgh.testudo.storage.index;

import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManagerFactory;
import com.github.sepgh.testudo.storage.pool.FileHandlerPoolFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultIndexStorageManagerFactory extends IndexStorageManagerFactory {
    private final Map<String, IndexStorageManager> storageManagers = new ConcurrentHashMap<>();
    private final FileHandlerPoolFactory fileHandlerPoolFactory;
    private final DatabaseStorageManagerFactory databaseStorageManagerFactory;

    public DefaultIndexStorageManagerFactory(EngineConfig engineConfig, IndexHeaderManagerFactory indexHeaderManagerFactory, FileHandlerPoolFactory fileHandlerPoolFactory, DatabaseStorageManagerFactory databaseStorageManagerFactory) {
        super(engineConfig, indexHeaderManagerFactory);
        this.fileHandlerPoolFactory = fileHandlerPoolFactory;
        this.databaseStorageManagerFactory = databaseStorageManagerFactory;
    }

    @Override
    public IndexStorageManager create(Scheme scheme, Scheme.Collection collection) {
        // If only a single index storage manager should be created, reuse if any is already created
        synchronized (this) {
            if (!engineConfig.isSplitIndexPerCollection()) {
                Set<String> keySet = this.storageManagers.keySet();
                if (!keySet.isEmpty()) {
                    return this.storageManagers.get(keySet.iterator().next());
                }
            }
        }

        return this.storageManagers.computeIfAbsent(scheme.getDbName(), key -> {
            String customName = null;
            if (engineConfig.isSplitIndexPerCollection()){
                customName = "col_" + collection.getId();
            }

            EngineConfig.IndexStorageManagerStrategy indexStorageManagerStrategy = engineConfig.getIndexStorageManagerStrategy();
            if (indexStorageManagerStrategy.equals(EngineConfig.IndexStorageManagerStrategy.ORGANIZED)) {
                return new OrganizedFileIndexStorageManager(customName, indexHeaderManagerFactory, engineConfig, fileHandlerPoolFactory.getInstance());
            } else if (indexStorageManagerStrategy.equals(EngineConfig.IndexStorageManagerStrategy.PAGE_BUFFER)) {
                return new DiskPageFileIndexStorageManager(engineConfig, indexHeaderManagerFactory, fileHandlerPoolFactory.getInstance(), databaseStorageManagerFactory.getInstance());
            } else {
                return new CompactFileIndexStorageManager(indexHeaderManagerFactory, engineConfig, fileHandlerPoolFactory.getInstance());
            }

        });
    }
}
