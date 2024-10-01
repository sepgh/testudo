package com.github.sepgh.testudo.storage.index;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManagerFactory;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.FileHandlerPool;
import com.github.sepgh.testudo.storage.pool.LimitedFileHandlerPool;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DefaultIndexStorageManagerFactory extends IndexStorageManagerFactory {
    private final Map<String, IndexStorageManager> storageManagers = new HashMap<>();
    private FileHandlerPool fileHandlerPool;

    public DefaultIndexStorageManagerFactory(EngineConfig engineConfig, IndexHeaderManagerFactory indexHeaderManagerFactory) {
        super(engineConfig, indexHeaderManagerFactory);
    }

    private synchronized FileHandlerPool getFileHandlerPool() {
        if (fileHandlerPool != null)
            return fileHandlerPool;

        if (engineConfig.getFileHandlerStrategy().equals(EngineConfig.FileHandlerStrategy.UNLIMITED)){
            fileHandlerPool = new UnlimitedFileHandlerPool(
                    FileHandler.SingletonFileHandlerFactory.getInstance(
                            this.engineConfig.getFileHandlerPoolThreads()
                    )
            );
        } else {
            fileHandlerPool = new LimitedFileHandlerPool(
                    FileHandler.SingletonFileHandlerFactory.getInstance(
                            this.engineConfig.getFileHandlerPoolThreads()
                    ),
                    this.engineConfig.getFileHandlerPoolMaxFiles()
            );
        }

        return fileHandlerPool;
    }


    @Override
    public IndexStorageManager create(Scheme.Collection collection, Scheme.Field field) {
        // If only a single index storage manager should be created, reuse if any is already created
        if (!engineConfig.isSplitIndexPerCollection()) {
            Set<String> keySet = this.storageManagers.keySet();
            if (!keySet.isEmpty()) {
                return this.storageManagers.get(keySet.iterator().next());
            }
        }

        return this.storageManagers.computeIfAbsent("%d_%d".formatted(collection.getId(), field.getId()), key -> {
            String customName = null;
            if (engineConfig.isSplitIndexPerCollection()){
                customName = "col_" + collection.getId();
            }

            EngineConfig.IndexStorageManagerStrategy indexStorageManagerStrategy = engineConfig.getIndexStorageManagerStrategy();
            if (indexStorageManagerStrategy.equals(EngineConfig.IndexStorageManagerStrategy.ORGANIZED)) {
                return new OrganizedFileIndexStorageManager(customName, indexHeaderManagerFactory, engineConfig, getFileHandlerPool());
            } else {
                return new CompactFileIndexStorageManager(indexHeaderManagerFactory, engineConfig, getFileHandlerPool());
            }

        });
    }
}
