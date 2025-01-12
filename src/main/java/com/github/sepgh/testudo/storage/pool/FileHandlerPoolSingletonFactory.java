package com.github.sepgh.testudo.storage.pool;

import com.github.sepgh.testudo.context.EngineConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class FileHandlerPoolSingletonFactory {
    protected final EngineConfig engineConfig;
    private FileHandlerPool fileHandlerPool;
    protected final ExecutorService executorService;

    protected FileHandlerPoolSingletonFactory(EngineConfig engineConfig, ExecutorService executorService) {
        this.engineConfig = engineConfig;
        this.executorService = executorService;
    }

    public synchronized final FileHandlerPool getInstance() {
        if (fileHandlerPool == null) {
            fileHandlerPool = this.create();
        }
        return fileHandlerPool;
    }

    protected abstract FileHandlerPool create();

    public static class DefaultFileHandlerPoolSingletonFactory extends FileHandlerPoolSingletonFactory {

        public DefaultFileHandlerPoolSingletonFactory(EngineConfig engineConfig, ExecutorService executorService) {
            super(engineConfig, executorService);
        }

        public DefaultFileHandlerPoolSingletonFactory(EngineConfig engineConfig) {
            super(engineConfig, Executors.newFixedThreadPool(engineConfig.getFileHandlerPoolThreads()));
        }

        @Override
        public FileHandlerPool create() {

            if (engineConfig.getFileHandlerStrategy().equals(EngineConfig.FileHandlerStrategy.UNLIMITED)){
                return new UnlimitedFileHandlerPool(
                        FileHandler.SingletonFileHandlerFactory.getInstance(executorService)
                );
            } else {
                return new LimitedFileHandlerPool(
                        FileHandler.SingletonFileHandlerFactory.getInstance(executorService),
                        engineConfig.getFileHandlerPoolMaxFiles()
                );
            }

        }

    }
}
