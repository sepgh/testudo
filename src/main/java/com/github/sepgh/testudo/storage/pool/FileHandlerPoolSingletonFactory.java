package com.github.sepgh.testudo.storage.pool;

import com.github.sepgh.testudo.context.EngineConfig;

public abstract class FileHandlerPoolSingletonFactory {
    protected final EngineConfig engineConfig;
    private FileHandlerPool fileHandlerPool;

    protected FileHandlerPoolSingletonFactory(EngineConfig engineConfig) {
        this.engineConfig = engineConfig;
    }

    public synchronized final FileHandlerPool getInstance() {
        if (fileHandlerPool == null) {
            fileHandlerPool = this.create();
        }
        return fileHandlerPool;
    }

    protected abstract FileHandlerPool create();

    public static class DefaultFileHandlerPoolSingletonFactory extends FileHandlerPoolSingletonFactory {

        public DefaultFileHandlerPoolSingletonFactory(EngineConfig engineConfig) {
            super(engineConfig);
        }

        @Override
        public  FileHandlerPool create() {

            if (engineConfig.getFileHandlerStrategy().equals(EngineConfig.FileHandlerStrategy.UNLIMITED)){
                return new UnlimitedFileHandlerPool(
                        FileHandler.SingletonFileHandlerFactory.getInstance(
                                engineConfig.getFileHandlerPoolThreads()
                        )
                );
            } else {
                return new LimitedFileHandlerPool(
                        FileHandler.SingletonFileHandlerFactory.getInstance(
                                engineConfig.getFileHandlerPoolThreads()
                        ),
                        engineConfig.getFileHandlerPoolMaxFiles()
                );
            }

        }
    }
}
