package com.github.sepgh.testudo.storage.pool;

import com.github.sepgh.testudo.context.EngineConfig;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public abstract class FileHandlerPoolFactory {
    protected final EngineConfig engineConfig;

    public abstract FileHandlerPool getInstance();

    public static class DefaultFileHandlerPoolFactory extends FileHandlerPoolFactory {
        private FileHandlerPool fileHandlerPool;

        public DefaultFileHandlerPoolFactory(EngineConfig engineConfig) {
            super(engineConfig);
        }

        @Override
        public synchronized FileHandlerPool getInstance() {
            if (fileHandlerPool != null){
                return fileHandlerPool;
            }

            if (engineConfig.getFileHandlerStrategy().equals(EngineConfig.FileHandlerStrategy.UNLIMITED)){
                fileHandlerPool = new UnlimitedFileHandlerPool(
                        FileHandler.SingletonFileHandlerFactory.getInstance(
                                engineConfig.getFileHandlerPoolThreads()
                        )
                );
            } else {
                fileHandlerPool = new LimitedFileHandlerPool(
                        FileHandler.SingletonFileHandlerFactory.getInstance(
                                engineConfig.getFileHandlerPoolThreads()
                        ),
                        engineConfig.getFileHandlerPoolMaxFiles()
                );
            }

            return fileHandlerPool;
        }
    }
}
