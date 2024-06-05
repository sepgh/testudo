package com.github.sepgh.internal.storage.pool;

import lombok.Getter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
public class FileHandler {
    private final AsynchronousFileChannel fileChannel;
    private final AtomicInteger usageCount = new AtomicInteger(0);

    public FileHandler(String filePath, ExecutorService executorService) throws IOException {
        this.fileChannel = AsynchronousFileChannel.open(Path.of(filePath),
                Set.of(
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE
                ),
                executorService
        );
    }

    public FileHandler(String filePath) throws IOException {
        this.fileChannel = AsynchronousFileChannel.open(Path.of(filePath),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        );
    }

    public void incrementUsage() {
        usageCount.incrementAndGet();
    }

    public void decrementUsage() {
        usageCount.decrementAndGet();
    }

    public void close() throws IOException {
        fileChannel.close();
        usageCount.set(0);
    }

    public static class SingletonFileHandlerFactory implements FileHandlerFactory {
        private ExecutorService executorService;

        private static SingletonFileHandlerFactory FACTORY_INSTANCE;

        public static synchronized SingletonFileHandlerFactory getInstance(){
            return getInstance(null);
        }

        public static synchronized SingletonFileHandlerFactory getInstance(int threadCount){
            return getInstance(Executors.newFixedThreadPool(threadCount));
        }

        public static synchronized SingletonFileHandlerFactory getInstance(@Nullable ExecutorService executorService){
            if (FACTORY_INSTANCE == null){
                FACTORY_INSTANCE = new SingletonFileHandlerFactory();
                if (executorService != null){
                    FACTORY_INSTANCE.executorService = executorService;
                }
            }
            return FACTORY_INSTANCE;
        }

        private SingletonFileHandlerFactory(){}

        @Override
        public synchronized FileHandler getFileHandler(String path) throws IOException {
            if (executorService == null)
                return new FileHandler(path);
            return new FileHandler(path, executorService);
        }
    }

}
