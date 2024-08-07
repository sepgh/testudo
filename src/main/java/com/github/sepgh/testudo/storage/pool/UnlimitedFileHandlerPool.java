package com.github.sepgh.testudo.storage.pool;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class UnlimitedFileHandlerPool implements FileHandlerPool {
    private final Map<String, FileHandler> fileHandlers;
    private final FileHandlerFactory fileHandlerFactory;

    public UnlimitedFileHandlerPool(FileHandlerFactory fileHandlerFactory) {
        this.fileHandlerFactory = fileHandlerFactory;
        fileHandlers = new ConcurrentHashMap<>();
    }

    public AsynchronousFileChannel getFileChannel(String filePath, long timeout, TimeUnit timeUnit) throws InterruptedException {
        FileHandler fileHandler = fileHandlers.computeIfAbsent(filePath, filePath1 -> {
            try {
                return fileHandlerFactory.getFileHandler(filePath1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        fileHandler.incrementUsage();
        return fileHandler.getFileChannel();
    }

    public void releaseFileChannel(String filePath, long timeout, TimeUnit timeUnit) {
        FileHandler fileHandler = fileHandlers.get(filePath);
        if (fileHandler != null) {
            fileHandler.decrementUsage();
        }
    }

    @Override
    public void closeAll(long timeout, TimeUnit timeUnit) {
        fileHandlers.forEach((s, fileHandler) -> {
            try {
                fileHandler.close(timeout, timeUnit);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
