package com.github.sepgh.testudo.storage.pool;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class LimitedFileHandlerPool implements FileHandlerPool {
    private final Map<String, FileHandler> fileHandlers;
    private final Semaphore semaphore;
    private final FileHandlerFactory fileHandlerFactory;

    public LimitedFileHandlerPool(FileHandlerFactory fileHandlerFactory, int maxFiles) {
        this.fileHandlerFactory = fileHandlerFactory;
        fileHandlers = new ConcurrentHashMap<>();
        semaphore = new Semaphore(maxFiles);
    }

    public FileHandler getFileHandler(String filePath){
        return this.fileHandlers.get(filePath);
    }

    public AsynchronousFileChannel getFileChannel(String filePath, long timeout, TimeUnit timeUnit) throws InterruptedException, IOException {

        FileHandler fileHandler;

        synchronized (fileHandlers) {
            fileHandler = fileHandlers.get(filePath);
            if (fileHandler == null) {
                if (!semaphore.tryAcquire(timeout, timeUnit)) {
                    throw new IllegalStateException("Timeout while waiting to acquire file handler for " + filePath);
                }
                fileHandler = fileHandlerFactory.getFileHandler(filePath);
                fileHandlers.put(filePath, fileHandler);
            }
            fileHandler.incrementUsage();
        }

        return fileHandler.getFileChannel();
    }

    @Override
    public synchronized void releaseFileChannel(String filePath, long timeout, TimeUnit timeUnit) {
        FileHandler fileHandler = fileHandlers.get(filePath);
        if (fileHandler != null) {
            fileHandler.decrementUsage();
            if (fileHandler.getUsageCount() <= 0) {
                try {
                    semaphore.release(); // Release the permit
                    fileHandlers.remove(filePath);
                    fileHandler.close(timeout, timeUnit);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
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