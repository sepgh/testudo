package com.github.sepgh.internal.storage.pool;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class LimitedFileHandlerPool implements FileHandlerPool {
    private final Map<String, FileHandler> fileHandlers;
    private final Semaphore semaphore;

    public LimitedFileHandlerPool(int maxFiles) {
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
                    System.out.println(this.getFileHandler(filePath.replace("index.1", "index.0")).getUsageCount());
                    throw new IllegalStateException("Timeout while waiting to acquire file handler for " + filePath);
                }
                fileHandler = new FileHandler(filePath);
                fileHandlers.put(filePath, fileHandler);
            }
            fileHandler.incrementUsage();
        }

        return fileHandler.getFileChannel();
    }

    public void releaseFileChannel(String filePath) {
        FileHandler fileHandler = fileHandlers.get(filePath);
        if (fileHandler != null) {
            fileHandler.decrementUsage();
            if (fileHandler.getUsageCount().get() <= 0) {
                try {
                    semaphore.release(); // Release the permit
                    fileHandlers.remove(filePath);
                    fileHandler.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void closeAll() {
        fileHandlers.forEach((s, fileHandler) -> {
            try {
                fileHandler.getFileChannel().close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

}