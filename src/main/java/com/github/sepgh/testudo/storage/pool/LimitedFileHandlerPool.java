package com.github.sepgh.testudo.storage.pool;

import com.github.sepgh.testudo.exception.InternalOperationException;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.sepgh.testudo.exception.ErrorMessage.EM_FILEHANDLER_CREATE;

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

    public AsynchronousFileChannel getFileChannel(String filePath, long timeout, TimeUnit timeUnit) throws InternalOperationException {

        FileHandler fileHandler;

        synchronized (fileHandlers) {
            fileHandler = fileHandlers.get(filePath);
            if (fileHandler == null) {
                try {
                    if (!semaphore.tryAcquire(timeout, timeUnit)) {
                        throw new IllegalStateException("Timeout while waiting to acquire file handler for " + filePath);
                    }
                    fileHandler = fileHandlerFactory.getFileHandler(filePath);
                    fileHandlers.put(filePath, fileHandler);
                } catch (IOException | InterruptedException e) {
                    throw new InternalOperationException(EM_FILEHANDLER_CREATE, e);
                }
            }
            fileHandler.incrementUsage();
        }

        return fileHandler.getFileChannel();
    }

    @Override
    public synchronized void releaseFileChannel(String filePath, long timeout, TimeUnit timeUnit) throws InternalOperationException {
        FileHandler fileHandler = fileHandlers.get(filePath);
        if (fileHandler != null) {
            fileHandler.decrementUsage();
            if (fileHandler.getUsageCount() <= 0) {
                semaphore.release(); // Release the permit
                fileHandlers.remove(filePath);
                fileHandler.close(timeout, timeUnit);
            }
        }
    }

    @Override
    public void closeAll(long timeout, TimeUnit timeUnit) throws InternalOperationException {
        AtomicReference<InternalOperationException> exception = new AtomicReference<>();
        fileHandlers.forEach((s, fileHandler) -> {
            try {
                fileHandler.close(timeout, timeUnit);
            } catch (InternalOperationException e) {
                exception.set(e);
            }
        });

        if (exception.get() != null) {
            throw exception.get();
        }
    }

}