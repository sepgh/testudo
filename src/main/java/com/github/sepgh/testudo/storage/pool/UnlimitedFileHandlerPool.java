package com.github.sepgh.testudo.storage.pool;

import com.github.sepgh.testudo.exception.InternalOperationException;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class UnlimitedFileHandlerPool implements FileHandlerPool {
    private final Map<String, FileHandler> fileHandlers;
    private final FileHandlerFactory fileHandlerFactory;

    public UnlimitedFileHandlerPool(FileHandlerFactory fileHandlerFactory) {
        this.fileHandlerFactory = fileHandlerFactory;
        fileHandlers = new ConcurrentHashMap<>();
    }

    public AsynchronousFileChannel getFileChannel(String filePath, long timeout, TimeUnit timeUnit) throws InternalOperationException {
        AtomicReference<InternalOperationException> exception = new AtomicReference<>();

        FileHandler fileHandler = fileHandlers.computeIfAbsent(filePath, filePath1 -> {
            try {
                return fileHandlerFactory.getFileHandler(filePath1);
            } catch (IOException e) {
                exception.set(new InternalOperationException(e));
                return null;
            }
        });

        if (exception.get() != null) {
            throw exception.get();
        }

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

    @Override
    public void closeAll() throws InternalOperationException {
        AtomicReference<InternalOperationException> exception = new AtomicReference<>();
        fileHandlers.forEach((s, fileHandler) -> {
            try {
                fileHandler.close();
            } catch (InternalOperationException e) {
                exception.set(e);
            }
        });

        if (exception.get() != null) {
            throw exception.get();
        }
    }
}
