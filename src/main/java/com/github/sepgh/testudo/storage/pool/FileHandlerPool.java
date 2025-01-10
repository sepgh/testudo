package com.github.sepgh.testudo.storage.pool;

import com.github.sepgh.testudo.exception.InternalOperationException;

import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public interface FileHandlerPool {
    default AsynchronousFileChannel getFileChannel(Path filePath, long timeout, TimeUnit timeUnit) throws InternalOperationException {
        return this.getFileChannel(filePath.toString(), timeout, timeUnit);
    }
    AsynchronousFileChannel getFileChannel(String filePath, long timeout, TimeUnit timeUnit) throws InternalOperationException;
    default void releaseFileChannel(Path path, long timeout, TimeUnit timeUnit) throws InternalOperationException {
        this.releaseFileChannel(path.toString(), timeout, timeUnit);
    }
    void releaseFileChannel(String path, long timeout, TimeUnit timeUnit) throws InternalOperationException;

    void closeAll(long timeout, TimeUnit timeUnit) throws InternalOperationException;
}
