package com.github.sepgh.internal.storage.pool;

import lombok.Getter;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class ManagedFileHandler implements AutoCloseable {

    private final FileHandlerPool fileHandlerPool;
    private final Path path;
    private final long timeout;
    private final TimeUnit timeUnit;

    public ManagedFileHandler(FileHandlerPool fileHandlerPool, Path path, long timeout, TimeUnit timeUnit) {
        this.fileHandlerPool = fileHandlerPool;
        this.path = path;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
    }

    public AsynchronousFileChannel getAsynchronousFileChannel() throws InterruptedException, IOException {
        return this.fileHandlerPool.getFileChannel(path, timeout, timeUnit);
    }

    @Override
    public void close() {
        this.fileHandlerPool.releaseFileChannel(path);
    }
}
