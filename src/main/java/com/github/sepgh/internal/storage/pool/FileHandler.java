package com.github.sepgh.internal.storage.pool;

import lombok.Getter;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
public class FileHandler {
    private final AsynchronousFileChannel fileChannel;
    private final AtomicInteger usageCount = new AtomicInteger(0);

    public FileHandler(String filePath) throws IOException {
        // Todo: executor service could be passed here
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
    }
}
