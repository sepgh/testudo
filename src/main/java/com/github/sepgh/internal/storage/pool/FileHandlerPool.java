package com.github.sepgh.internal.storage.pool;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public interface FileHandlerPool {
    default AsynchronousFileChannel getFileChannel(Path filePath, long timeout, TimeUnit timeUnit) throws InterruptedException, IOException {
        return this.getFileChannel(filePath.toString(), timeout, timeUnit);
    }
    AsynchronousFileChannel getFileChannel(String filePath, long timeout, TimeUnit timeUnit) throws InterruptedException, IOException;
    default void releaseFileChannel(Path path){
        this.releaseFileChannel(path.toString());
    }
    void releaseFileChannel(String path);

    void closeAll();
}
