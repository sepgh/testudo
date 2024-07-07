package com.github.sepgh.testudo.storage;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.storage.header.HeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandlerPool;
import com.github.sepgh.testudo.storage.pool.ManagedFileHandler;
import com.github.sepgh.testudo.utils.FileUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

public class SingleFileIndexStorageManager extends BaseFileIndexStorageManager {

    public SingleFileIndexStorageManager(Path path, @Nullable String customName, HeaderManager headerManager, EngineConfig engineConfig, FileHandlerPool fileHandlerPool) throws IOException, ExecutionException, InterruptedException {
        super(path, customName, headerManager, engineConfig, fileHandlerPool);
    }

    public SingleFileIndexStorageManager(Path path, @Nullable String customName, HeaderManager headerManager, EngineConfig engineConfig, FileHandlerPool fileHandlerPool, int binarySpace) throws IOException, ExecutionException, InterruptedException {
        super(path, customName, headerManager, engineConfig, fileHandlerPool, binarySpace);
    }

    public SingleFileIndexStorageManager(Path path, HeaderManager headerManager, EngineConfig engineConfig, FileHandlerPool fileHandlerPool, int binarySpaceMax) throws IOException, ExecutionException, InterruptedException {
        super(path, headerManager, engineConfig, fileHandlerPool, binarySpaceMax);
    }

    public SingleFileIndexStorageManager(Path path, HeaderManager headerManager, EngineConfig engineConfig, int binarySpaceMax) throws IOException, ExecutionException, InterruptedException {
        super(path, headerManager, engineConfig, binarySpaceMax);
    }

    protected Path getIndexFilePath(int table, int chunk) {
        if (customName == null)
            return Path.of(path.toString(), String.format("%s.bin", INDEX_FILE_NAME));
        return Path.of(path.toString(), String.format("%s.%s.bin", INDEX_FILE_NAME, customName));
    }

    protected Pointer getAllocatedSpaceForNewNode(int tableId, int chunk) throws IOException, ExecutionException, InterruptedException {
        ManagedFileHandler managedFileHandler = this.getManagedFileHandler(tableId, 0);
        AsynchronousFileChannel asynchronousFileChannel = managedFileHandler.getAsynchronousFileChannel();

        Long position = FileUtils.allocate(asynchronousFileChannel, this.getIndexGrowthAllocationSize()).get();
        managedFileHandler.close();
        return new Pointer(Pointer.TYPE_NODE, position, chunk);
    }

}
