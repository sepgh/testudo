package com.github.sepgh.testudo.storage;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.storage.header.IndexHeaderManager;
import com.github.sepgh.testudo.storage.header.IndexHeaderManagerFactory;
import com.github.sepgh.testudo.storage.pool.FileHandlerPool;
import com.github.sepgh.testudo.storage.pool.ManagedFileHandler;
import com.github.sepgh.testudo.utils.FileUtils;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class SingleFileIndexStorageManager extends BaseFileIndexStorageManager {

    public SingleFileIndexStorageManager(Path path, IndexHeaderManagerFactory indexHeaderManagerFactory, EngineConfig engineConfig, FileHandlerPool fileHandlerPool) {
        super(path, null, indexHeaderManagerFactory, engineConfig, fileHandlerPool);
    }

    public SingleFileIndexStorageManager(Path path, IndexHeaderManagerFactory indexHeaderManagerFactory, EngineConfig engineConfig, FileHandlerPool fileHandlerPool, int binarySpaceMax) throws IOException, ExecutionException, InterruptedException {
        super(path, indexHeaderManagerFactory, engineConfig, fileHandlerPool, binarySpaceMax);
    }

    public SingleFileIndexStorageManager(Path path, IndexHeaderManagerFactory indexHeaderManagerFactory, EngineConfig engineConfig, int binarySpaceMax) throws IOException, ExecutionException, InterruptedException {
        super(path, indexHeaderManagerFactory, engineConfig, binarySpaceMax);
    }

    protected Path getIndexFilePath(int indexId, int chunk) {
        return Path.of(path.toString(), String.format("%s.bin", INDEX_FILE_NAME));
    }

    @Override
    protected IndexHeaderManager.Location getIndexBeginningInChunk(int indexId, int chunk) throws InterruptedException {
        return new IndexHeaderManager.Location(0,0);
    }

    protected Pointer getAllocatedSpaceForNewNode(int indexId, int chunk) throws IOException, ExecutionException, InterruptedException {
        ManagedFileHandler managedFileHandler = this.getManagedFileHandler(indexId, 0);
        AsynchronousFileChannel asynchronousFileChannel = managedFileHandler.getAsynchronousFileChannel();

        synchronized (this){
            // Check if we have an empty space
            long fileSize = asynchronousFileChannel.size();
            if (fileSize > this.getIndexGrowthAllocationSize()){
                long positionToCheck = fileSize - this.getIndexGrowthAllocationSize();

                byte[] bytes = FileUtils.readBytes(asynchronousFileChannel, positionToCheck, this.getIndexGrowthAllocationSize()).get();
                Optional<Integer> optionalAdditionalPosition = getPossibleAllocationLocation(bytes);
                if (optionalAdditionalPosition.isPresent()){
                    long finalPosition = positionToCheck + optionalAdditionalPosition.get();
                    managedFileHandler.close();
                    return new Pointer(Pointer.TYPE_NODE, finalPosition, chunk);
                }

            }

            Long position = FileUtils.allocate(asynchronousFileChannel, this.getIndexGrowthAllocationSize()).get();
            managedFileHandler.close();
            return new Pointer(Pointer.TYPE_NODE, position, chunk);
        }

    }

}
