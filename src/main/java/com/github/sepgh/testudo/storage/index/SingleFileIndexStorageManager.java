package com.github.sepgh.testudo.storage.index;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManager;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManagerFactory;
import com.github.sepgh.testudo.storage.pool.FileHandlerPool;
import com.github.sepgh.testudo.storage.pool.ManagedFileHandler;
import com.github.sepgh.testudo.utils.FileUtils;
import com.github.sepgh.testudo.utils.KVSize;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class SingleFileIndexStorageManager extends BaseFileIndexStorageManager {

    public SingleFileIndexStorageManager(IndexHeaderManagerFactory indexHeaderManagerFactory, EngineConfig engineConfig, FileHandlerPool fileHandlerPool) {
        super(null, indexHeaderManagerFactory, engineConfig, fileHandlerPool);
    }

    public SingleFileIndexStorageManager(String customName, IndexHeaderManagerFactory indexHeaderManagerFactory, EngineConfig engineConfig, FileHandlerPool fileHandlerPool) {
        super(customName, indexHeaderManagerFactory, engineConfig, fileHandlerPool);
    }

    public SingleFileIndexStorageManager(IndexHeaderManagerFactory indexHeaderManagerFactory, EngineConfig engineConfig) {
        super(indexHeaderManagerFactory, engineConfig);
    }

    public SingleFileIndexStorageManager(String customName, IndexHeaderManagerFactory indexHeaderManagerFactory, EngineConfig engineConfig) {
        super(customName, indexHeaderManagerFactory, engineConfig);
    }

    protected Path getIndexFilePath(int indexId, int chunk) {
        return Path.of(path.toString(), String.format("%s.bin", INDEX_FILE_NAME));
    }

    @Override
    protected IndexHeaderManager.Location getIndexBeginningInChunk(int indexId, int chunk) throws InterruptedException {
        return new IndexHeaderManager.Location(0,0);
    }

    protected Pointer getAllocatedSpaceForNewNode(int indexId, int chunk, KVSize kvSize) throws IOException, ExecutionException, InterruptedException {
        ManagedFileHandler managedFileHandler = this.getManagedFileHandler(indexId, 0);
        AsynchronousFileChannel asynchronousFileChannel = managedFileHandler.getAsynchronousFileChannel();

        synchronized (this){
            // Check if we have an empty space
            long fileSize = asynchronousFileChannel.size();
            if (fileSize >= this.getIndexGrowthAllocationSize(kvSize)){
                long positionToCheck = fileSize - this.getIndexGrowthAllocationSize(kvSize);

                byte[] bytes = FileUtils.readBytes(asynchronousFileChannel, positionToCheck, this.getIndexGrowthAllocationSize(kvSize)).get();
                Optional<Integer> optionalAdditionalPosition = getPossibleAllocationLocation(bytes, kvSize);
                if (optionalAdditionalPosition.isPresent()){
                    long finalPosition = positionToCheck + optionalAdditionalPosition.get();
                    managedFileHandler.close();
                    return new Pointer(Pointer.TYPE_NODE, finalPosition, chunk);
                }

            }

            Long position = FileUtils.allocate(asynchronousFileChannel, this.getIndexGrowthAllocationSize(kvSize)).get();
            managedFileHandler.close();
            return new Pointer(Pointer.TYPE_NODE, position, chunk);
        }

    }

}
