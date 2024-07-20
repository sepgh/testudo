package com.github.sepgh.testudo.storage.index;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManager;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManagerFactory;
import com.github.sepgh.testudo.storage.pool.FileHandlerPool;
import com.github.sepgh.testudo.utils.FileUtils;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class ExtendedFileIndexStorageManager extends BaseFileIndexStorageManager {

    public ExtendedFileIndexStorageManager(String customName, IndexHeaderManagerFactory indexHeaderManagerFactory, EngineConfig engineConfig, FileHandlerPool fileHandlerPool) throws IOException, ExecutionException, InterruptedException {
        super(customName, indexHeaderManagerFactory, engineConfig, fileHandlerPool);
    }

    public ExtendedFileIndexStorageManager(String customName, IndexHeaderManagerFactory indexHeaderManagerFactory, EngineConfig engineConfig, FileHandlerPool fileHandlerPool, int binarySpace) throws IOException, ExecutionException, InterruptedException {
        super(customName, indexHeaderManagerFactory, engineConfig, fileHandlerPool, binarySpace);
    }

    protected Path getIndexFilePath(int indexId, int chunk) {
        return Path.of(path.toString(), String.format("%s-%d.%d", INDEX_FILE_NAME, indexId, chunk));
    }

    @Override
    protected IndexHeaderManager.Location getIndexBeginningInChunk(int indexId, int chunk) throws InterruptedException {
        Optional<IndexHeaderManager.Location> indexBeginningInChunk = this.indexHeaderManager.getIndexBeginningInChunk(indexId, chunk);
        return indexBeginningInChunk.orElseGet(() -> new IndexHeaderManager.Location(chunk, 0));
    }

    protected Pointer getAllocatedSpaceForNewNode(int indexId, int chunk) throws IOException, ExecutionException, InterruptedException {
        Optional<IndexHeaderManager.Location> optionalIndexBeginningLocation = this.indexHeaderManager.getIndexBeginningInChunk(indexId, chunk);
        boolean createNewChunk = optionalIndexBeginningLocation.isPresent();

        // Despite if it's a new chunk for this index or not, go to next chunk if file is already full
        AsynchronousFileChannel asynchronousFileChannel = this.acquireFileChannel(indexId, chunk);
        if (
                this.engineConfig.getBTreeMaxFileSize() != EngineConfig.UNLIMITED_FILE_SIZE &&
                        asynchronousFileChannel.size() >= this.engineConfig.getBTreeMaxFileSize()
        ) {
            this.releaseFileChannel(indexId, chunk);
            return this.getAllocatedSpaceForNewNode(indexId, chunk + 1);
        }

        // If it's a new chunk for this index, allocate at the end of the file!
        if (createNewChunk){
            this.indexHeaderManager.setIndexBeginningInChunk(indexId, new IndexHeaderManager.Location(chunk, asynchronousFileChannel.size()));
            Long position = FileUtils.allocate(asynchronousFileChannel, this.getIndexGrowthAllocationSize()).get();
            this.releaseFileChannel(indexId, chunk);
            return new Pointer(Pointer.TYPE_NODE, position, chunk);
        }


        // If it's not a new chunk for this index, see if other indexes exist or not

        long positionToCheck = asynchronousFileChannel.size() - this.getIndexGrowthAllocationSize();

        if (positionToCheck >= 0){
            // Check if we have an empty space
            byte[] bytes = FileUtils.readBytes(asynchronousFileChannel, positionToCheck, this.getIndexGrowthAllocationSize()).get();
            Optional<Integer> optionalAdditionalPosition = getPossibleAllocationLocation(bytes);
            if (optionalAdditionalPosition.isPresent()){
                long finalPosition = positionToCheck + optionalAdditionalPosition.get();
                this.releaseFileChannel(indexId, chunk);
                return new Pointer(Pointer.TYPE_NODE, finalPosition, chunk);
            }
        }

        // Empty space not found, allocate in the end or before next index
        long allocatedOffset = FileUtils.allocate(
                asynchronousFileChannel,
                this.getIndexGrowthAllocationSize()
        ).get();

        this.releaseFileChannel(indexId, chunk);

        return new Pointer(Pointer.TYPE_NODE, allocatedOffset, chunk);
    }

}
