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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;


/*
Todo: when a node is removed, depending on it's index and chunk we can keep a reference to it's location to reuse it in next allocation calls
*/

public class CompactFileIndexStorageManager extends BaseFileIndexStorageManager {

    public CompactFileIndexStorageManager(Path path, String customName, IndexHeaderManagerFactory indexHeaderManagerFactory, EngineConfig engineConfig, FileHandlerPool fileHandlerPool) throws IOException, ExecutionException, InterruptedException {
        super(path, customName, indexHeaderManagerFactory, engineConfig, fileHandlerPool);
    }

    public CompactFileIndexStorageManager(Path path, String customName, IndexHeaderManagerFactory indexHeaderManagerFactory, EngineConfig engineConfig, FileHandlerPool fileHandlerPool, int binarySpace) throws IOException, ExecutionException, InterruptedException {
        super(path, customName, indexHeaderManagerFactory, engineConfig, fileHandlerPool, binarySpace);
    }

    protected Path getIndexFilePath(int indexId, int chunk) {
        return Path.of(path.toString(), String.format("%s.%s.%d", INDEX_FILE_NAME, customName, chunk));
    }

    @Override
    protected IndexHeaderManager.Location getIndexBeginningInChunk(int indexId, int chunk) throws InterruptedException {
        Optional<IndexHeaderManager.Location> optional = this.indexHeaderManager.getIndexBeginningInChunk(indexId, chunk);
        if (optional.isPresent()) {
            return optional.get();
        }

        List<Integer> indexesInChunk = this.indexHeaderManager.getIndexesInChunk(chunk);
        IndexHeaderManager.Location location;
        if (indexesInChunk.isEmpty()) {
            location = new IndexHeaderManager.Location(chunk, 0);
        } else {
            AsynchronousFileChannel asynchronousFileChannel = this.acquireFileChannel(indexId, chunk);
            try {
                location = new IndexHeaderManager.Location(chunk, asynchronousFileChannel.size());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            this.releaseFileChannel(indexId, chunk);
        }

        try {
            this.indexHeaderManager.setIndexBeginningInChunk(indexId, location);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return location;
    }

    protected synchronized Pointer getAllocatedSpaceForNewNode(int indexId, int chunk) throws IOException, ExecutionException, InterruptedException {
        Optional<IndexHeaderManager.Location> optionalIndexBeginningLocation = this.indexHeaderManager.getIndexBeginningInChunk(indexId, chunk);
        boolean createNewChunk = optionalIndexBeginningLocation.isEmpty();

        // Despite if it's a new chunk for this index or not, go to next chunk if file is already full
        AsynchronousFileChannel asynchronousFileChannel = this.acquireFileChannel(indexId, chunk);
        if (
                this.engineConfig.getBTreeMaxFileSize() != EngineConfig.BTREE_UNLIMITED_FILE_SIZE &&
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

        Optional<IndexHeaderManager.Location> nextIndexBeginningInChunk = this.indexHeaderManager.getNextIndexBeginningInChunk(indexId, chunk);
        long positionToCheck;
        if (nextIndexBeginningInChunk.isPresent()){
            // Another index exists, so lets check if we have a space left before the next index
            positionToCheck = nextIndexBeginningInChunk.get().getOffset() - this.getIndexGrowthAllocationSize();
        } else {
            // Another index doesn't exist, so lets check if we have a space left in the end of the file
            positionToCheck = asynchronousFileChannel.size() - this.getIndexGrowthAllocationSize();
        }

        if (positionToCheck > 0){
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
        long allocatedOffset;
        if (nextIndexBeginningInChunk.isPresent()){
            allocatedOffset = FileUtils.allocate(
                    asynchronousFileChannel,
                    nextIndexBeginningInChunk.get().getOffset(),
                    this.getIndexGrowthAllocationSize()
            ).get();
            Integer nextIndex = this.indexHeaderManager.getNextIndexIdInChunk(indexId, chunk).get();
            this.indexHeaderManager.setIndexBeginningInChunk(nextIndex, new IndexHeaderManager.Location(chunk, nextIndexBeginningInChunk.get().getOffset() + this.getIndexGrowthAllocationSize()));
        } else {
            allocatedOffset = FileUtils.allocate(
                    asynchronousFileChannel,
                    this.getIndexGrowthAllocationSize()
            ).get();
        }

        this.releaseFileChannel(indexId, chunk);

        return new Pointer(Pointer.TYPE_NODE, allocatedOffset, chunk);
    }

}
