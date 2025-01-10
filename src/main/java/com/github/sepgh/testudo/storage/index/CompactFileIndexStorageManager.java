package com.github.sepgh.testudo.storage.index;

import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.ds.KVSize;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.ErrorMessage;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManager;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManagerSingletonFactory;
import com.github.sepgh.testudo.storage.pool.FileHandlerPool;
import com.github.sepgh.testudo.storage.pool.ManagedFileHandler;
import com.github.sepgh.testudo.utils.FileUtils;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class CompactFileIndexStorageManager extends BaseFileIndexStorageManager {

    public CompactFileIndexStorageManager(IndexHeaderManagerSingletonFactory indexHeaderManagerSingletonFactory, EngineConfig engineConfig, FileHandlerPool fileHandlerPool) {
        super(null, indexHeaderManagerSingletonFactory, engineConfig, fileHandlerPool);
    }

    public CompactFileIndexStorageManager(IndexHeaderManagerSingletonFactory indexHeaderManagerSingletonFactory, EngineConfig engineConfig) {
        super(indexHeaderManagerSingletonFactory, engineConfig);
    }

    protected Path getIndexFilePath(int indexId, int chunk) {
        return Path.of(path.toString(), String.format("%s.bin", INDEX_FILE_NAME));
    }

    @Override
    protected IndexHeaderManager.Location getIndexBeginningInChunk(int indexId, int chunk) {
        return new IndexHeaderManager.Location(0,0);
    }

    /* Todo:
    *           When nodes with different types of keys and values try to allocate space from the same file
    *       we will face a problem when we want to check if there is empty space at the end of the file
    *           The way checking for space works is by seeing if according the the currently passed `kvSize` there is a node
    *       at a specific byte (checking first byte and see if it equals to any bit flags of a tree node).
    *       This logic is present at getPossibleAllocationLocation() method.
    *           Considering that different kvSizes may be passed here, allocation can simply fail
    *       and a space that is already used by another node (but may be empty for now) may be returned!
    *           Possible workaround is either to track last node position and size, or to change implementation and use PageBuffer
    */
    protected Pointer getAllocatedSpaceForNewNode(int indexId, int chunk, KVSize kvSize) throws InternalOperationException {
        ManagedFileHandler managedFileHandler = this.getManagedFileHandler(indexId, 0);
        try {
            AsynchronousFileChannel asynchronousFileChannel = managedFileHandler.getAsynchronousFileChannel();

            synchronized (this){
                long fileSize = asynchronousFileChannel.size();

                // Check if we have an empty space
                // Todo: in reference to the comment above the method, isnt this if statement a lill dumb?
                //       After second allocation, its always true
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
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw new InternalOperationException(ErrorMessage.EM_FILE_ALLOCATION, e);
        }


    }

}
