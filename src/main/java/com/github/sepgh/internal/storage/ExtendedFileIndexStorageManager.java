package com.github.sepgh.internal.storage;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;
import com.github.sepgh.internal.storage.pool.FileHandlerPool;
import com.github.sepgh.internal.storage.pool.ManagedFileHandler;
import com.github.sepgh.internal.utils.FileUtils;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class ExtendedFileIndexStorageManager extends CompactFileIndexStorageManager {


    public ExtendedFileIndexStorageManager(Path path, HeaderManager headerManager, EngineConfig engineConfig, FileHandlerPool fileHandlerPool) throws IOException, ExecutionException, InterruptedException {
        super(path, headerManager, engineConfig, fileHandlerPool);
    }

    public ExtendedFileIndexStorageManager(Path path, HeaderManager headerManager, EngineConfig engineConfig) throws IOException, ExecutionException, InterruptedException {
        super(path, headerManager, engineConfig);
    }

    protected Path getIndexFilePath(int table, int chunk) {
        return Path.of(path.toString(), String.format("%s-%d.%d", INDEX_FILE_NAME, table, chunk));
    }

    protected Pointer getAllocatedSpaceForNewNode(int tableId, int chunk) throws IOException, ExecutionException, InterruptedException {
        Header.Table table = headerManager.getHeader().getTableOfId(tableId).get();
        Optional<Header.IndexChunk> optional = table.getIndexChunk(chunk);
        boolean newChunkCreatedForTable = optional.isEmpty();

        ManagedFileHandler managedFileHandler = this.getManagedFileHandler(tableId, chunk);
        AsynchronousFileChannel asynchronousFileChannel = managedFileHandler.getAsynchronousFileChannel();
        long fileSize = asynchronousFileChannel.size();

        if (newChunkCreatedForTable){
            FileUtils.allocate(asynchronousFileChannel, engineConfig.indexGrowthAllocationSize()).get();
            managedFileHandler.close();
            List<Header.IndexChunk> newChunks = new ArrayList<>(table.getChunks());
            newChunks.add(new Header.IndexChunk(chunk, 0));
            table.setChunks(newChunks);
            headerManager.update();
            return new Pointer(Pointer.TYPE_NODE, 0, chunk);

        }

        if (fileSize > 0){
            long positionToCheck = fileSize - engineConfig.indexGrowthAllocationSize();
            byte[] bytes = FileUtils.readBytes(asynchronousFileChannel, positionToCheck, engineConfig.indexGrowthAllocationSize()).get();
            Optional<Integer> optionalAdditionalPosition = getPossibleAllocationLocation(bytes);
            if (optionalAdditionalPosition.isPresent()){
                long finalPosition = positionToCheck + optionalAdditionalPosition.get();
                managedFileHandler.close();
                return new Pointer(Pointer.TYPE_NODE, finalPosition, chunk);
            }
        }

        if (fileSize >= engineConfig.getBTreeMaxFileSize()){
            managedFileHandler.close();
            return this.getAllocatedSpaceForNewNode(tableId, chunk + 1);
        }

        long allocatedOffset = FileUtils.allocate(asynchronousFileChannel, engineConfig.indexGrowthAllocationSize()).get();

        managedFileHandler.close();

        return new Pointer(Pointer.TYPE_NODE, allocatedOffset, chunk);
    }

}
