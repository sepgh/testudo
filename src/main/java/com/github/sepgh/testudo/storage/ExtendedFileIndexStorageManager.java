package com.github.sepgh.testudo.storage;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.storage.header.Header;
import com.github.sepgh.testudo.storage.header.HeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandlerPool;
import com.github.sepgh.testudo.storage.pool.ManagedFileHandler;
import com.github.sepgh.testudo.utils.FileUtils;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class ExtendedFileIndexStorageManager extends CompactFileIndexStorageManager {

    public ExtendedFileIndexStorageManager(Path path, HeaderManager headerManager, EngineConfig engineConfig, FileHandlerPool fileHandlerPool, int binarySpaceMax) throws IOException, ExecutionException, InterruptedException {
        super(path, headerManager, engineConfig, fileHandlerPool, binarySpaceMax);
    }

    public ExtendedFileIndexStorageManager(Path path, HeaderManager headerManager, EngineConfig engineConfig, int binarySpaceMax) throws IOException, ExecutionException, InterruptedException {
        super(path, headerManager, engineConfig, binarySpaceMax);
    }

    protected Path getIndexFilePath(int table, int chunk) {
        if (customName == null)
            return Path.of(path.toString(), String.format("%s-%d.%d", INDEX_FILE_NAME, table, chunk));
        return Path.of(path.toString(), String.format("%s-%d.%s.%d", INDEX_FILE_NAME, table, customName, chunk));
    }

    protected Pointer getAllocatedSpaceForNewNode(int tableId, int chunk) throws IOException, ExecutionException, InterruptedException {
        Header.Table table = headerManager.getHeader().getTableOfId(tableId).get();
        Optional<Header.IndexChunk> optional = table.getIndexChunk(chunk);
        boolean newChunkCreatedForTable = optional.isEmpty();

        ManagedFileHandler managedFileHandler = this.getManagedFileHandler(tableId, chunk);
        AsynchronousFileChannel asynchronousFileChannel = managedFileHandler.getAsynchronousFileChannel();
        long fileSize = asynchronousFileChannel.size();

        if (newChunkCreatedForTable){
            FileUtils.allocate(asynchronousFileChannel, this.getIndexGrowthAllocationSize()).get();
            managedFileHandler.close();
            List<Header.IndexChunk> newChunks = new ArrayList<>(table.getChunks());
            newChunks.add(new Header.IndexChunk(chunk, 0));
            table.setChunks(newChunks);
            headerManager.update();
            return new Pointer(Pointer.TYPE_NODE, 0, chunk);

        }

        if (fileSize > 0){
            long positionToCheck = fileSize - this.getIndexGrowthAllocationSize();
            byte[] bytes = FileUtils.readBytes(asynchronousFileChannel, positionToCheck, this.getIndexGrowthAllocationSize()).get();
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

        long allocatedOffset = FileUtils.allocate(asynchronousFileChannel, this.getIndexGrowthAllocationSize()).get();

        managedFileHandler.close();

        return new Pointer(Pointer.TYPE_NODE, allocatedOffset, chunk);
    }

}
