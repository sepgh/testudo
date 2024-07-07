package com.github.sepgh.testudo.storage;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.storage.header.Header;
import com.github.sepgh.testudo.storage.header.HeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandlerPool;
import com.github.sepgh.testudo.storage.pool.ManagedFileHandler;
import com.github.sepgh.testudo.utils.FileUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class CompactFileIndexStorageManager extends BaseFileIndexStorageManager {

    public CompactFileIndexStorageManager(Path path, @Nullable String customName, HeaderManager headerManager, EngineConfig engineConfig, FileHandlerPool fileHandlerPool) throws IOException, ExecutionException, InterruptedException {
        super(path, customName, headerManager, engineConfig, fileHandlerPool);
    }

    public CompactFileIndexStorageManager(Path path, @Nullable String customName, HeaderManager headerManager, EngineConfig engineConfig, FileHandlerPool fileHandlerPool, int binarySpace) throws IOException, ExecutionException, InterruptedException {
        super(path, customName, headerManager, engineConfig, fileHandlerPool, binarySpace);
    }

    public CompactFileIndexStorageManager(Path path, HeaderManager headerManager, EngineConfig engineConfig, FileHandlerPool fileHandlerPool, int binarySpaceMax) throws IOException, ExecutionException, InterruptedException {
        super(path, headerManager, engineConfig, fileHandlerPool, binarySpaceMax);
    }

    public CompactFileIndexStorageManager(Path path, HeaderManager headerManager, EngineConfig engineConfig, int binarySpaceMax) throws IOException, ExecutionException, InterruptedException {
        super(path, headerManager, engineConfig, binarySpaceMax);
    }


    protected Path getIndexFilePath(int table, int chunk) {
        if (customName == null)
            return Path.of(path.toString(), String.format("%s.%d", INDEX_FILE_NAME, chunk));
        return Path.of(path.toString(), String.format("%s.%s.%d", INDEX_FILE_NAME, customName, chunk));
    }

    protected Pointer getAllocatedSpaceForNewNode(int tableId, int chunk) throws IOException, ExecutionException, InterruptedException {
        Header.Table table = headerManager.getHeader().getTableOfId(tableId).get();
        Optional<Header.IndexChunk> optional = table.getIndexChunk(chunk);
        boolean newChunkCreatedForTable = optional.isEmpty();

        ManagedFileHandler managedFileHandler = this.getManagedFileHandler(tableId, chunk);
        AsynchronousFileChannel asynchronousFileChannel = managedFileHandler.getAsynchronousFileChannel();
        long fileSize = asynchronousFileChannel.size();

        if (newChunkCreatedForTable){
            if (fileSize >= engineConfig.getBTreeMaxFileSize()){
                managedFileHandler.close();
                return getAllocatedSpaceForNewNode(tableId, chunk + 1);
            } else {
                Long position = FileUtils.allocate(asynchronousFileChannel, this.getIndexGrowthAllocationSize()).get();
                managedFileHandler.close();
                List<Header.IndexChunk> newChunks = new ArrayList<>(table.getChunks());
                newChunks.add(new Header.IndexChunk(chunk, position));
                table.setChunks(newChunks);
                headerManager.update();
                return new Pointer(Pointer.TYPE_NODE, position, chunk);
            }
        }

        List<Header.Table> tablesIncludingChunk = getTablesIncludingChunk(chunk);
        int indexOfTable = getIndexOfTable(tablesIncludingChunk, tableId);
        boolean isLastTable = indexOfTable == tablesIncludingChunk.size() - 1;


        if (fileSize > 0){
            long positionToCheck =
                    isLastTable ?
                            fileSize - this.getIndexGrowthAllocationSize()
                            :
                            tablesIncludingChunk.get(indexOfTable + 1).getIndexChunk(chunk).get().getOffset() - this.getIndexGrowthAllocationSize();

            if (positionToCheck > 0 && positionToCheck > tablesIncludingChunk.get(indexOfTable).getIndexChunk(chunk).get().getOffset()) {
                byte[] bytes = FileUtils.readBytes(asynchronousFileChannel, positionToCheck, this.getIndexGrowthAllocationSize()).get();
                Optional<Integer> optionalAdditionalPosition = getPossibleAllocationLocation(bytes);
                if (optionalAdditionalPosition.isPresent()){
                    long finalPosition = positionToCheck + optionalAdditionalPosition.get();
                    managedFileHandler.close();
                    return new Pointer(Pointer.TYPE_NODE, finalPosition, chunk);
                }
            }
        }

        if (fileSize >= engineConfig.getBTreeMaxFileSize()){
            managedFileHandler.close();
            return this.getAllocatedSpaceForNewNode(tableId, chunk + 1);
        }


        long allocatedOffset;
        if (isLastTable){
            allocatedOffset = FileUtils.allocate(asynchronousFileChannel, this.getIndexGrowthAllocationSize()).get();
        } else {
            allocatedOffset = FileUtils.allocate(
                    asynchronousFileChannel,
                    tablesIncludingChunk.get(indexOfTable + 1).getIndexChunk(chunk).get().getOffset(),
                    this.getIndexGrowthAllocationSize()
            ).get();

            for (int i = indexOfTable + 1; i < tablesIncludingChunk.size(); i++){
                Header.Table nextTable = tablesIncludingChunk.get(i);
                Header.IndexChunk indexChunk = nextTable.getIndexChunk(chunk).get();
                indexChunk.setOffset(indexChunk.getOffset() + this.getIndexGrowthAllocationSize());
            }
            headerManager.update();
        }
        managedFileHandler.close();

        return new Pointer(Pointer.TYPE_NODE, allocatedOffset, chunk);
    }

}
