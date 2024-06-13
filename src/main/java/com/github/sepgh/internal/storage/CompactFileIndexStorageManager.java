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

public class CompactFileIndexStorageManager extends BaseFileIndexStorageManager {
    public CompactFileIndexStorageManager(Path path, HeaderManager headerManager, EngineConfig engineConfig, FileHandlerPool fileHandlerPool, int binarySpaceMax) throws IOException, ExecutionException, InterruptedException {
        super(path, headerManager, engineConfig, fileHandlerPool, binarySpaceMax);
        this.initialize();
    }

    public CompactFileIndexStorageManager(Path path, HeaderManager headerManager, EngineConfig engineConfig, int binarySpaceMax) throws IOException, ExecutionException, InterruptedException {
        super(path, headerManager, engineConfig, binarySpaceMax);
        this.initialize();
    }


    // Todo: temporarily this is the solution for allocating space for new tables
    //       alternatively, previous state of the database and new state should be compared, some data may need removal
    private void initialize() throws IOException, ExecutionException, InterruptedException {
        Header header = this.headerManager.getHeader();
        int chunkId = 0;
        for (Header.Table table : header.getTables()) {
            if (!table.isInitialized()) {
                boolean stored = false;
                while (!stored){
                    try (ManagedFileHandler managedFileHandler = this.getManagedFileHandler(table.getId(), chunkId)){
                        AsynchronousFileChannel asynchronousFileChannel = managedFileHandler.getAsynchronousFileChannel();
                        if (asynchronousFileChannel.size() != engineConfig.getBTreeMaxFileSize()){
                            long position = FileUtils.allocate(asynchronousFileChannel, this.getIndexGrowthAllocationSize()).get();
                            table.setChunks(Collections.singletonList(new Header.IndexChunk(chunkId, position)));
                            table.setInitialized(true);
                            stored = true;
                        } else {
                            chunkId++;
                        }
                    }
                }
            }
        }
        this.headerManager.update(header);
    }

    protected Path getIndexFilePath(int table, int chunk) {
        return Path.of(path.toString(), String.format("%s.%d", INDEX_FILE_NAME, chunk));
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
