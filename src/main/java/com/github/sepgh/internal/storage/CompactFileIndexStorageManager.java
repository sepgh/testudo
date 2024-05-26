package com.github.sepgh.internal.storage;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;
import com.github.sepgh.internal.storage.pool.FileHandlerPool;
import com.github.sepgh.internal.storage.pool.ManagedFileHandler;
import com.github.sepgh.internal.utils.FileUtils;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class CompactFileIndexStorageManager extends BaseFileIndexStorageManager {
    public CompactFileIndexStorageManager(Path path, HeaderManager headerManager, EngineConfig engineConfig, FileHandlerPool fileHandlerPool) throws IOException, ExecutionException, InterruptedException {
        super(path, headerManager, engineConfig, fileHandlerPool);
        this.initialize();
    }

    public CompactFileIndexStorageManager(Path path, HeaderManager headerManager, EngineConfig engineConfig) throws IOException, ExecutionException, InterruptedException {
        super(path, headerManager, engineConfig);
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
                            long position = FileUtils.allocate(asynchronousFileChannel, engineConfig.indexGrowthAllocationSize()).get();
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

}
