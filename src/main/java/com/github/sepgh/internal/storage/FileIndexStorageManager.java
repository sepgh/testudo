package com.github.sepgh.internal.storage;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.storage.exception.ChunkIsFullException;
import com.github.sepgh.internal.storage.header.HeaderManager;
import com.github.sepgh.internal.tree.Pointer;
import com.github.sepgh.internal.tree.TreeNode;
import com.github.sepgh.internal.utils.FileUtils;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

@Slf4j
public class FileIndexStorageManager implements IndexStorageManager {
    public static final String INDEX_FILE_NAME = "index";
    private final Path path;
    private final Map<Integer, AsynchronousFileChannel> pool = new HashMap<>(5); // Map of chunk to
    private final HeaderManager headerManager;

    @Getter
    private final EngineConfig engineConfig;

    public FileIndexStorageManager(Path path, EngineConfig engineConfig, HeaderManager headerManager) {
        this.path = path;
        this.engineConfig = engineConfig;
        this.headerManager = headerManager;
    }

    public FileIndexStorageManager(Path path, HeaderManager headerManager) {
        this(path, EngineConfig.Default.getDefault(), headerManager);
    }

    @SneakyThrows
    private synchronized AsynchronousFileChannel getAsynchronousFileChannel(int chunk) {
        if (pool.containsKey(chunk)){
            return pool.get(chunk);
        }

        Path indexPath = Path.of(path.toString(), String.format("%s.%d", INDEX_FILE_NAME, chunk));
        AsynchronousFileChannel channel = AsynchronousFileChannel.open(indexPath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        pool.put(chunk, channel);

        initializeFile(channel);
        return channel;
    }

    private void initializeFile(AsynchronousFileChannel channel) throws IOException, ExecutionException, InterruptedException {
        if (channel.size() == 0) {
            FileUtils.write(channel, 0, new byte[engineConfig.indexGrowthAllocationSize()]).get();
        }
    }


    @Override
    public Optional<Pointer> getRoot(int table) {
        return Optional.empty();  // Todo
    }

    // Todo: Note that maybe this could be cached and kept until it changes? combination of table name and pointer can make it happen
    // Todo: maybe if some other thread is writing (modifying tree) we should not read. At least not if they are in same table?
    public CompletableFuture<byte[]> readNode(int table, long position, int chunk){
        AsynchronousFileChannel asynchronousFileChannel = getAsynchronousFileChannel(chunk);
        long filePosition = headerManager.getHeader().getTableOfId(table).get().getIndexChunk(chunk).get().getOffset() + position;
        return FileUtils.readBytes(asynchronousFileChannel, filePosition, engineConfig.getPaddedSize());
    }

    /*
        This is where everything gets more complicated! A new chunk may be required if maximum file size is exceeded
        Also, current file may no longer have capacity and allocating space for new node may mess up the data by collision between trees
        In that case we need to allocate space in specific location by pushing next tables forward (AND UPDATE HEADER)
    */
    @Override
    public CompletableFuture<AllocationResult> allocateForNewNode(int table, int chunk) throws IOException, ChunkIsFullException {

        AsynchronousFileChannel asynchronousFileChannel = this.getAsynchronousFileChannel(chunk);

        /*
            We start by checking if there is already an empty allocated area in the BTree of current table in this chunk
            If there is, we return position of that area
         */

        int indexOfTableMetaData = headerManager.getHeader().indexOfTable(table);

        boolean isLastTable = indexOfTableMetaData == headerManager.getHeader().tablesCount() - 1;
        long position = isLastTable ?
                asynchronousFileChannel.size() - engineConfig.indexGrowthAllocationSize()
                :
                headerManager.getHeader().getTableOfIndex(indexOfTableMetaData + 1).get().getIndexChunk(chunk).get().getOffset() - engineConfig.indexGrowthAllocationSize();

        Future<byte[]> future = FileUtils.readBytes(asynchronousFileChannel, position, engineConfig.indexGrowthAllocationSize());
        byte[] bytes = new byte[0];
        try {
            bytes = future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
        Optional<Integer> optionalAdditionalPosition = getPossibleAllocationLocation(bytes);
        if (optionalAdditionalPosition.isPresent()){
            return CompletableFuture.completedFuture(new AllocationResult(position + optionalAdditionalPosition.get(), engineConfig.getPaddedSize()));  // Todo: caller cant know we changed chunk
            // Header may need an update, as this table may not have been available in another chunk but now will be available
        }


        /*
            If there isn't an empty allocated location, we check if maximum size is reached.
            If it is, we won't be allocating and just move on to next chunk
                through recursion till we reach to a chunk where we can allocate space
         */
        if (asynchronousFileChannel.size() >= engineConfig.getBTreeMaxFileSize()){
            throw new ChunkIsFullException();
        }


        /* Allocate space  */
        CompletableFuture<AllocationResult> resultsCompletableFuture = new CompletableFuture<>();

        BiConsumer<Long, Throwable> consumer = (aLong, throwable) -> {
            resultsCompletableFuture.complete(
                    new AllocationResult(
                            aLong,
                            engineConfig.getPaddedSize()
                    )
            );
        };

        if (isLastTable){
            FileUtils.allocate(asynchronousFileChannel, engineConfig.indexGrowthAllocationSize()).whenComplete(consumer);
        }else {
            FileUtils.allocate(asynchronousFileChannel, position, engineConfig.indexGrowthAllocationSize()).whenComplete(consumer);
        }
        return resultsCompletableFuture;
    }

    @Override
    public boolean chunkHasSpaceForNode(int chunk) throws IOException {
        AsynchronousFileChannel asynchronousFileChannel = this.getAsynchronousFileChannel(chunk);
        return asynchronousFileChannel.size() + engineConfig.getPaddedSize() <= engineConfig.getBTreeMaxFileSize();
    }

    @Override
    public CompletableFuture<Integer> writeNode(int table, byte[] data, long position, int chunk) {
        // Todo: maybe check if space is actually clear before writing, and have one method for write and other for overwrite
        AsynchronousFileChannel asynchronousFileChannel = this.getAsynchronousFileChannel(chunk);
        return FileUtils.write(asynchronousFileChannel, position, data);
    }

    private Optional<Integer> getPossibleAllocationLocation(byte[] bytes){
        for (int i = 0; i < engineConfig.getBTreeGrowthNodeAllocationCount(); i++){
            int position = i * engineConfig.getPaddedSize();
            if (bytes[position] != TreeNode.TYPE_LEAF_NODE && bytes[position] != TreeNode.TYPE_INTERNAL_NODE){
                return Optional.of(position);
            }
        }
        return Optional.empty();
    }

    public void close() {
        pool.forEach((integer, asynchronousFileChannel) -> {
            try {
                asynchronousFileChannel.close();
            } catch (IOException e) {
                log.error("Failed to close file channel for chunk " + integer, e);
            }
        });
    }

}
