package com.github.sepgh.internal.storage;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.tree.TreeNode;
import com.github.sepgh.internal.utils.FileUtils;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class IndexFileManager {
    public static final String INDEX_FILE_NAME = "index";
    private final Path path;
    private final Map<Integer, AsynchronousFileChannel> pool = new HashMap<>(5); // Map of chunk to
    private final Map<Integer, IndexHeader> indexHeaders = new HashMap<>(5);
    private final HeaderReader headerReader = new HeaderReader();
    private final EngineConfig engineConfig;

    public IndexFileManager(Path path, EngineConfig engineConfig) {
        this.path = path;
        this.engineConfig = engineConfig;
    }

    public IndexFileManager(Path path) {
        this(path, EngineConfig.Default.getDefault());
    }

    @SneakyThrows
    private synchronized AsynchronousFileChannel getAsynchronousFileChannel(int chunk){
        if (pool.containsKey(chunk)){
            return pool.get(chunk);
        }

        Path indexPath = Path.of(path.toString(), String.format("%s.%d", INDEX_FILE_NAME, chunk));
        AsynchronousFileChannel channel = AsynchronousFileChannel.open(indexPath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

        pool.put(chunk, channel);

        initializeFile(channel);
        initializeHeader(chunk, channel);
        return channel;
    }

    private void initializeFile(AsynchronousFileChannel channel) throws IOException {
        if (channel.size() == 0) {
            ByteBuffer buffer = ByteBuffer.allocate(1024 + (engineConfig.indexGrowthAllocationSize() + engineConfig.getPaddedSize()));
            long position = 0;
            Future<Integer> operation = channel.write(buffer, position);
            buffer.clear();

            while(!operation.isDone());
            channel.force(true);
        }

    }

    protected void initializeHeader(int chunk, AsynchronousFileChannel channel) throws ExecutionException, InterruptedException {
        Future<byte[]> future = headerReader.readUntilChar(channel, IndexHeader.HEADER_ENDING_CHAR);
        byte[] bytes = future.get();
        indexHeaders.put(chunk, new IndexHeader(bytes));
    }

    // Todo: Note that maybe this could be cached and kept until it changes? combination of table name and pointer can make it happen
    // Todo: maybe if some other thread is writing (modifying tree) we should not read. At least not if they are in same table?
    public Future<byte[]> readNode(int table, int position, int chunk){
        AsynchronousFileChannel asynchronousFileChannel = getAsynchronousFileChannel(chunk);
        long filePosition = indexHeaders.get(chunk).getTableMetaData(table).get().getOffset() + position;
        return FileUtils.readBytes(asynchronousFileChannel, filePosition, engineConfig.getPaddedSize());
    }

    /*
        This is where everything gets more complicated! A new chunk may be required if maximum file size is exceeded
        Also, current file may no longer have capacity and allocating space for new node may mess up the data by collision between trees
        In that case we need to allocate space in specific location by pushing next tables forward (AND UPDATE HEADER)
    */
    public Future<Long> allocateForNewNode(int table, int chunk) throws IOException, ExecutionException, InterruptedException {

        AsynchronousFileChannel asynchronousFileChannel = this.getAsynchronousFileChannel(chunk);

        /*
            We start by checking if there is already an empty allocated area in the BTree of current table in this chunk
            If there is, we return position of that area
         */
        IndexHeader indexHeader = indexHeaders.get(chunk);
        IndexHeader.TableMetaData tableMetaData = indexHeader.getTableMetaData(table).get();  // Table definitely exists
        int indexOfTableMetaData = indexHeader.indexOf(tableMetaData);

        boolean isLastTable = indexOfTableMetaData == indexHeader.size() - 1;
        long position = isLastTable ?
                asynchronousFileChannel.size() - engineConfig.indexGrowthAllocationSize()
                :
                indexHeader.getTableMetaData(indexOfTableMetaData + 1).get().getOffset() - engineConfig.indexGrowthAllocationSize();

        Future<byte[]> future = FileUtils.readBytes(asynchronousFileChannel, position, engineConfig.indexGrowthAllocationSize());
        byte[] bytes = future.get();
        Optional<Integer> optionalAdditionalPosition = getPossibleAllocationLocation(bytes);
        if (optionalAdditionalPosition.isPresent()){
            return CompletableFuture.completedFuture(position + optionalAdditionalPosition.get());
        }


        /*
            If there isn't an empty allocated location, we check if maximum size is reached.
            If it is, we won't be allocating and just move on to next chunk
                through recursion till we reach to a chunk where we can allocate space
         */
        if (asynchronousFileChannel.size() >= engineConfig.maxIndexFileSize()){
            return allocateForNewNode(table, chunk + 1);
        }


        /* Allocate space  */

        if (isLastTable){
            return FileUtils.allocate(asynchronousFileChannel, engineConfig.indexGrowthAllocationSize());
        }
        return FileUtils.allocate(asynchronousFileChannel, position, engineConfig.indexGrowthAllocationSize());
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

}
