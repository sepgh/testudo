package com.github.sepgh.internal.storage;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;
import com.github.sepgh.internal.tree.Pointer;
import com.github.sepgh.internal.utils.FileUtils;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.github.sepgh.internal.tree.node.BaseTreeNode.TYPE_INTERNAL_NODE_BIT;
import static com.github.sepgh.internal.tree.node.BaseTreeNode.TYPE_LEAF_NODE_BIT;

public class FileIndexStorageManager implements IndexStorageManager {
    private final Path path;
    private final HeaderManager headerManager;
    private final EngineConfig engineConfig;
    public static final String INDEX_FILE_NAME = "index";
    private final List<AsynchronousFileChannel> pool = new ArrayList<>(5);

    public FileIndexStorageManager(Path path, HeaderManager headerManager, EngineConfig engineConfig) throws IOException, ExecutionException, InterruptedException {
        this.path = path;
        this.headerManager = headerManager;
        this.engineConfig = engineConfig;
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
                    AsynchronousFileChannel asynchronousFileChannel = getAsynchronousFileChannel(chunkId);
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
        this.headerManager.update(header);
    }

    @SneakyThrows
    private AsynchronousFileChannel getAsynchronousFileChannel(int chunk) {
        if (pool.size() > chunk){
            return pool.get(chunk);
        }

        Path indexPath = Path.of(path.toString(), String.format("%s.%d", INDEX_FILE_NAME, chunk));
        AsynchronousFileChannel channel = AsynchronousFileChannel.open(
                indexPath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        );

        pool.add(chunk, channel);

        return channel;
    }

    @Override
    public CompletableFuture<Optional<NodeData>> getRoot(int table) {
        CompletableFuture<Optional<NodeData>> output = new CompletableFuture<>();

        Optional<Header.Table> optionalTable = headerManager.getHeader().getTableOfId(table);
        if (optionalTable.isEmpty() || optionalTable.get().getRoot() == null){
            output.complete(Optional.empty());
            return output;
        }

        Header.IndexChunk root = optionalTable.get().getRoot();
        FileUtils.readBytes(
                getAsynchronousFileChannel(root.getChunk()),
                root.getOffset(),
                engineConfig.getPaddedSize()
        ).whenComplete((bytes, throwable) -> {
            if (throwable != null){
                output.completeExceptionally(throwable);
                return;
            }

            output.complete(
                    Optional.of(
                            new NodeData(new Pointer(Pointer.TYPE_NODE, root.getOffset(), root.getChunk()), bytes)
                    )
            );
        });

        return output;
    }

    @Override
    public byte[] getEmptyNode() {
        return new byte[engineConfig.getPaddedSize()];
    }

    @Override
    public CompletableFuture<NodeData> readNode(int table, long position, int chunk) {
        CompletableFuture<NodeData> output = new CompletableFuture<>();
        AsynchronousFileChannel asynchronousFileChannel = getAsynchronousFileChannel(chunk);
        long filePosition = headerManager.getHeader().getTableOfId(table).get().getIndexChunk(chunk).get().getOffset() + position;
        FileUtils.readBytes(asynchronousFileChannel, filePosition, engineConfig.getPaddedSize()).whenComplete((bytes, throwable) -> {
            if (throwable != null){
                output.completeExceptionally(throwable);
                return;
            }
            output.complete(
                    new NodeData(
                            new Pointer(Pointer.TYPE_NODE, position, chunk),
                            bytes
                    )
            );
        });
        return output;
    }

    @Override
    public CompletableFuture<NodeData> writeNewNode(int table, byte[] data, boolean isRoot) throws IOException, ExecutionException, InterruptedException {
        CompletableFuture<NodeData> output = new CompletableFuture<>();
        Header.Table headerTable = headerManager.getHeader().getTableOfIndex(table).get();
        Pointer pointer = this.getAllocatedSpaceForNewNode(table, headerTable.getChunks().getLast().getChunk());

        if (data.length < engineConfig.getPaddedSize()){
            byte[] finalData = new byte[engineConfig.getPaddedSize()];
            System.arraycopy(data, 0, finalData, 0, engineConfig.getPaddedSize());
            data = finalData;
        }

        byte[] finalData1 = data;
        long offset = pointer.getPosition();
        pointer.setPosition(offset - headerManager.getHeader().getTableOfId(table).get().getIndexChunk(pointer.getChunk()).get().getOffset());
        FileUtils.write(getAsynchronousFileChannel(pointer.getChunk()), offset, data).whenComplete((size, throwable) -> {
            if (throwable != null){
                output.completeExceptionally(throwable);
            }

            output.complete(
                    new NodeData(pointer, finalData1)
            );
        });
        if (isRoot){
            headerManager.getHeader().getTableOfId(table).get().setRoot(new Header.IndexChunk(pointer.getChunk(), pointer.getPosition()));
            headerManager.update();
        }
        return output;
    }

    // Todo: as currently written in README, after allocating space, the chunk offset of tables after the tableId should be updated
    private Pointer getAllocatedSpaceForNewNode(int tableId, int chunk) throws IOException, ExecutionException, InterruptedException {
        Header.Table table = headerManager.getHeader().getTableOfIndex(tableId).get();
        Optional<Header.IndexChunk> optional = table.getIndexChunk(chunk);
        boolean newChunkCreated = optional.isEmpty();

        AsynchronousFileChannel asynchronousFileChannel = this.getAsynchronousFileChannel(chunk);
        int indexOfTableMetaData = headerManager.getHeader().indexOfTable(tableId);

        boolean isLastTable = indexOfTableMetaData == headerManager.getHeader().tablesCount() - 1;
        long fileSize = asynchronousFileChannel.size();
        long position = 0;
        if (fileSize != 0){
            position = isLastTable ?
                    fileSize - engineConfig.indexGrowthAllocationSize()
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
                long finalPosition = position + optionalAdditionalPosition.get();
                return new Pointer(Pointer.TYPE_NODE, finalPosition, chunk);
            }


            /*
                If there isn't an empty allocated location, we check if maximum size is reached.
                If it is, we won't be allocating and just move on to next chunk
                    through recursion till we reach to a chunk where we can allocate space
             */
            if (fileSize >= engineConfig.getBTreeMaxFileSize()){
                return getAllocatedSpaceForNewNode(tableId, chunk + 1);
            }


        }

        Long finalPosition;
        if (isLastTable || position == 0){
            finalPosition = FileUtils.allocate(asynchronousFileChannel, engineConfig.indexGrowthAllocationSize()).get();
        }else {
            finalPosition = FileUtils.allocate(asynchronousFileChannel, position, engineConfig.indexGrowthAllocationSize()).get();
        }

        if (newChunkCreated){
            List<Header.IndexChunk> newChunks = new ArrayList<>(table.getChunks());
            newChunks.add(new Header.IndexChunk(chunk, finalPosition));
            table.setChunks(newChunks);
            headerManager.update();
        }

        return new Pointer(Pointer.TYPE_NODE, finalPosition, chunk);
    }

    private Optional<Integer> getPossibleAllocationLocation(byte[] bytes){
        for (int i = 0; i < engineConfig.getBTreeGrowthNodeAllocationCount(); i++){
            int position = i * engineConfig.getPaddedSize();
            if ((bytes[position] & TYPE_LEAF_NODE_BIT) != TYPE_LEAF_NODE_BIT && (bytes[position] & TYPE_INTERNAL_NODE_BIT) != TYPE_INTERNAL_NODE_BIT){
                return Optional.of(position);
            }
        }
        return Optional.empty();
    }

    @Override
    public CompletableFuture<Integer> updateNode(int table, byte[] data, Pointer pointer) {
        long offset = headerManager.getHeader().getTableOfId(table).get().getIndexChunk(pointer.getChunk()).get().getOffset() + pointer.getPosition();
        return FileUtils.write(getAsynchronousFileChannel(pointer.getChunk()), offset, data);
    }

    @Override
    public void close() throws IOException {
        for (AsynchronousFileChannel asynchronousFileChannel : this.pool) {
            asynchronousFileChannel.close();
        }
    }
}
