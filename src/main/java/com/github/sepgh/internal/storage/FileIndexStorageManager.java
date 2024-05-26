package com.github.sepgh.internal.storage;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;
import com.github.sepgh.internal.index.Pointer;
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

import static com.github.sepgh.internal.index.tree.node.BaseTreeNode.TYPE_INTERNAL_NODE_BIT;
import static com.github.sepgh.internal.index.tree.node.BaseTreeNode.TYPE_LEAF_NODE_BIT;

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
    public CompletableFuture<NodeData> fillRoot(int table, byte[] data){
        CompletableFuture<NodeData> output = new CompletableFuture<>();

        Optional<Header.Table> optionalTable = headerManager.getHeader().getTableOfId(table);

        Header.Table headerTable = optionalTable.get();
        if (optionalTable.isEmpty() || headerTable.getRoot() == null){
            output.completeExceptionally(new Exception("Root position is undetermined")); // Todo
            return output;
        }

        long rootAbsolutePosition = headerTable.getIndexChunk(headerTable.getRoot().getChunk()).get().getOffset() + headerTable.getRoot().getOffset();
        FileUtils.write(getAsynchronousFileChannel(headerTable.getRoot().getChunk()),rootAbsolutePosition, data).whenComplete((size, throwable) -> {
            if (throwable != null){
                output.completeExceptionally(throwable);
            }

            output.complete(
                    new NodeData(
                            new Pointer(
                                    Pointer.TYPE_NODE,
                                    optionalTable.get().getRoot().getOffset(),
                                    optionalTable.get().getRoot().getChunk()
                            ),
                            data
                    )
            );
        });

        return output;
    }

    @Override
    public CompletableFuture<Optional<NodeData>> getRoot(int table) {
        CompletableFuture<Optional<NodeData>> output = new CompletableFuture<>();

        Optional<Header.Table> optionalTable = headerManager.getHeader().getTableOfId(table);
        if (optionalTable.isEmpty() || optionalTable.get().getRoot() == null){
            output.complete(Optional.empty());
            return output;
        }

        Header.Table headerTable = optionalTable.get();
        Header.IndexChunk root = headerTable.getRoot();
        FileUtils.readBytes(
                getAsynchronousFileChannel(root.getChunk()),
                headerTable.getIndexChunk(root.getChunk()).get().getOffset()  + root.getOffset(),
                engineConfig.getPaddedSize()
        ).whenComplete((bytes, throwable) -> {
            if (throwable != null){
                output.completeExceptionally(throwable);
                return;
            }

            if (bytes.length == 0 || bytes[0] == (byte) 0x00){
                output.complete(Optional.empty());
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
        Header.Table headerTable = headerManager.getHeader().getTableOfId(table).get();
        Pointer pointer = this.getAllocatedSpaceForNewNode(table, headerTable.getChunks().getLast().getChunk());

        if (data.length < engineConfig.getPaddedSize()){
            byte[] finalData = new byte[engineConfig.getPaddedSize()];
            System.arraycopy(data, 0, finalData, 0, engineConfig.getPaddedSize());
            data = finalData;
        }

        byte[] finalData1 = data;
        long offset = pointer.getPosition();

        // setting pointer position according to the table offset. Reading table again since a new chunk may have been created
        pointer.setPosition(offset - headerManager.getHeader().getTableOfId(table).get().getIndexChunk(pointer.getChunk()).get().getOffset());
        FileUtils.write(getAsynchronousFileChannel(pointer.getChunk()), offset, data).whenComplete((size, throwable) -> {
            if (throwable != null){
                output.completeExceptionally(throwable);
                return;
            }

            output.complete(
                    new NodeData(pointer, finalData1)
            );
        });
        if (isRoot){
            this.updateRoot(table, pointer);
        }
        return output;
    }

    private List<Header.Table> getTablesIncludingChunk(int chunk){
        return headerManager.getHeader().getTables().stream().filter(table -> table.getIndexChunk(chunk).isPresent()).toList();
    }

    private int getIndexOfTable(List<Header.Table> tables, int table){
        int index = -1;
        for (int i = 0; i < tables.size(); i++)
            if (tables.get(i).getId() == table){
                index = i;
                break;
            }

        return index;
    }

    /**
     * ## How it works:
     *      if the chunk is new for this table, then just allocate at the end of the file and add the chunk index to header
     *      and return. But if there isn't space left, try next chunk.
     *      if file size is not 0,
     *          see if there is any empty space in the file (allocated before but never written to) and return the
     *          pointer to that space if is available.
     *      if file size is equal or greater than maximum file size try next chunk
     *      allocate space at end of the file and return pointer if the table is at end of the file
     *      otherwise, allocate space right before the next table in this chunk begins and push next tables to the end
     *          also make sure to update possible roots and chunk indexes offset for next tables
     * @param tableId table to allocate space in
     * @param chunk chunk to allocate space in
     * @return Pointer to the beginning of allocated location
     */
    private Pointer getAllocatedSpaceForNewNode(int tableId, int chunk) throws IOException, ExecutionException, InterruptedException {
        Header.Table table = headerManager.getHeader().getTableOfId(tableId).get();
        Optional<Header.IndexChunk> optional = table.getIndexChunk(chunk);
        boolean newChunkCreatedForTable = optional.isEmpty();

        AsynchronousFileChannel asynchronousFileChannel = this.getAsynchronousFileChannel(chunk);
        long fileSize = asynchronousFileChannel.size();

        if (newChunkCreatedForTable){
            if (fileSize >= engineConfig.getBTreeMaxFileSize()){
                return getAllocatedSpaceForNewNode(tableId, chunk + 1);
            } else {
                Long position = FileUtils.allocate(asynchronousFileChannel, engineConfig.indexGrowthAllocationSize()).get();
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
                            fileSize - engineConfig.indexGrowthAllocationSize()
                            :
                            tablesIncludingChunk.get(indexOfTable + 1).getIndexChunk(chunk).get().getOffset() - engineConfig.indexGrowthAllocationSize();

            if (positionToCheck > 0 && positionToCheck > tablesIncludingChunk.get(indexOfTable).getIndexChunk(chunk).get().getOffset()) {
                byte[] bytes = FileUtils.readBytes(asynchronousFileChannel, positionToCheck, engineConfig.indexGrowthAllocationSize()).get();
                Optional<Integer> optionalAdditionalPosition = getPossibleAllocationLocation(bytes);
                if (optionalAdditionalPosition.isPresent()){
                    long finalPosition = positionToCheck + optionalAdditionalPosition.get();
                    return new Pointer(Pointer.TYPE_NODE, finalPosition, chunk);
                }
            }
        }

        if (fileSize >= engineConfig.getBTreeMaxFileSize())
            return this.getAllocatedSpaceForNewNode(tableId, chunk + 1);


        long allocatedOffset;
        if (isLastTable){
            allocatedOffset = FileUtils.allocate(asynchronousFileChannel, engineConfig.indexGrowthAllocationSize()).get();
        } else {
            allocatedOffset = FileUtils.allocate(
                    asynchronousFileChannel,
                    tablesIncludingChunk.get(indexOfTable + 1).getIndexChunk(chunk).get().getOffset(),
                    engineConfig.indexGrowthAllocationSize()
            ).get();

            for (int i = indexOfTable + 1; i < tablesIncludingChunk.size(); i++){
                Header.Table nextTable = tablesIncludingChunk.get(i);
                Header.IndexChunk indexChunk = nextTable.getIndexChunk(chunk).get();
                indexChunk.setOffset(indexChunk.getOffset() + engineConfig.indexGrowthAllocationSize());
            }
            headerManager.update();
        }

        return new Pointer(Pointer.TYPE_NODE, allocatedOffset, chunk);
    }

    /*
    * Returns the empty position within byte[] passed to the method
    */
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
    public CompletableFuture<Integer> updateNode(int table, byte[] data, Pointer pointer, boolean isRoot) throws IOException {
        long offset = headerManager.getHeader().getTableOfId(table).get().getIndexChunk(pointer.getChunk()).get().getOffset() + pointer.getPosition();
        CompletableFuture<Integer> output = FileUtils.write(getAsynchronousFileChannel(pointer.getChunk()), offset, data);
        if (isRoot){
            this.updateRoot(table, pointer);
        }
        return output;
    }

    private void updateRoot(int table, Pointer pointer) throws IOException {
        headerManager.getHeader().getTableOfId(table).get().setRoot(new Header.IndexChunk(pointer.getChunk(), pointer.getPosition()));
        headerManager.update();
    }

    @Override
    public void close() throws IOException {
        for (AsynchronousFileChannel asynchronousFileChannel : this.pool) {
            asynchronousFileChannel.close();
        }
    }

    @Override
    public CompletableFuture<Integer> removeNode(int table, Pointer pointer) {
        long offset = headerManager.getHeader().getTableOfId(table).get().getIndexChunk(pointer.getChunk()).get().getOffset() + pointer.getPosition();
        return FileUtils.write(getAsynchronousFileChannel(pointer.getChunk()), offset, new byte[engineConfig.getPaddedSize()]);
    }
}
