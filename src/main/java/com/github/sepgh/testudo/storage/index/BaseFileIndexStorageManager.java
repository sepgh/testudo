package com.github.sepgh.testudo.storage.index;

import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManager;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManagerFactory;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.FileHandlerPool;
import com.github.sepgh.testudo.storage.pool.ManagedFileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import com.github.sepgh.testudo.utils.FileUtils;
import com.github.sepgh.testudo.ds.KVSize;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.testudo.index.tree.node.AbstractTreeNode.TYPE_INTERNAL_NODE_BIT;
import static com.github.sepgh.testudo.index.tree.node.AbstractTreeNode.TYPE_LEAF_NODE_BIT;

public abstract class BaseFileIndexStorageManager extends AbstractFileIndexStorageManager {
    protected final IndexHeaderManager indexHeaderManager;
    protected final FileHandlerPool fileHandlerPool;
    public static final String INDEX_FILE_NAME = "index";
    protected final String customName;

    public BaseFileIndexStorageManager(
            @Nullable String customName,
            IndexHeaderManagerFactory indexHeaderManagerFactory,
            EngineConfig engineConfig,
            FileHandlerPool fileHandlerPool
    ) {
        super(engineConfig);
        this.customName = customName;
        this.indexHeaderManager = indexHeaderManagerFactory.getInstance(this.getHeaderPath());
        this.fileHandlerPool = fileHandlerPool;
    }

    public BaseFileIndexStorageManager(
            IndexHeaderManagerFactory indexHeaderManagerFactory,
            EngineConfig engineConfig,
            FileHandlerPool fileHandlerPool
    ) {
        this(null, indexHeaderManagerFactory, engineConfig, fileHandlerPool);
    }

    public BaseFileIndexStorageManager(IndexHeaderManagerFactory indexHeaderManagerFactory, EngineConfig engineConfig) {
        this(null, indexHeaderManagerFactory, engineConfig, new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance()));
    }

    public BaseFileIndexStorageManager(String customName, IndexHeaderManagerFactory indexHeaderManagerFactory, EngineConfig engineConfig) {
        this(customName, indexHeaderManagerFactory, engineConfig, new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance()));
    }

    protected abstract Path getIndexFilePath(int indexId, int chunk);

    protected AsynchronousFileChannel acquireFileChannel(int indexId, int chunk) throws InterruptedException {
        Path indexFilePath = getIndexFilePath(indexId, chunk);
        try {
            return this.fileHandlerPool.getFileChannel(indexFilePath, engineConfig.getFileAcquireTimeout(), engineConfig.getFileAcquireUnit());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void releaseFileChannel(int indexId, int chunk) {
        Path indexFilePath = getIndexFilePath(indexId, chunk);
        this.fileHandlerPool.releaseFileChannel(indexFilePath, engineConfig.getFileCloseTimeout(), engineConfig.getFileCloseUnit());
    }

    protected ManagedFileHandler getManagedFileHandler(int indexId, int chunk){
        Path indexFilePath = getIndexFilePath(indexId, chunk);
        return new ManagedFileHandler(this.fileHandlerPool, indexFilePath,  engineConfig);
    }

    protected abstract IndexHeaderManager.Location getIndexBeginningInChunk(int indexId, int chunk) throws InterruptedException;

    @Override
    public CompletableFuture<Optional<NodeData>> getRoot(int indexId, KVSize kvSize) throws InterruptedException {
        CompletableFuture<Optional<NodeData>> output = new CompletableFuture<>();

        Optional<IndexHeaderManager.Location> optionalRootOfIndex = indexHeaderManager.getRootOfIndex(indexId);
        if (optionalRootOfIndex.isEmpty()){
            output.complete(Optional.empty());
            return output;
        }

        IndexHeaderManager.Location rootLocation = optionalRootOfIndex.get();
        IndexHeaderManager.Location beginningInChunk = getIndexBeginningInChunk(indexId, rootLocation.getChunk());

        FileUtils.readBytes(
                acquireFileChannel(indexId, rootLocation.getChunk()),
                beginningInChunk.getOffset()  + rootLocation.getOffset(),
                this.getBinarySpace(kvSize)
        ).whenComplete((bytes, throwable) -> {
            releaseFileChannel(indexId, rootLocation.getChunk());
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
                            new NodeData(new Pointer(Pointer.TYPE_NODE, rootLocation.getOffset(), rootLocation.getChunk()), bytes)
                    )
            );
        });

        return output;
    }


    @Override
    public CompletableFuture<NodeData> readNode(int indexId, long position, int chunk, KVSize size) throws InterruptedException, IOException {
        CompletableFuture<NodeData> output = new CompletableFuture<>();

        IndexHeaderManager.Location beginningInChunk = getIndexBeginningInChunk(indexId, chunk);

        long filePosition = beginningInChunk.getOffset() + position;

        AsynchronousFileChannel asynchronousFileChannel = acquireFileChannel(indexId, chunk);
        if (asynchronousFileChannel.size() == 0){
            releaseFileChannel(indexId, chunk);
            throw new IOException("Nothing available to read.");
        }

        FileUtils.readBytes(asynchronousFileChannel, filePosition, this.getBinarySpace(size)).whenComplete((bytes, throwable) -> {
            releaseFileChannel(indexId, chunk);
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
    public CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data, boolean isRoot, KVSize kvSize) throws IOException, ExecutionException, InterruptedException {
        CompletableFuture<NodeData> output = new CompletableFuture<>();
        Pointer pointer = this.getAllocatedSpaceForNewNode(indexId, 0, kvSize);
        int binarySpace = this.getBinarySpace(kvSize);
        if (data.length < binarySpace){
            byte[] finalData = new byte[binarySpace];
            System.arraycopy(data, 0, finalData, 0, data.length);
            data = finalData;
        }

        byte[] finalData1 = data;
        long offset = pointer.getPosition();

        IndexHeaderManager.Location indexBeginningInChunk = this.getIndexBeginningInChunk(indexId, pointer.getChunk());

        // setting pointer position according to the index fileOffset. Reading table again since a new chunk may have been created
        pointer.setPosition(offset - indexBeginningInChunk.getOffset());

        FileUtils.write(acquireFileChannel(indexId, pointer.getChunk()), offset, data).whenComplete((size, throwable) -> {
            releaseFileChannel(indexId, pointer.getChunk());

            if (throwable != null){
                output.completeExceptionally(throwable);
                return;
            }

            if (isRoot){
                try {
                    this.updateRoot(indexId, pointer);
                } catch (IOException e) {
                    throw new RuntimeException(
                            String.format(ROOT_UPDATE_RUNTIME_ERR_STR, getHeaderPath(), getIndexFilePath(indexId, pointer.getChunk())),
                            e
                    );
                }
            }

            output.complete(
                    new NodeData(pointer, finalData1)
            );
        });
        return output;
    }


    /**
     * ## How it works:
     *      if the chunk is new for this indexId, then just allocate at the end of the file and add the chunk index to header
     *      and return. But if there isn't space left, try next chunk.
     *      if file size is not 0,
     *          see if there is any empty space in the file (allocated before but never written to) and return the
     *          pointer to that space if is available.
     *      if file size is equal or greater than maximum file size try next chunk
     *      allocate space at end of the file and return pointer if the index is at end of the file
     *      otherwise, allocate space right before the next index in this chunk begins and push next tables to the end
     *          also make sure to update possible roots and chunk indexes fileOffset for next indexes
     * @param indexId index to allocate space in
     * @return Pointer to the beginning of allocated location
     */
    protected abstract Pointer getAllocatedSpaceForNewNode(int indexId, int chunk, KVSize kvSize) throws IOException, ExecutionException, InterruptedException;

    /*
     * Returns the empty position within byte[] passed to the method
     */
    protected Optional<Integer> getPossibleAllocationLocation(byte[] bytes, KVSize kvSize){
        for (int i = 0; i < engineConfig.getBTreeGrowthNodeAllocationCount(); i++){
            int position = i * this.getBinarySpace(kvSize);
            if ((bytes[position] & TYPE_LEAF_NODE_BIT) != TYPE_LEAF_NODE_BIT && (bytes[position] & TYPE_INTERNAL_NODE_BIT) != TYPE_INTERNAL_NODE_BIT){
                return Optional.of(position);
            }
        }
        return Optional.empty();
    }

    @Override
    public CompletableFuture<Void> updateNode(int indexId, byte[] data, Pointer pointer, boolean isRoot) throws InterruptedException, IOException {
        long offset = getIndexBeginningInChunk(indexId, pointer.getChunk()).getOffset() + pointer.getPosition();

        AsynchronousFileChannel asynchronousFileChannel = acquireFileChannel(indexId, pointer.getChunk());

        CompletableFuture<Void> output = new CompletableFuture<>();

        FileUtils.write(asynchronousFileChannel, offset, data).whenComplete((integer, throwable) -> {
            releaseFileChannel(indexId, pointer.getChunk());
            if (isRoot){
                try {
                    this.updateRoot(indexId, pointer);
                } catch (IOException e) {
                    throw new RuntimeException(
                            String.format(ROOT_UPDATE_RUNTIME_ERR_STR, getHeaderPath(), getIndexFilePath(indexId, pointer.getChunk())),
                            e
                    );
                }
            }
            output.complete(null);
        });

        return output;
    }

    private void updateRoot(int indexId, Pointer pointer) throws IOException {
        indexHeaderManager.setRootOfIndex(indexId, IndexHeaderManager.Location.fromPointer(pointer));
    }

    @Override
    public void close() throws IOException {
        this.fileHandlerPool.closeAll(engineConfig.getFileCloseTimeout(), engineConfig.getFileCloseUnit());
    }

    @Override
    public CompletableFuture<Void> removeNode(int indexId, Pointer pointer, KVSize size) throws InterruptedException {
        CompletableFuture<Void> output = new CompletableFuture<>();

        IndexHeaderManager.Location location = indexHeaderManager.getIndexBeginningInChunk(indexId, pointer.getChunk()).orElseGet(() -> new IndexHeaderManager.Location(pointer.getChunk(), 0));
        long offset = location.getOffset() + pointer.getPosition();
        FileUtils.write(acquireFileChannel(indexId, pointer.getChunk()), offset, new byte[this.getBinarySpace(size)]).whenComplete((integer, throwable) -> {
            releaseFileChannel(indexId, pointer.getChunk());
            if (throwable != null){
                output.completeExceptionally(throwable);
            } else {
                output.complete(null);
            }
        });

        return output;
    }

    @Override
    public boolean exists(int indexId) {
        Optional<IndexHeaderManager.Location> rootOfIndex = indexHeaderManager.getRootOfIndex(indexId);
        return rootOfIndex.filter(location -> Files.exists(getIndexFilePath(indexId, location.getChunk()))).isPresent();
    }
}
