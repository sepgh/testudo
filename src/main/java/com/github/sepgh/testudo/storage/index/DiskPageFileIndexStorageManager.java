package com.github.sepgh.testudo.storage.index;

import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.exception.VerificationException;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManager;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManagerFactory;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.FileHandlerPool;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import com.github.sepgh.testudo.ds.KVSize;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class DiskPageFileIndexStorageManager extends AbstractFileIndexStorageManager {
    protected final IndexHeaderManager indexHeaderManager;
    protected final FileHandlerPool fileHandlerPool;
    protected final DatabaseStorageManager databaseStorageManager;
    private final int INDEX_SCHEME_ID = -1;

    public static final int VERSION = 1;

    public DiskPageFileIndexStorageManager(EngineConfig engineConfig, IndexHeaderManagerFactory indexHeaderManagerFactory, FileHandlerPool fileHandlerPool, DatabaseStorageManager databaseStorageManager) {
        super(engineConfig);
        this.indexHeaderManager = indexHeaderManagerFactory.getInstance(this.getHeaderPath());
        this.fileHandlerPool = fileHandlerPool;
        this.databaseStorageManager = databaseStorageManager;
    }

    public DiskPageFileIndexStorageManager(EngineConfig engineConfig, IndexHeaderManagerFactory indexHeaderManagerFactory, DatabaseStorageManager databaseStorageManager) {
        this(engineConfig, indexHeaderManagerFactory, new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance(engineConfig.getFileHandlerPoolThreads())), databaseStorageManager);
    }

    public DiskPageFileIndexStorageManager(EngineConfig engineConfig, DatabaseStorageManager databaseStorageManager) {
        this(engineConfig, new JsonIndexHeaderManager.Factory(), databaseStorageManager);
    }

    @Override
    public CompletableFuture<Optional<NodeData>> getRoot(int indexId, KVSize kvSize) throws InterruptedException {
        CompletableFuture<Optional<NodeData>> output = new CompletableFuture<>();

        Optional<IndexHeaderManager.Location> optionalRootOfIndex = indexHeaderManager.getRootOfIndex(indexId);
        if (optionalRootOfIndex.isEmpty()){
            output.complete(Optional.empty());
            return output;
        }

        Pointer pointer = optionalRootOfIndex.get().toPointer(Pointer.TYPE_NODE);
        Optional<DBObject> dbObjectOptional = this.databaseStorageManager.select(pointer);
        if (dbObjectOptional.isEmpty()){
            output.complete(Optional.empty());
            return output;
        }

        output.complete(
                Optional.of(
                        new NodeData(pointer, dbObjectOptional.get().getData())
                )
        );

        return output;
    }

    @Override
    public CompletableFuture<NodeData> readNode(int indexId, long position, int chunk, KVSize kvSize) throws InterruptedException, IOException {
        CompletableFuture<NodeData> output = new CompletableFuture<>();
        Pointer pointer = new Pointer(Pointer.TYPE_NODE, position, chunk);
        Optional<DBObject> dbObjectOptional = this.databaseStorageManager.select(pointer);

        if (dbObjectOptional.isEmpty()){
            output.completeExceptionally(new Throwable("Could not read node data at " + pointer));
            return output;
        }

        output.complete(new NodeData(pointer, dbObjectOptional.get().getData()));
        return output;
    }

    private void updateRoot(int indexId, Pointer pointer) throws IOException {
        indexHeaderManager.setRootOfIndex(indexId, IndexHeaderManager.Location.fromPointer(pointer));
    }

    @Override
    public CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data, boolean isRoot, KVSize size) throws IOException, ExecutionException, InterruptedException {
        CompletableFuture<NodeData> output = new CompletableFuture<>();
        Pointer pointer = this.databaseStorageManager.store(INDEX_SCHEME_ID, indexId, VERSION, data);
        pointer.setType(Pointer.TYPE_NODE);
        output.complete(new NodeData(
                pointer,
                data
        ));
        if (isRoot){
            try {
                this.updateRoot(indexId, pointer);
            } catch (IOException e) {
                throw new RuntimeException("Could not update root data but node is committed! DB is broken unless you manually update root of index %d to %s".formatted(indexId, pointer.toString()), e);
            }
        }
        return output;
    }

    @Override
    public CompletableFuture<Void> updateNode(int indexId, byte[] data, Pointer pointer, boolean isRoot) throws IOException, InterruptedException {
        CompletableFuture<Void> output = new CompletableFuture<>();
        AtomicBoolean flag = new AtomicBoolean(false);
        try {
            this.databaseStorageManager.update(pointer, dbObject -> {
                try {
                    dbObject.modifyData(data);
                    flag.set(true);
                } catch (VerificationException.InvalidDBObjectWrapper e) {
                    output.completeExceptionally(e);
                }
            });

            if (flag.get()) {
                if (isRoot){
                    try {
                        this.updateRoot(indexId, pointer);
                    } catch (IOException e) {
                        throw new RuntimeException("Could not update root data but node is committed! DB is broken unless you manually update root of index %d to %s".formatted(indexId, pointer.toString()), e);
                    }
                }
                output.complete(null);
            }

        } catch (ExecutionException e) {
            output.completeExceptionally(e);
        }

        return output;
    }

    @Override
    public void close() throws IOException {
        this.databaseStorageManager.close();
    }

    @Override
    public CompletableFuture<Void> removeNode(int indexId, Pointer pointer, KVSize size) throws InterruptedException {
        CompletableFuture<Void> output = new CompletableFuture<>();

        try {
            this.databaseStorageManager.remove(pointer);
            output.complete(null);
        } catch (IOException | ExecutionException e) {
            output.completeExceptionally(e);
        }
        return output;
    }

    @Override
    public boolean exists(int indexId) {
        return true;
    }
}
