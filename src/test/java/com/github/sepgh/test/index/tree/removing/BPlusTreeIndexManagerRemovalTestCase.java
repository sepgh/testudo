package com.github.sepgh.test.index.tree.removing;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.AsyncIndexManagerDecorator;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.index.tree.node.data.LongImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.storage.index.OrganizedFileIndexStorageManager;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class BPlusTreeIndexManagerRemovalTestCase extends BaseBPlusTreeIndexManagerRemovalTestCase {

    protected IndexStorageManager getIndexStorageManager() throws IOException, ExecutionException, InterruptedException {
        return new OrganizedFileIndexStorageManager(
                "test",
                new JsonIndexHeaderManager.Factory(),
                engineConfig,
                new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance())
        );
    }

    protected IndexManager<Long, Pointer> getIndexManager(IndexStorageManager indexStorageManager) {
        return new ClusterBPlusTreeIndexManager<>(1, degree, indexStorageManager, new LongImmutableBinaryObjectWrapper());
    }

    @Test
    public void testRemovingLeftToRight() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {
        IndexStorageManager indexStorageManager = getIndexStorageManager();
        IndexManager<Long, Pointer> indexManager = getIndexManager(indexStorageManager);
        super.testRemovingLeftToRight(indexManager, indexStorageManager);
    }

    @Test
    public void testRemovingRightToLeft() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {
        IndexStorageManager indexStorageManager = getIndexStorageManager();
        IndexManager<Long, Pointer> indexManager = getIndexManager(indexStorageManager);
        super.testRemovingRightToLeft(indexManager, indexStorageManager);
    }


    @Test
    public void testRemovingRoot() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {
        IndexStorageManager indexStorageManager = getIndexStorageManager();
        IndexManager<Long, Pointer> indexManager = getIndexManager(indexStorageManager);
        super.testRemovingRoot(indexManager, indexStorageManager);
    }

    @Test
    @Timeout(2)
    public void testRemovingLeftToRightAsync() throws IOException, ExecutionException, InterruptedException, InternalOperationException {
        IndexStorageManager indexStorageManager = getIndexStorageManager();
        IndexManager<Long, Pointer> indexManager = getIndexManager(indexStorageManager);
        IndexManager<Long, Pointer> asycnIndexManager = new AsyncIndexManagerDecorator<>(indexManager);
        super.testRemovingLeftToRightAsync(asycnIndexManager);
    }

}
