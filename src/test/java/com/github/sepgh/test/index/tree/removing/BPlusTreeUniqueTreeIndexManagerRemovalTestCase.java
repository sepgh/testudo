package com.github.sepgh.test.index.tree.removing;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.OrganizedFileIndexStorageManager;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.test.TestParams.DEFAULT_INDEX_BINARY_OBJECT_FACTORY;

public class BPlusTreeUniqueTreeIndexManagerRemovalTestCase extends BaseBPlusTreeUniqueTreeIndexManagerRemovalTestCase {

    protected IndexStorageManager getIndexStorageManager() throws IOException, ExecutionException, InterruptedException {
        return new OrganizedFileIndexStorageManager(
                "test",
                new JsonIndexHeaderManager.Factory(),
                engineConfig,
                new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance())
        );
    }

    protected UniqueTreeIndexManager<Long, Pointer> getIndexManager(IndexStorageManager indexStorageManager) {
        return new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, indexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
    }

    @Test
    public void testRemovingLeftToRight() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {
        IndexStorageManager indexStorageManager = getIndexStorageManager();
        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager = getIndexManager(indexStorageManager);
        super.testRemovingLeftToRight(uniqueTreeIndexManager, indexStorageManager);
    }

    @Test
    public void testRemovingRightToLeft() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {
        IndexStorageManager indexStorageManager = getIndexStorageManager();
        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager = getIndexManager(indexStorageManager);
        super.testRemovingRightToLeft(uniqueTreeIndexManager, indexStorageManager);
    }


    @Test
    public void testRemovingRoot() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {
        IndexStorageManager indexStorageManager = getIndexStorageManager();
        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager = getIndexManager(indexStorageManager);
        super.testRemovingRoot(uniqueTreeIndexManager, indexStorageManager);
    }

}
