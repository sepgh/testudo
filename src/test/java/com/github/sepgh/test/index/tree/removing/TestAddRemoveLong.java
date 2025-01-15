package com.github.sepgh.test.index.tree.removing;

import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.index.tree.BPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.OrganizedFileIndexStorageManager;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.test.TestParams.LONG_INDEX_BINARY_OBJECT_FACTORY;

public class TestAddRemoveLong extends BaseBPlusTreeUniqueTreeIndexManagerRemovalTestCase {

    protected IndexStorageManager getIndexStorageManager() {
        return new OrganizedFileIndexStorageManager(
                "test",
                new JsonIndexHeaderManager.SingletonFactory(),
                engineConfig,
                new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance())
        );
    }

    private UniqueTreeIndexManager<Long, Long> getIndexManager(IndexStorageManager indexStorageManager) {
        return new BPlusTreeUniqueTreeIndexManager<>(1, degree, indexStorageManager, LONG_INDEX_BINARY_OBJECT_FACTORY.get(), LONG_INDEX_BINARY_OBJECT_FACTORY.get());
    }

    @Test
    public void test() throws IOException, ExecutionException, InterruptedException, InternalOperationException {
        IndexStorageManager indexStorageManager = getIndexStorageManager();
        UniqueTreeIndexManager<Long, Long> indexManager = getIndexManager(indexStorageManager);

        for (long i = 1; i < 11; i++) {
            indexManager.addIndex(i, i);
        }

        for (long i = 1; i < 11; i++) {
            indexManager.removeIndex(i);
        }

    }

}
