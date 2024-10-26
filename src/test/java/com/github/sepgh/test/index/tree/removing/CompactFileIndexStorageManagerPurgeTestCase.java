package com.github.sepgh.test.index.tree.removing;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.index.tree.BPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.storage.index.CompactFileIndexStorageManager;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.github.sepgh.test.TestParams.DEFAULT_INDEX_BINARY_OBJECT_FACTORY;

public class CompactFileIndexStorageManagerPurgeTestCase extends BaseBPlusTreeUniqueTreeIndexManagerRemovalTestCase {

    private IndexStorageManager getIndexStorageManager() {
        return new CompactFileIndexStorageManager(
                new JsonIndexHeaderManager.Factory(),
                engineConfig,
                new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance())
        );
    }

    private UniqueTreeIndexManager<Long, Long> getIndexManager(IndexStorageManager indexStorageManager) {
        return new BPlusTreeUniqueTreeIndexManager<>(1, degree, indexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get(), DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
    }

    @Test
    public void testPurge() throws IndexExistsException, InternalOperationException {
        IndexStorageManager indexStorageManager = getIndexStorageManager();

        UniqueTreeIndexManager<Long, Long> indexManager = getIndexManager(indexStorageManager);

        for (long i = 1; i < 11; i++) {
            indexManager.addIndex(i, i);
        }

        indexManager.purgeIndex();

        for (long i = 1; i < 10L * degree * BPlusTreeUniqueTreeIndexManager.PURGE_ITERATION_MULTIPLIER; i++) {
            Assertions.assertFalse(indexManager.getIndex(i).isPresent(), "%d is present while it shouldn't be!".formatted(i));
        }

    }

}
