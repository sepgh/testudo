package com.github.sepgh.test.index;

import com.github.sepgh.test.TestParams;
import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.index.DuplicateBitmapIndexManager;
import com.github.sepgh.testudo.index.DuplicateIndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.index.data.PointerIndexBinaryObject;
import com.github.sepgh.testudo.index.tree.BPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.serialization.IntegerSerializer;
import com.github.sepgh.testudo.storage.db.DiskPageDatabaseStorageManager;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.OrganizedFileIndexStorageManager;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ListIterator;
import java.util.Optional;

public class DuplicateBitmapIndexManagerTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private DiskPageDatabaseStorageManager diskPageDatabaseStorageManager;

    @BeforeEach
    public void setUp() throws IOException {
        this.dbPath = Files.createTempDirectory("TEST_BinaryListIteratorTestCase");
        this.engineConfig = EngineConfig.builder()
                .baseDBPath(this.dbPath.toString())
                .bTreeDegree(10)
                .build();

        this.diskPageDatabaseStorageManager = new DiskPageDatabaseStorageManager(
                engineConfig,
                new UnlimitedFileHandlerPool(
                        FileHandler.SingletonFileHandlerFactory.getInstance()
                )
        );
    }

    @AfterEach
    public void destroy() throws IOException {
        FileUtils.deleteDirectory(dbPath.toString());
    }

    private UniqueTreeIndexManager<Integer, Pointer> getIntegerPointerUniqueTreeIndexManager() {
        IndexStorageManager indexStorageManager = new OrganizedFileIndexStorageManager(new JsonIndexHeaderManager.Factory(), engineConfig, new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance()));

        return new BPlusTreeUniqueTreeIndexManager<>(
                1,
                engineConfig.getBTreeDegree(),
                indexStorageManager,
                new IntegerSerializer().getIndexBinaryObjectFactory(TestParams.FAKE_FIELD),
                new PointerIndexBinaryObject.Factory()
        );
    }

    @Test
    public void test_addRemoveAndIterate() throws Exception {
        UniqueTreeIndexManager<Integer, Pointer> uniqueTreeIndexManager = getIntegerPointerUniqueTreeIndexManager();

        DuplicateIndexManager<Integer, Integer> duplicateIndexManager = new DuplicateBitmapIndexManager<>(
                1,
                uniqueTreeIndexManager,
                new IntegerSerializer().getIndexBinaryObjectFactory(TestParams.FAKE_FIELD),
                diskPageDatabaseStorageManager
        );

        int[] testItems = new int[]{1,2,3,4,9,10,11,12};

        for (int item : testItems) {
            duplicateIndexManager.addIndex(1, item);
        }


        Optional<ListIterator<Integer>> listIteratorOptional =
                duplicateIndexManager.getIndex(1);

        Assertions.assertTrue(listIteratorOptional.isPresent());
        ListIterator<Integer> integerListIterator = listIteratorOptional.get();

        for (int item : testItems) {
            Assertions.assertTrue(integerListIterator.hasNext());
            Assertions.assertEquals(item, integerListIterator.next());
        }


        Assertions.assertTrue(duplicateIndexManager.removeIndex(1, testItems[0]));

        // Todo: items that dont exist should return false when getting removed
        //       Currently an exception is thrown because doing `off()` on bitmap will extend it's size,
        //       and updating with extended size is not valid
//        Assertions.assertFalse(duplicateIndexManager.removeIndex(1, 10000));


        listIteratorOptional = duplicateIndexManager.getIndex(1);
        integerListIterator = listIteratorOptional.get();
        for (int item : testItems) {
            if (item == 1)
                continue;
            Assertions.assertTrue(integerListIterator.hasNext());
            Assertions.assertEquals(item, integerListIterator.next());
        }


        duplicateIndexManager.purgeIndex();
        listIteratorOptional = duplicateIndexManager.getIndex(1);
        Assertions.assertTrue(listIteratorOptional.isEmpty());

    }
}
