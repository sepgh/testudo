package com.github.sepgh.test.index;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.index.*;
import com.github.sepgh.testudo.index.tree.BPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.data.NoZeroIntegerIndexBinaryObject;
import com.github.sepgh.testudo.index.tree.node.data.PointerIndexBinaryObject;
import com.github.sepgh.testudo.storage.db.DiskPageDatabaseStorageManager;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.OrganizedFileIndexStorageManager;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import com.github.sepgh.testudo.utils.LockableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ListIterator;
import java.util.Optional;

public class DuplicateBPlusTreeIndexManagerBridgeTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private DiskPageDatabaseStorageManager diskPageDatabaseStorageManager;

    @BeforeEach
    public void setUp() throws IOException {
        this.dbPath = Files.createTempDirectory("TEST_BinaryListIteratorTestCase");
        this.engineConfig = EngineConfig.builder()
                .clusterIndexKeyStrategy(EngineConfig.ClusterIndexKeyStrategy.INTEGER)
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

    @Test
    public void test_addRemoveAndIterate() throws Exception {
        UniqueTreeIndexManager<Integer, Pointer> uniqueTreeIndexManager = getIntegerPointerUniqueTreeIndexManager();

        DuplicateIndexManager<Integer, Integer> duplicateIndexManager = new DuplicateBPlusTreeIndexManagerBridge<>(
                1,
                engineConfig,
                uniqueTreeIndexManager,
                new NoZeroIntegerIndexBinaryObject.Factory(),
                diskPageDatabaseStorageManager
        );


        duplicateIndexManager.addIndex(1, 10);
        duplicateIndexManager.addIndex(1, 100);
        duplicateIndexManager.addIndex(1, 1000);
        duplicateIndexManager.addIndex(1, 10000);


        Optional<ListIterator<Integer>> listIteratorOptional = duplicateIndexManager.getIndex(1);
        Assertions.assertTrue(listIteratorOptional.isPresent());


        ListIterator<Integer> integerListIterator = listIteratorOptional.get();
        Assertions.assertTrue(integerListIterator.hasNext());
        Assertions.assertEquals(10, integerListIterator.next());
        Assertions.assertTrue(integerListIterator.hasNext());
        Assertions.assertEquals(100, integerListIterator.next());
        Assertions.assertTrue(integerListIterator.hasNext());
        Assertions.assertEquals(1000, integerListIterator.next());
        Assertions.assertTrue(integerListIterator.hasNext());
        Assertions.assertEquals(10000, integerListIterator.next());

        Assertions.assertFalse(duplicateIndexManager.getIndex(2).isPresent());

        Assertions.assertTrue(duplicateIndexManager.removeIndex(1, 10));
        Assertions.assertFalse(duplicateIndexManager.removeIndex(1, 20));
    }

    @Test
    public void test_iterateKeyValues() throws Exception {
        UniqueTreeIndexManager<Integer, Pointer> uniqueTreeIndexManager = getIntegerPointerUniqueTreeIndexManager();

        DuplicateIndexManager<Integer, Integer> duplicateIndexManager = new DuplicateBPlusTreeIndexManagerBridge<>(
                1,
                engineConfig,
                uniqueTreeIndexManager,
                new NoZeroIntegerIndexBinaryObject.Factory(),
                diskPageDatabaseStorageManager
        );


        duplicateIndexManager.addIndex(1, 10);
        duplicateIndexManager.addIndex(1, 100);
        duplicateIndexManager.addIndex(2, 20);
        duplicateIndexManager.addIndex(2, 200);

        LockableIterator<KeyValue<Integer, ListIterator<Integer>>> sortedIterator = duplicateIndexManager.getSortedIterator();

        Assertions.assertTrue(sortedIterator.hasNext());
        KeyValue<Integer, ListIterator<Integer>> iteratorKeyValue = sortedIterator.next();
        Assertions.assertEquals(1, iteratorKeyValue.key());
        ListIterator<Integer> valuesIterator = iteratorKeyValue.value();
        Assertions.assertTrue(valuesIterator.hasNext());
        Assertions.assertEquals(10, valuesIterator.next());
        Assertions.assertTrue(valuesIterator.hasNext());
        Assertions.assertEquals(100, valuesIterator.next());
        Assertions.assertFalse(valuesIterator.hasNext());

        Assertions.assertTrue(sortedIterator.hasNext());
        iteratorKeyValue = sortedIterator.next();
        Assertions.assertEquals(2, iteratorKeyValue.key());
        valuesIterator = iteratorKeyValue.value();
        Assertions.assertTrue(valuesIterator.hasNext());
        Assertions.assertEquals(20, valuesIterator.next());
        Assertions.assertTrue(valuesIterator.hasNext());
        Assertions.assertEquals(200, valuesIterator.next());
        Assertions.assertFalse(valuesIterator.hasNext());

    }

    private UniqueTreeIndexManager<Integer, Pointer> getIntegerPointerUniqueTreeIndexManager() {
        IndexStorageManager indexStorageManager = new OrganizedFileIndexStorageManager(new JsonIndexHeaderManager.Factory(), engineConfig, new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance()));

        return new BPlusTreeUniqueTreeIndexManager<>(
                1,
                engineConfig.getBTreeDegree(),
                indexStorageManager,
                new NoZeroIntegerIndexBinaryObject.Factory(),
                new PointerIndexBinaryObject.Factory()
        );
    }


}
