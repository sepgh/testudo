package com.github.sepgh.test.storage;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.index.tree.node.data.LongImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.storage.BTreeSizeCalculator;
import com.github.sepgh.testudo.storage.CompactFileIndexStorageManager;
import com.github.sepgh.testudo.storage.IndexStorageManager;
import com.github.sepgh.testudo.storage.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import com.github.sepgh.testudo.storage.session.IndexIOSession;
import com.github.sepgh.testudo.storage.session.IndexIOSessionFactory;
import com.github.sepgh.testudo.storage.session.MemorySnapshotIndexIOSession;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.testudo.storage.BaseFileIndexStorageManager.INDEX_FILE_NAME;

public class MemorySnapshotIndexIOSessionTestCase {

    private Path dbPath;
    private EngineConfig engineConfig;
    private int degree = 4;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_MemorySnapshotIndexIOSessionTestCase");
        engineConfig = EngineConfig.builder()
                .bTreeDegree(degree)
                .bTreeGrowthNodeAllocationCount(5)
                .build();
        engineConfig.setBTreeMaxFileSize(20L * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));

        byte[] writingBytes = new byte[]{};
        Path indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    @AfterEach
    public void destroy() throws IOException {
        Path indexPath0 = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.delete(indexPath0);
        try {
            Path indexPath1 = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
            Files.delete(indexPath1);
        } catch (NoSuchFileException ignored){}
    }

    private CompactFileIndexStorageManager getCompactFileIndexStorageManager() throws IOException, ExecutionException, InterruptedException {
        return new CompactFileIndexStorageManager(
                dbPath,
                "test",
                new JsonIndexHeaderManager.Factory(),
                engineConfig,
                new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance())
        );
    }

    @Timeout(2)
    @Test
    public void testCreateRollback() throws IOException, ExecutionException, InterruptedException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, InternalOperationException, IndexExistsException {
        
        IndexStorageManager indexStorageManager = getCompactFileIndexStorageManager();
        final IndexIOSession<Long> indexIOSession = new MemorySnapshotIndexIOSession<>(indexStorageManager, 1, new NodeFactory.ClusterNodeFactory<>(new LongImmutableBinaryObjectWrapper()));
        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new IndexIOSessionFactory() {
            @Override
            public <K extends Comparable<K>> IndexIOSession<K> create(IndexStorageManager indexStorageManager, int table, NodeFactory<K> nodeFactory) {
                return (IndexIOSession<K>) indexIOSession;
            }
        }, new LongImmutableBinaryObjectWrapper());

        for (long i = 1; i < 12; i++){
            indexManager.addIndex(1, i, Pointer.empty());
        }

        Assertions.assertTrue(indexManager.getIndex(1, 11L).isPresent());

        indexManager.addIndex(1, 12L, Pointer.empty());
        Assertions.assertTrue(indexManager.getIndex(1, 12L).isPresent());

        Class<MemorySnapshotIndexIOSession> aClass = MemorySnapshotIndexIOSession.class;
        Method method = aClass.getDeclaredMethod("rollback");
        method.setAccessible(true);
        method.invoke(indexIOSession);

        // Since the same indexIOSession instance shouldn't be used to re-read after rollback we create a new instance
        indexManager = new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new LongImmutableBinaryObjectWrapper());
        Assertions.assertFalse(indexManager.getIndex(1, 12L).isPresent());

        indexStorageManager.close();
    }

    @Timeout(2)
    @Test
    public void testDeleteRollback() throws IOException, ExecutionException, InterruptedException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, InternalOperationException, IndexExistsException {
        
        IndexStorageManager indexStorageManager = getCompactFileIndexStorageManager();
        final IndexIOSession<Long> indexIOSession = new MemorySnapshotIndexIOSession<>(indexStorageManager, 1, new NodeFactory.ClusterNodeFactory<>(new LongImmutableBinaryObjectWrapper()));
        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new IndexIOSessionFactory() {
            @Override
            public <K extends Comparable<K>> IndexIOSession<K> create(IndexStorageManager indexStorageManager, int table, NodeFactory<K> nodeFactory) {
                return (IndexIOSession<K>) indexIOSession;
            }
        }, new LongImmutableBinaryObjectWrapper());
        IndexManager<Long, Pointer> indexManager2 = new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new LongImmutableBinaryObjectWrapper());

        for (long i = 1; i < 13; i++){
            indexManager2.addIndex(1, i, Pointer.empty());
        }

        Assertions.assertTrue(indexManager2.getIndex(1, 12L).isPresent());

        Assertions.assertTrue(indexManager.removeIndex(1, 12L));
        Assertions.assertTrue(indexManager2.getIndex(1, 12L).isEmpty());

        Class<MemorySnapshotIndexIOSession> aClass = MemorySnapshotIndexIOSession.class;
        Method method = aClass.getDeclaredMethod("rollback");
        method.setAccessible(true);
        method.invoke(indexIOSession);

        Assertions.assertTrue(indexManager2.getIndex(1, 12L).isPresent());

        indexStorageManager.close();
    }


}
