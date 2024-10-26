package com.github.sepgh.test.storage;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.index.data.PointerIndexBinaryObject;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.storage.index.BTreeSizeCalculator;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.OrganizedFileIndexStorageManager;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.index.session.IndexIOSession;
import com.github.sepgh.testudo.storage.index.session.IndexIOSessionFactory;
import com.github.sepgh.testudo.storage.index.session.MemorySnapshotIndexIOSession;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import com.github.sepgh.testudo.utils.KVSize;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.test.TestParams.DEFAULT_INDEX_BINARY_OBJECT_FACTORY;
import static com.github.sepgh.testudo.storage.index.BaseFileIndexStorageManager.INDEX_FILE_NAME;

public class MemorySnapshotIndexIOSessionTestCase {
    private final static KVSize KV_SIZE =  new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES);
    private Path dbPath;
    private EngineConfig engineConfig;
    private int degree = 4;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_MemorySnapshotIndexIOSessionTestCase");
        engineConfig = EngineConfig.builder()
                .baseDBPath(dbPath.toString())
                .bTreeDegree(degree)
                .bTreeGrowthNodeAllocationCount(5)
                .build();
        engineConfig.setBTreeMaxFileSize(20L * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size()));

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

    private OrganizedFileIndexStorageManager getCompactFileIndexStorageManager() throws IOException, ExecutionException, InterruptedException {
        return new OrganizedFileIndexStorageManager(
                "test",
                new JsonIndexHeaderManager.Factory(),
                engineConfig,
                new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance())
        );
    }

    @Timeout(2)
    @Test
    public void testCreateRollback() throws IOException, ExecutionException, InterruptedException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InternalOperationException, IndexExistsException {
        
        IndexStorageManager indexStorageManager = getCompactFileIndexStorageManager();
        final IndexIOSession<Long> indexIOSession = new MemorySnapshotIndexIOSession<>(indexStorageManager, 1, new NodeFactory.ClusterNodeFactory<>(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get()), KV_SIZE);
        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager = new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, indexStorageManager, new IndexIOSessionFactory() {
            @Override
            public <K extends Comparable<K>> IndexIOSession<K> create(IndexStorageManager indexStorageManager, int table, NodeFactory<K> nodeFactory, KVSize kvSize) {
                return (IndexIOSession<K>) indexIOSession;
            }
        }, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());

        for (long i = 1; i < 12; i++){
            uniqueTreeIndexManager.addIndex(i, new Pointer(Pointer.TYPE_DATA, 0, 0));
        }

        Assertions.assertTrue(uniqueTreeIndexManager.getIndex(11L).isPresent());

        uniqueTreeIndexManager.addIndex(12L, new Pointer(Pointer.TYPE_DATA, 0, 0));
        Assertions.assertTrue(uniqueTreeIndexManager.getIndex(12L).isPresent());

        Class<MemorySnapshotIndexIOSession> aClass = MemorySnapshotIndexIOSession.class;
        Method method = aClass.getDeclaredMethod("rollback");
        method.setAccessible(true);
        method.invoke(indexIOSession);

        // Since the same indexIOSession instance shouldn't be used to re-read after rollback we create a new instance
        uniqueTreeIndexManager = new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, indexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
        Assertions.assertFalse(uniqueTreeIndexManager.getIndex(12L).isPresent());

        indexStorageManager.close();
    }

    @Timeout(2)
    @Test
    public void testDeleteRollback() throws IOException, ExecutionException, InterruptedException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InternalOperationException, IndexExistsException {

        IndexStorageManager indexStorageManager = getCompactFileIndexStorageManager();
        final IndexIOSession<Long> indexIOSession = new MemorySnapshotIndexIOSession<>(indexStorageManager, 1, new NodeFactory.ClusterNodeFactory<>(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get()), KV_SIZE);
        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager = new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, indexStorageManager, new IndexIOSessionFactory() {
            @Override
            public <K extends Comparable<K>> IndexIOSession<K> create(IndexStorageManager indexStorageManager, int table, NodeFactory<K> nodeFactory, KVSize kvSize) {
                return (IndexIOSession<K>) indexIOSession;
            }
        }, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager2 = new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, indexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());

        for (long i = 1; i < 13; i++){
            uniqueTreeIndexManager2.addIndex(i, new Pointer(Pointer.TYPE_DATA, 0, 0));
        }

        Assertions.assertTrue(uniqueTreeIndexManager2.getIndex(12L).isPresent());

        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex( 12L));
        Assertions.assertTrue(uniqueTreeIndexManager2.getIndex( 12L).isEmpty());

        Class<MemorySnapshotIndexIOSession> aClass = MemorySnapshotIndexIOSession.class;
        Method method = aClass.getDeclaredMethod("rollback");
        method.setAccessible(true);
        method.invoke(indexIOSession);

        Assertions.assertTrue(uniqueTreeIndexManager2.getIndex(12L).isPresent());

        indexStorageManager.close();
    }


}
