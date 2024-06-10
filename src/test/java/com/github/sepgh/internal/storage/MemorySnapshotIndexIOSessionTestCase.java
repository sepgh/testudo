package com.github.sepgh.internal.storage;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.index.IndexManager;
import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.NodeFactory;
import com.github.sepgh.internal.index.tree.node.cluster.ClusterBPlusTreeIndexManager;
import com.github.sepgh.internal.index.tree.node.data.NodeInnerObj;
import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;
import com.github.sepgh.internal.storage.session.IndexIOSession;
import com.github.sepgh.internal.storage.session.IndexIOSessionFactory;
import com.github.sepgh.internal.storage.session.MemorySnapshotIndexIOSession;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.internal.storage.BaseFileIndexStorageManager.INDEX_FILE_NAME;

public class MemorySnapshotIndexIOSessionTestCase {

    private Path dbPath;
    private EngineConfig engineConfig;
    private Header header;
    private int degree = 4;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_MemorySnapshotIndexIOSessionTestCase");
        engineConfig = EngineConfig.builder()
                .bTreeDegree(degree)
                .bTreeGrowthNodeAllocationCount(5)
                .build();
        engineConfig.setBTreeMaxFileSize(20L * engineConfig.getPaddedSize());

        byte[] writingBytes = new byte[]{};
        Path indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        header = Header.builder()
                .database("sample")
                .tables(
                        Collections.singletonList(
                                Header.Table.builder()
                                        .id(1)
                                        .name("test")
                                        .chunks(
                                                Collections.singletonList(
                                                        Header.IndexChunk.builder()
                                                                .chunk(0)
                                                                .offset(0)
                                                                .build()
                                                )
                                        ).root(new Header.IndexChunk(0, 0L))
                                        .initialized(true)
                                        .build()
                        )
                )
                .build();

        Assertions.assertTrue(header.getTableOfId(1).isPresent());
        Assertions.assertTrue(header.getTableOfId(1).get().getIndexChunk(0).isPresent());
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


    @Timeout(2)
    @Test
    public void testCreateRollback() throws IOException, ExecutionException, InterruptedException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        HeaderManager headerManager = new InMemoryHeaderManager(header);

        IndexStorageManager indexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig);
        final IndexIOSession<Long> indexIOSession = new MemorySnapshotIndexIOSession<>(indexStorageManager, 1, new NodeFactory.ClusterNodeFactory<>(NodeInnerObj.Strategy.LONG));
        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new IndexIOSessionFactory() {
            @Override
            public <K extends Comparable<K>> IndexIOSession<K> create(IndexStorageManager indexStorageManager, int table, NodeFactory<K> nodeFactory) {
                return (IndexIOSession<K>) indexIOSession;
            }
        }, NodeInnerObj.Strategy.LONG);

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
        indexManager = new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, NodeInnerObj.Strategy.LONG);
        Assertions.assertFalse(indexManager.getIndex(1, 12L).isPresent());

    }

    @Timeout(2)
    @Test
    public void testDeleteRollback() throws IOException, ExecutionException, InterruptedException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        HeaderManager headerManager = new InMemoryHeaderManager(header);

        IndexStorageManager indexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig);
        final IndexIOSession<Long> indexIOSession = new MemorySnapshotIndexIOSession<>(indexStorageManager, 1, new NodeFactory.ClusterNodeFactory<>(NodeInnerObj.Strategy.LONG));
        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new IndexIOSessionFactory() {
            @Override
            public <K extends Comparable<K>> IndexIOSession<K> create(IndexStorageManager indexStorageManager, int table, NodeFactory<K> nodeFactory) {
                return (IndexIOSession<K>) indexIOSession;
            }
        }, NodeInnerObj.Strategy.LONG);
        IndexManager<Long, Pointer> indexManager2 = new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, NodeInnerObj.Strategy.LONG);

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
    }


}
