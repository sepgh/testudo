package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.storage.FileIndexStorageManager;
import com.github.sepgh.internal.storage.InMemoryHeaderManager;
import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;
import com.github.sepgh.internal.tree.exception.IllegalNodeAccess;
import com.github.sepgh.internal.tree.node.BaseTreeNode;
import com.github.sepgh.internal.tree.node.InternalTreeNode;
import com.github.sepgh.internal.tree.node.LeafTreeNode;
import com.google.common.io.BaseEncoding;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.internal.storage.FileIndexStorageManager.INDEX_FILE_NAME;

public class BTreeIndexManagerTestCase {
    private static Path dbPath;
    private static EngineConfig engineConfig;
    private static Header header;
    private static int order = 3;

    @BeforeAll
    public static void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_BTreeIndexManagerTestCase");
        engineConfig = EngineConfig.builder()
                .bTreeNodeMaxKey(order)
                .bTreeGrowthNodeAllocationCount(2)
                .build();
        engineConfig.setBTreeMaxFileSize(4L * engineConfig.getPaddedSize());

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
                                        )
                                        .initialized(true)
                                        .build()
                        )
                )
                .build();

        Assertions.assertTrue(header.getTableOfId(1).isPresent());
        Assertions.assertTrue(header.getTableOfId(1).get().getIndexChunk(0).isPresent());
    }

    @AfterAll
    public static void destroy() throws IOException {
        Path indexPath0 = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.delete(indexPath0);
        try {
            Path indexPath1 = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
            Files.delete(indexPath1);
        } catch (NoSuchFileException ignored){}
    }


    @Test
    public void addIndex() throws IOException, ExecutionException, InterruptedException, IllegalNodeAccess {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        FileIndexStorageManager fileIndexStorageManager = new FileIndexStorageManager(dbPath, headerManager, engineConfig);

        IndexManager indexManager = new BTreeIndexManager(order, fileIndexStorageManager);
        BaseTreeNode baseTreeNode = indexManager.addIndex(1, 10, new Pointer(Pointer.TYPE_DATA, 100, 0));

        Assertions.assertTrue(baseTreeNode.isRoot());
        Assertions.assertEquals(0, baseTreeNode.getNodePointer().position());
        Assertions.assertEquals(0, baseTreeNode.getNodePointer().chunk());

        IndexStorageManager.NodeData nodeData = fileIndexStorageManager.readNode(1, baseTreeNode.getNodePointer()).get();
        LeafTreeNode leafTreeNode = new LeafTreeNode(nodeData.bytes());
        Assertions.assertTrue(leafTreeNode.isRoot());
        Iterator<Map.Entry<Long, Pointer>> entryIterator = leafTreeNode.keyValues();
        Assertions.assertTrue(entryIterator.hasNext());
        Map.Entry<Long, Pointer> pointerEntry = entryIterator.next();
        Assertions.assertEquals(pointerEntry.getKey(), 10);
        Assertions.assertEquals(pointerEntry.getValue().position(), 100);
    }

    @Test
    public void testSingleSplitAddIndex() throws IOException, ExecutionException, InterruptedException, IllegalNodeAccess {
        Random random = new Random();

        List<Long> testIdentifiers = new ArrayList<>(order + 1);
        int i = 0;
        for (;i <= order; i++){
            testIdentifiers.add(random.nextLong() % 100);
        }

        Assertions.assertEquals(order+1, i);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);


        HeaderManager headerManager = new InMemoryHeaderManager(header);
        FileIndexStorageManager fileIndexStorageManager = new FileIndexStorageManager(dbPath, headerManager, engineConfig);
        IndexManager indexManager = new BTreeIndexManager(order, fileIndexStorageManager);


        BaseTreeNode lastTreeNode = null;
        for (Long testIdentifier : testIdentifiers) {
            lastTreeNode = indexManager.addIndex(1, testIdentifier, samplePointer);
        }

        Assertions.assertTrue(lastTreeNode.isLeaf());
        Assertions.assertEquals(2, lastTreeNode.keyList().size());
        Assertions.assertEquals(samplePointer.position(), ((LeafTreeNode) lastTreeNode).keyValues().next().getValue().position());

        Optional<IndexStorageManager.NodeData> optional = fileIndexStorageManager.getRoot(1).get();
        Assertions.assertTrue(optional.isPresent());

        BaseTreeNode rootNode = BaseTreeNode.fromBytes(optional.get().bytes());
        Assertions.assertTrue(rootNode.isRoot());
        Assertions.assertFalse(rootNode.isLeaf());

        Assertions.assertEquals(1, rootNode.keyList().size());

        testIdentifiers.sort(Long::compareTo);

        List<Pointer> children = ((InternalTreeNode) rootNode).childrenList();
        Assertions.assertEquals(2, children.size());
        Assertions.assertEquals(testIdentifiers.get(2), rootNode.keyList().get(0));

        // First child
        LeafTreeNode childLeafTreeNode = new LeafTreeNode(fileIndexStorageManager.readNode(1, children.get(0)).get().bytes());
        List<Map.Entry<Long, Pointer>> keyValueList = childLeafTreeNode.keyValueList();
        Assertions.assertEquals(testIdentifiers.get(0), keyValueList.get(0).getKey());
        Assertions.assertEquals(testIdentifiers.get(1), keyValueList.get(1).getKey());
        //Second child
        childLeafTreeNode = new LeafTreeNode(fileIndexStorageManager.readNode(1, children.get(1)).get().bytes());
        keyValueList = childLeafTreeNode.keyValueList();
        Assertions.assertEquals(testIdentifiers.get(2), keyValueList.get(0).getKey());
        Assertions.assertEquals(testIdentifiers.get(3), keyValueList.get(1).getKey());


    }

}
