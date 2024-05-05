package com.github.sepgh.internal.tree.storing;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.storage.FileIndexStorageManager;
import com.github.sepgh.internal.storage.InMemoryHeaderManager;
import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;
import com.github.sepgh.internal.tree.BTreeIndexManager;
import com.github.sepgh.internal.tree.IndexManager;
import com.github.sepgh.internal.tree.Pointer;
import com.github.sepgh.internal.tree.node.BaseTreeNode;
import com.github.sepgh.internal.tree.node.InternalTreeNode;
import com.github.sepgh.internal.tree.node.LeafTreeNode;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.internal.storage.FileIndexStorageManager.INDEX_FILE_NAME;

public class BTreeIndexManagerTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private Header header;
    private int order = 3;

    @BeforeEach
    public void setUp() throws IOException {
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
                                        .root(
                                                Header.IndexChunk.builder()
                                                        .chunk(0)
                                                        .offset(0)
                                                        .build()
                                        )
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


    @Test
    @Timeout(value = 2)
    public void addIndex() throws IOException, ExecutionException, InterruptedException {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        FileIndexStorageManager fileIndexStorageManager = new FileIndexStorageManager(dbPath, headerManager, engineConfig);

        IndexManager indexManager = new BTreeIndexManager(order, fileIndexStorageManager);
        BaseTreeNode baseTreeNode = indexManager.addIndex(1, 10, new Pointer(Pointer.TYPE_DATA, 100, 0));

        Assertions.assertTrue(baseTreeNode.isRoot());
        Assertions.assertEquals(0, baseTreeNode.getNodePointer().getPosition());
        Assertions.assertEquals(0, baseTreeNode.getNodePointer().getChunk());

        IndexStorageManager.NodeData nodeData = fileIndexStorageManager.readNode(1, baseTreeNode.getNodePointer()).get();
        LeafTreeNode leafTreeNode = new LeafTreeNode(nodeData.bytes());
        Assertions.assertTrue(leafTreeNode.isRoot());
        Iterator<Map.Entry<Long, Pointer>> entryIterator = leafTreeNode.keyValues();
        Assertions.assertTrue(entryIterator.hasNext());
        Map.Entry<Long, Pointer> pointerEntry = entryIterator.next();
        Assertions.assertEquals(pointerEntry.getKey(), 10);
        Assertions.assertEquals(pointerEntry.getValue().getPosition(), 100);
    }

    @Test
    @Timeout(value = 2)
    public void testSingleSplitAddIndex() throws IOException, ExecutionException, InterruptedException {
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
        Assertions.assertEquals(samplePointer.getPosition(), ((LeafTreeNode) lastTreeNode).keyValues().next().getValue().getPosition());

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
        LeafTreeNode secondChildLeafTreeNode = new LeafTreeNode(fileIndexStorageManager.readNode(1, children.get(1)).get().bytes());
        keyValueList = secondChildLeafTreeNode.keyValueList();
        Assertions.assertEquals(testIdentifiers.get(2), keyValueList.get(0).getKey());
        Assertions.assertEquals(testIdentifiers.get(3), keyValueList.get(1).getKey());

        Assertions.assertTrue(childLeafTreeNode.getNext().isPresent());
        Assertions.assertEquals(children.get(1), childLeafTreeNode.getNext().get());

        Assertions.assertTrue(secondChildLeafTreeNode.getPrevious().isPresent());
        Assertions.assertEquals(children.get(0), secondChildLeafTreeNode.getPrevious().get());

    }

    /**
     *
     * The B+Tree in this test will include numbers from [1-12] added respectively
     * The shape of the tree will be like below, and the test verifies that
     * 007
     * ├── .
     * │   ├── 001
     * │   └── 002
     * ├── 003
     * │   ├── 003
     * │   └── 004
     * ├── 005
     * │   ├── 005
     * │   └── 006
     * ├── .
     * │   ├── 007
     * │   └── 008
     * ├── 009
     * │   ├── 009
     * │   └── 010
     * └── 0011
     *     ├── 011
     *     └── 012
     */
    @Test
    @Timeout(value = 2)
    public void testMultiSplitAddIndex() throws IOException, ExecutionException, InterruptedException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
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
        Assertions.assertEquals(samplePointer.getPosition(), ((LeafTreeNode) lastTreeNode).keyValues().next().getValue().getPosition());

        Optional<IndexStorageManager.NodeData> optional = fileIndexStorageManager.getRoot(1).get();
        Assertions.assertTrue(optional.isPresent());

        BaseTreeNode rootNode = BaseTreeNode.fromBytes(optional.get().bytes());
        Assertions.assertTrue(rootNode.isRoot());
        Assertions.assertFalse(rootNode.isLeaf());

        Assertions.assertEquals(7, rootNode.keys().next());

        // Checking root child at left
        BaseTreeNode leftChildInternalNode = BaseTreeNode.fromBytes(
                fileIndexStorageManager.readNode(
                        1,
                        ((InternalTreeNode) rootNode
                ).getChildAtIndex(0).get()).get().bytes()
        );
        List<Long> leftChildInternalNodeKeys = leftChildInternalNode.keyList();
        List<Pointer> leftChildInternalNodeChildren = ((InternalTreeNode)leftChildInternalNode).childrenList();
        Assertions.assertEquals(2, leftChildInternalNodeKeys.size());
        Assertions.assertEquals(3, leftChildInternalNodeKeys.get(0));
        Assertions.assertEquals(5, leftChildInternalNodeKeys.get(1));

        // Far left leaf
        LeafTreeNode currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                fileIndexStorageManager.readNode(
                        1,
                        leftChildInternalNodeChildren.get(0)
                ).get().bytes()
        );
        List<Long> currentLeafKeys = currentLeaf.keyList();
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(1, currentLeafKeys.get(0));
        Assertions.assertEquals(2, currentLeafKeys.get(1));

        // 2nd Leaf
        Optional<Pointer> nextPointer = currentLeaf.getNext();
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), leftChildInternalNodeChildren.get(1));

        currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                fileIndexStorageManager.readNode(
                        1,
                        leftChildInternalNodeChildren.get(1)
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.keyList();
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(3, currentLeafKeys.get(0));
        Assertions.assertEquals(4, currentLeafKeys.get(1));

        //3rd leaf
        nextPointer = currentLeaf.getNext();
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), leftChildInternalNodeChildren.get(2));  // Todo

        currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                fileIndexStorageManager.readNode(
                        1,
                        leftChildInternalNodeChildren.get(2)
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.keyList();
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(5, currentLeafKeys.get(0));
        Assertions.assertEquals(6, currentLeafKeys.get(1));


        // Checking root child at right
        BaseTreeNode rightChildInternalNode = BaseTreeNode.fromBytes(
                fileIndexStorageManager.readNode(
                        1,
                        ((InternalTreeNode) rootNode
                        ).getChildAtIndex(1).get()).get().bytes()
        );
        List<Long> rightChildInternalNodeKeys = rightChildInternalNode.keyList();
        List<Pointer> rightChildInternalNodeChildren = ((InternalTreeNode)rightChildInternalNode).childrenList();
        Assertions.assertEquals(2, rightChildInternalNodeKeys.size());
        Assertions.assertEquals(9, rightChildInternalNodeKeys.get(0));
        Assertions.assertEquals(11, rightChildInternalNodeKeys.get(1));

        // 4th leaf
        nextPointer = currentLeaf.getNext();
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), rightChildInternalNodeChildren.get(0));

        currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                fileIndexStorageManager.readNode(
                        1,
                        rightChildInternalNodeChildren.get(0)
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.keyList();
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(7, currentLeafKeys.get(0));
        Assertions.assertEquals(8, currentLeafKeys.get(1));


        // 5th leaf
        nextPointer = currentLeaf.getNext();
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), rightChildInternalNodeChildren.get(1));

        currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                fileIndexStorageManager.readNode(
                        1,
                        rightChildInternalNodeChildren.get(1)
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.keyList();
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(9, currentLeafKeys.get(0));
        Assertions.assertEquals(10, currentLeafKeys.get(1));


        // 6th node
        nextPointer = currentLeaf.getNext();
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), rightChildInternalNodeChildren.get(2));

        currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                fileIndexStorageManager.readNode(
                        1,
                        rightChildInternalNodeChildren.get(2)
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.keyList();
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(11, currentLeafKeys.get(0));
        Assertions.assertEquals(12, currentLeafKeys.get(1));

    }

}
