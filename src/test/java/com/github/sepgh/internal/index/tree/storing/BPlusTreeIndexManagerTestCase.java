package com.github.sepgh.internal.index.tree.storing;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.index.IndexManager;
import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.BaseTreeNode;
import com.github.sepgh.internal.index.tree.node.InternalTreeNode;
import com.github.sepgh.internal.index.tree.node.LeafTreeNode;
import com.github.sepgh.internal.storage.CompactFileIndexStorageManager;
import com.github.sepgh.internal.storage.InMemoryHeaderManager;
import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;
import com.github.sepgh.internal.index.tree.BPlusTreeIndexManager;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.internal.storage.CompactFileIndexStorageManager.INDEX_FILE_NAME;

public class BPlusTreeIndexManagerTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private Header header;
    private int degree = 4;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_BTreeIndexManagerTestCase");
        engineConfig = EngineConfig.builder()
                .bTreeDegree(degree)
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
        CompactFileIndexStorageManager compactFileIndexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig);

        IndexManager indexManager = new BPlusTreeIndexManager(degree, compactFileIndexStorageManager);
        BaseTreeNode baseTreeNode = indexManager.addIndex(1, 10, new Pointer(Pointer.TYPE_DATA, 100, 0));

        Assertions.assertTrue(baseTreeNode.isRoot());
        Assertions.assertEquals(0, baseTreeNode.getPointer().getPosition());
        Assertions.assertEquals(0, baseTreeNode.getPointer().getChunk());

        IndexStorageManager.NodeData nodeData = compactFileIndexStorageManager.readNode(1, baseTreeNode.getPointer()).get();
        LeafTreeNode leafTreeNode = new LeafTreeNode(nodeData.bytes());
        Assertions.assertTrue(leafTreeNode.isRoot());
        Iterator<LeafTreeNode.KeyValue> entryIterator = leafTreeNode.getKeyValues(degree);
        Assertions.assertTrue(entryIterator.hasNext());
        LeafTreeNode.KeyValue pointerEntry = entryIterator.next();
        Assertions.assertEquals(pointerEntry.key(), 10);
        Assertions.assertEquals(pointerEntry.value().getPosition(), 100);
    }

    @Test
    @Timeout(value = 2)
    public void testSingleSplitAddIndex() throws IOException, ExecutionException, InterruptedException {
        Random random = new Random();

        List<Long> testIdentifiers = new ArrayList<>(degree);
        int i = 0;
        while (testIdentifiers.size() < degree){
            long l = random.nextLong() % 100;
            if (!testIdentifiers.contains(l)){
                testIdentifiers.add(l);
                i++;
            }
        }

        Assertions.assertEquals(degree, i);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);


        HeaderManager headerManager = new InMemoryHeaderManager(header);
        CompactFileIndexStorageManager compactFileIndexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig);
        IndexManager indexManager = new BPlusTreeIndexManager(degree, compactFileIndexStorageManager);


        BaseTreeNode lastTreeNode = null;
        for (Long testIdentifier : testIdentifiers) {
            lastTreeNode = indexManager.addIndex(1, testIdentifier, samplePointer);
        }

        Assertions.assertTrue(lastTreeNode.isLeaf());
        Assertions.assertEquals(2, lastTreeNode.getKeyList(degree).size());
        Assertions.assertEquals(samplePointer.getPosition(), ((LeafTreeNode) lastTreeNode).getKeyValues(degree).next().value().getPosition());

        Optional<IndexStorageManager.NodeData> optional = compactFileIndexStorageManager.getRoot(1).get();
        Assertions.assertTrue(optional.isPresent());

        BaseTreeNode rootNode = BaseTreeNode.fromBytes(optional.get().bytes());
        Assertions.assertTrue(rootNode.isRoot());
        Assertions.assertFalse(rootNode.isLeaf());

        Assertions.assertEquals(1, rootNode.getKeyList(degree).size());

        testIdentifiers.sort(Long::compareTo);

        List<InternalTreeNode.ChildPointers> children = ((InternalTreeNode) rootNode).getChildPointersList(degree);
        Assertions.assertEquals(1, children.size());
        Assertions.assertNotNull(children.get(0).getLeft());
        Assertions.assertNotNull(children.get(0).getRight());
        Assertions.assertEquals(testIdentifiers.get(2), rootNode.getKeyList(degree).get(0));

        // First child
        LeafTreeNode childLeafTreeNode = new LeafTreeNode(compactFileIndexStorageManager.readNode(1, children.get(0).getLeft()).get().bytes());
        List<LeafTreeNode.KeyValue> keyValueList = childLeafTreeNode.getKeyValueList(degree);
        Assertions.assertEquals(testIdentifiers.get(0), keyValueList.get(0).key());
        Assertions.assertEquals(testIdentifiers.get(1), keyValueList.get(1).key());

        //Second child
        LeafTreeNode secondChildLeafTreeNode = new LeafTreeNode(compactFileIndexStorageManager.readNode(1, children.get(0).getRight()).get().bytes());
        keyValueList = secondChildLeafTreeNode.getKeyValueList(degree);
        Assertions.assertEquals(testIdentifiers.get(2), keyValueList.get(0).key());
        Assertions.assertEquals(testIdentifiers.get(3), keyValueList.get(1).key());

        Assertions.assertTrue(childLeafTreeNode.getNextSiblingPointer(degree).isPresent());
        Assertions.assertEquals(children.get(0).getRight(), childLeafTreeNode.getNextSiblingPointer(degree).get());

        Assertions.assertTrue(secondChildLeafTreeNode.getPreviousSiblingPointer(degree).isPresent());
        Assertions.assertEquals(children.get(0).getLeft(), secondChildLeafTreeNode.getPreviousSiblingPointer(degree).get());

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
        CompactFileIndexStorageManager compactFileIndexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig);
        IndexManager indexManager = new BPlusTreeIndexManager(degree, compactFileIndexStorageManager);


        BaseTreeNode lastTreeNode = null;
        for (Long testIdentifier : testIdentifiers) {
            lastTreeNode = indexManager.addIndex(1, testIdentifier, samplePointer);
        }

        Assertions.assertTrue(lastTreeNode.isLeaf());
        Assertions.assertEquals(2, lastTreeNode.getKeyList(degree).size());
        Assertions.assertEquals(samplePointer.getPosition(), ((LeafTreeNode) lastTreeNode).getKeyValues(degree).next().value().getPosition());

        StoredTreeStructureVerifier.testOrderedTreeStructure(compactFileIndexStorageManager, 1, 1, degree);

    }

}
