package com.github.sepgh.internal.tree.storing;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.helper.IndexFileDescriptor;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.internal.storage.FileIndexStorageManager.INDEX_FILE_NAME;

/*
*  The purpose of this test case is to assure allocation wouldn't cause issue in multi-table environment
*  Additionally, new chunks should be created and not cause problem
*/
public class MultiTableBTreeIndexManagerAllocationAndChunkTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private Header header;
    private int order = 3;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_MultiTableBTreeIndexManagerAllocationAndChunkTestCase");
        engineConfig = EngineConfig.builder()
                .bTreeNodeMaxKey(order)
                .bTreeGrowthNodeAllocationCount(1)
                .build();
        engineConfig.setBTreeMaxFileSize(4L * 2 * engineConfig.getPaddedSize());

        byte[] writingBytes = new byte[6 * engineConfig.getPaddedSize()];
        Path indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        header = Header.builder()
                .database("sample")
                .tables(
                        Arrays.asList(
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
                                        .build(),
                                Header.Table.builder()
                                        .id(2)
                                        .name("test2")
                                        .chunks(
                                                Collections.singletonList(
                                                        Header.IndexChunk.builder()
                                                                .chunk(0)
                                                                .offset(2L * engineConfig.getPaddedSize())
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
        Assertions.assertTrue(header.getTableOfId(2).isPresent());
        Assertions.assertTrue(header.getTableOfId(2).get().getIndexChunk(0).isPresent());
    }

    @AfterEach
    public void destroy() {
        new File(dbPath.toString()).delete();
    }


    /**
     *
     * The B+Tree in this test will include numbers from [1-12] added respectively
     * The shape of the tree will be like below, and the test verifies that
     * The test validates the tree for 2 tables in same database
     * 007
     * ├── .
     * │   ├── 001   [LEAF NODE 1]
     * │   └── 002   [LEAF NODE 1]
     * ├── 003
     * │   ├── 003   [LEAF NODE 2]
     * │   └── 004   [LEAF NODE 2]
     * ├── 005
     * │   ├── 005   [LEAF NODE 3]
     * │   └── 006   [LEAF NODE 3]
     * ├── .
     * │   ├── 007   [LEAF NODE 4]
     * │   └── 008   [LEAF NODE 4]
     * ├── 009
     * │   ├── 009   [LEAF NODE 5]
     * │   └── 010   [LEAF NODE 5]
     * └── 0011
     *     ├── 011   [LEAF NODE 6]
     *     └── 012   [LEAF NODE 6]
     */
    @Test
    public void testMultiSplitAddIndex() throws IOException, ExecutionException, InterruptedException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        HeaderManager headerManager = new InMemoryHeaderManager(header);
        FileIndexStorageManager fileIndexStorageManager = new FileIndexStorageManager(dbPath, headerManager, engineConfig);
        IndexManager indexManager = new BTreeIndexManager(order, fileIndexStorageManager);


        for (int tableId = 1; tableId <= 2; tableId++){

            for (long testIdentifier : testIdentifiers) {
                indexManager.addIndex(tableId, testIdentifier, samplePointer);
            }

            Optional<IndexStorageManager.NodeData> optional = fileIndexStorageManager.getRoot(tableId).get();
            Assertions.assertTrue(optional.isPresent());
            Assertions.assertTrue(optional.get().pointer().getChunk() != 0);

            BaseTreeNode rootNode = BaseTreeNode.fromBytes(optional.get().bytes());
            Assertions.assertTrue(rootNode.isRoot());
            Assertions.assertFalse(rootNode.isLeaf());

            Assertions.assertEquals(7, rootNode.keys().next());

            // Checking root child at left
            BaseTreeNode leftChildInternalNode = BaseTreeNode.fromBytes(
                    fileIndexStorageManager.readNode(
                            tableId,
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
                            tableId,
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
                            tableId,
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
            Assertions.assertEquals(nextPointer.get(), leftChildInternalNodeChildren.get(2));

            currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                    fileIndexStorageManager.readNode(
                            tableId,
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
                            tableId,
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
                            tableId,
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
                            tableId,
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
                            tableId,
                            rightChildInternalNodeChildren.get(2)
                    ).get().bytes()
            );
            currentLeafKeys = currentLeaf.keyList();
            Assertions.assertEquals(2, currentLeafKeys.size());
            Assertions.assertEquals(11, currentLeafKeys.get(0));
            Assertions.assertEquals(12, currentLeafKeys.get(1));

        }

    }


    @Test
    public void testMultiSplitAddIndexDifferentAddOrders() throws IOException, ExecutionException, InterruptedException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        HeaderManager headerManager = new InMemoryHeaderManager(header);
        FileIndexStorageManager fileIndexStorageManager = new FileIndexStorageManager(dbPath, headerManager, engineConfig);
        IndexManager indexManager = new BTreeIndexManager(order, fileIndexStorageManager);

        IndexFileDescriptor indexFileDescriptor = new IndexFileDescriptor(
                AsynchronousFileChannel.open(
                        Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0)),
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE
                ),
                headerManager,
                engineConfig
        );

        int index = 0;
        int runs = 0;
        while (runs < testIdentifiers.size()){
            indexManager.addIndex(1, testIdentifiers.get(index), samplePointer);
            indexManager.addIndex(2, testIdentifiers.get(index) * 10, samplePointer);
            index++;
            runs++;
            indexFileDescriptor.describe();
        }


        for (int tableId = 1; tableId <= 2; tableId++) {

            int multi = 1;
            if (tableId == 2){
                multi = 10;
            }

            Optional<IndexStorageManager.NodeData> optional = fileIndexStorageManager.getRoot(tableId).get();
            Assertions.assertTrue(optional.isPresent());
            BaseTreeNode rootNode = BaseTreeNode.fromBytes(optional.get().bytes());
            Assertions.assertTrue(rootNode.isRoot());
            Assertions.assertFalse(rootNode.isLeaf());

            Assertions.assertEquals(multi * 7, rootNode.keys().next());
            Assertions.assertTrue(optional.get().pointer().getChunk() != 0);

            // Checking root child at left
            BaseTreeNode leftChildInternalNode = BaseTreeNode.fromBytes(
                    fileIndexStorageManager.readNode(
                            tableId,
                            ((InternalTreeNode) rootNode
                            ).getChildAtIndex(0).get()).get().bytes()
            );
            List<Long> leftChildInternalNodeKeys = leftChildInternalNode.keyList();
            List<Pointer> leftChildInternalNodeChildren = ((InternalTreeNode)leftChildInternalNode).childrenList();
            Assertions.assertEquals( 2, leftChildInternalNodeKeys.size());
            Assertions.assertEquals(multi * 3, leftChildInternalNodeKeys.get(0));
            Assertions.assertEquals(multi * 5, leftChildInternalNodeKeys.get(1));

            // Far left leaf
            LeafTreeNode currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                    fileIndexStorageManager.readNode(
                            tableId,
                            leftChildInternalNodeChildren.get(0)
                    ).get().bytes()
            );
            List<Long> currentLeafKeys = currentLeaf.keyList();
            Assertions.assertEquals(2, currentLeafKeys.size());
            Assertions.assertEquals(multi * 1, currentLeafKeys.get(0));
            Assertions.assertEquals(multi * 2, currentLeafKeys.get(1));

            // 2nd Leaf
            Optional<Pointer> nextPointer = currentLeaf.getNext();
            Assertions.assertTrue(nextPointer.isPresent());
            Assertions.assertEquals(nextPointer.get(), leftChildInternalNodeChildren.get(1));

            currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                    fileIndexStorageManager.readNode(
                            tableId,
                            leftChildInternalNodeChildren.get(1)
                    ).get().bytes()
            );
            currentLeafKeys = currentLeaf.keyList();
            Assertions.assertEquals(2, currentLeafKeys.size());
            Assertions.assertEquals(multi * 3, currentLeafKeys.get(0));
            Assertions.assertEquals(multi * 4, currentLeafKeys.get(1));

            //3rd leaf
            nextPointer = currentLeaf.getNext();
            Assertions.assertTrue(nextPointer.isPresent());
            Assertions.assertEquals(nextPointer.get(), leftChildInternalNodeChildren.get(2));

            currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                    fileIndexStorageManager.readNode(
                            tableId,
                            leftChildInternalNodeChildren.get(2)
                    ).get().bytes()
            );
            currentLeafKeys = currentLeaf.keyList();
            Assertions.assertEquals(2, currentLeafKeys.size());
            Assertions.assertEquals(multi * 5, currentLeafKeys.get(0));
            Assertions.assertEquals(multi * 6, currentLeafKeys.get(1));


            // Checking root child at right
            BaseTreeNode rightChildInternalNode = BaseTreeNode.fromBytes(
                    fileIndexStorageManager.readNode(
                            tableId,
                            ((InternalTreeNode) rootNode
                            ).getChildAtIndex(1).get()).get().bytes()
            );
            List<Long> rightChildInternalNodeKeys = rightChildInternalNode.keyList();
            List<Pointer> rightChildInternalNodeChildren = ((InternalTreeNode)rightChildInternalNode).childrenList();
            Assertions.assertEquals(2, rightChildInternalNodeKeys.size());
            Assertions.assertEquals(multi * 9, rightChildInternalNodeKeys.get(0));
            Assertions.assertEquals(multi * 11, rightChildInternalNodeKeys.get(1));

            // 4th leaf
            nextPointer = currentLeaf.getNext();
            Assertions.assertTrue(nextPointer.isPresent());
            Assertions.assertEquals(nextPointer.get(), rightChildInternalNodeChildren.get(0));

            currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                    fileIndexStorageManager.readNode(
                            tableId,
                            rightChildInternalNodeChildren.get(0)
                    ).get().bytes()
            );
            currentLeafKeys = currentLeaf.keyList();
            Assertions.assertEquals(2, currentLeafKeys.size());
            Assertions.assertEquals(multi * 7, currentLeafKeys.get(0));
            Assertions.assertEquals(multi * 8, currentLeafKeys.get(1));


            // 5th leaf
            nextPointer = currentLeaf.getNext();
            Assertions.assertTrue(nextPointer.isPresent());
            Assertions.assertEquals(nextPointer.get(), rightChildInternalNodeChildren.get(1));

            currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                    fileIndexStorageManager.readNode(
                            tableId,
                            rightChildInternalNodeChildren.get(1)
                    ).get().bytes()
            );
            currentLeafKeys = currentLeaf.keyList();
            Assertions.assertEquals(2, currentLeafKeys.size());
            Assertions.assertEquals(multi * 9, currentLeafKeys.get(0));
            Assertions.assertEquals(multi * 10, currentLeafKeys.get(1));


            // 6th node
            nextPointer = currentLeaf.getNext();
            Assertions.assertTrue(nextPointer.isPresent());
            Assertions.assertEquals(nextPointer.get(), rightChildInternalNodeChildren.get(2));

            currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                    fileIndexStorageManager.readNode(
                            tableId,
                            rightChildInternalNodeChildren.get(2)
                    ).get().bytes()
            );
            currentLeafKeys = currentLeaf.keyList();
            Assertions.assertEquals(2, currentLeafKeys.size());
            Assertions.assertEquals(multi * 11, currentLeafKeys.get(0));
            Assertions.assertEquals(multi * 12, currentLeafKeys.get(1));

        }

    }


    /**
     *
     * The B+Tree in this test will include numbers from [1-12] added respectively
     * The shape of the tree will be like below, and the test verifies that
     * The test validates the tree for 2 tables in same database
     * 009
     * ├── .
     * │   ├── 001
     * │   └── 002
     * ├── 003
     * │   ├── 003
     * │   ├── 004
     * │   └── 005
     * ├── 006
     * │   ├── 006
     * │   ├── 007
     * │   └── 008
     * ├── .
     * │   ├── 009
     * │   └── 010
     * └── 011
     *     ├── 011
     *     └── 012
     */
    @Test
    public void testMultiSplitAddIndex2() throws IOException, ExecutionException, InterruptedException {

        List<Long> testIdentifiers = Arrays.asList(1L, 4L, 9L, 6L, 10L, 8L, 3L, 2L, 11L, 5L, 7L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (int tableId = 1; tableId <= 2; tableId++){
            HeaderManager headerManager = new InMemoryHeaderManager(header);
            FileIndexStorageManager fileIndexStorageManager = new FileIndexStorageManager(dbPath, headerManager, engineConfig);
            IndexManager indexManager = new BTreeIndexManager(order, fileIndexStorageManager);


            for (long testIdentifier : testIdentifiers) {
                indexManager.addIndex(tableId, testIdentifier, samplePointer);
            }

            Optional<IndexStorageManager.NodeData> optional = fileIndexStorageManager.getRoot(tableId).get();
            Assertions.assertTrue(optional.isPresent());
            Assertions.assertTrue(optional.get().pointer().getChunk() != 0);


            BaseTreeNode rootNode = BaseTreeNode.fromBytes(optional.get().bytes());
            Assertions.assertTrue(rootNode.isRoot());
            Assertions.assertFalse(rootNode.isLeaf());

            Assertions.assertEquals(9, rootNode.keys().next());

            // Checking root child at left
            BaseTreeNode leftChildInternalNode = BaseTreeNode.fromBytes(
                    fileIndexStorageManager.readNode(
                            tableId,
                            ((InternalTreeNode) rootNode
                            ).getChildAtIndex(0).get()).get().bytes()
            );
            List<Long> leftChildInternalNodeKeys = leftChildInternalNode.keyList();
            List<Pointer> leftChildInternalNodeChildren = ((InternalTreeNode)leftChildInternalNode).childrenList();
            Assertions.assertEquals(2, leftChildInternalNodeKeys.size());
            Assertions.assertEquals(3, leftChildInternalNodeKeys.get(0));
            Assertions.assertEquals(6, leftChildInternalNodeKeys.get(1));


            // Far left leaf
            LeafTreeNode currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                    fileIndexStorageManager.readNode(
                            tableId,
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
                            tableId,
                            leftChildInternalNodeChildren.get(1)
                    ).get().bytes()
            );
            currentLeafKeys = currentLeaf.keyList();
            Assertions.assertEquals(3, currentLeafKeys.size());
            Assertions.assertEquals(3, currentLeafKeys.get(0));
            Assertions.assertEquals(4, currentLeafKeys.get(1));
            Assertions.assertEquals(5, currentLeafKeys.get(2));


            //3rd leaf
            nextPointer = currentLeaf.getNext();
            Assertions.assertTrue(nextPointer.isPresent());
            Assertions.assertEquals(nextPointer.get(), leftChildInternalNodeChildren.get(2));

            currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                    fileIndexStorageManager.readNode(
                            tableId,
                            leftChildInternalNodeChildren.get(2)
                    ).get().bytes()
            );
            currentLeafKeys = currentLeaf.keyList();
            Assertions.assertEquals(3, currentLeafKeys.size());
            Assertions.assertEquals(6, currentLeafKeys.get(0));
            Assertions.assertEquals(7, currentLeafKeys.get(1));
            Assertions.assertEquals(8, currentLeafKeys.get(2));


            // Checking root child at right
            BaseTreeNode rightChildInternalNode = BaseTreeNode.fromBytes(
                    fileIndexStorageManager.readNode(
                            tableId,
                            ((InternalTreeNode) rootNode
                            ).getChildAtIndex(1).get()).get().bytes()
            );
            List<Long> rightChildInternalNodeKeys = rightChildInternalNode.keyList();
            List<Pointer> rightChildInternalNodeChildren = ((InternalTreeNode) rightChildInternalNode).childrenList();
            Assertions.assertEquals(1, rightChildInternalNodeKeys.size());
            Assertions.assertEquals(11, rightChildInternalNodeKeys.get(0));


            // 4th leaf
            nextPointer = currentLeaf.getNext();
            Assertions.assertTrue(nextPointer.isPresent());
            Assertions.assertEquals(nextPointer.get(), rightChildInternalNodeChildren.get(0));

            currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                    fileIndexStorageManager.readNode(
                            tableId,
                            rightChildInternalNodeChildren.get(0)
                    ).get().bytes()
            );
            currentLeafKeys = currentLeaf.keyList();
            Assertions.assertEquals(2, currentLeafKeys.size());
            Assertions.assertEquals(9, currentLeafKeys.get(0));
            Assertions.assertEquals(10, currentLeafKeys.get(1));


            // 5th leaf
            nextPointer = currentLeaf.getNext();
            Assertions.assertTrue(nextPointer.isPresent());
            Assertions.assertEquals(nextPointer.get(), rightChildInternalNodeChildren.get(1));
            currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                    fileIndexStorageManager.readNode(
                            tableId,
                            rightChildInternalNodeChildren.get(1)
                    ).get().bytes()
            );
            currentLeafKeys = currentLeaf.keyList();
            Assertions.assertEquals(2, currentLeafKeys.size());
            Assertions.assertEquals(11, currentLeafKeys.get(0));
            Assertions.assertEquals(12, currentLeafKeys.get(1));

        }

    }

}
