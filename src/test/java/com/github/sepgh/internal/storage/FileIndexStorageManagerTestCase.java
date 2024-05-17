package com.github.sepgh.internal.storage;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;
import com.github.sepgh.internal.tree.Pointer;
import com.github.sepgh.internal.tree.node.BaseTreeNode;
import com.github.sepgh.internal.tree.node.InternalTreeNode;
import com.github.sepgh.internal.tree.node.LeafTreeNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.internal.storage.FileIndexStorageManager.INDEX_FILE_NAME;

public class FileIndexStorageManagerTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private Header header;
    private final byte[] singleKeyLeafNodeRepresentation = {
            ((byte) (0x00 | BaseTreeNode.ROOT_BIT | BaseTreeNode.TYPE_LEAF_NODE_BIT)), // Leaf

            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F,  // Key 1

            // >> Start pointer to child 1
            Pointer.TYPE_DATA,  // type
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,  // position
            0x00, 0x00, 0x00, 0x01, // chunk
            // >> End pointer to child 1
    };
    private final byte[] singleKeyInternalNodeRepresentation = {
            ((byte) (0x00 | BaseTreeNode.ROOT_BIT | BaseTreeNode.TYPE_INTERNAL_NODE_BIT)), // Not leaf

            // >> Start pointer to child 1
            Pointer.TYPE_NODE,  // type
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,  // position
            0x00, 0x00, 0x00, 0x01, // chunk
            // >> End pointer to child 1

            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F,  // Key

            // >> Start pointer to child 2
            Pointer.TYPE_NODE,  // type
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,  // position
            0x00, 0x00, 0x00, 0x02, // chunk
            // >> End pointer to child 2
    };

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_IndexFileManagerTestCase");
        engineConfig = EngineConfig.builder()
                .bTreeNodeMaxKey(1)
                .bTreeGrowthNodeAllocationCount(1)
                .build();
        engineConfig.setBTreeMaxFileSize(3L * engineConfig.getPaddedSize());

        byte[] writingBytes = new byte[engineConfig.getPaddedSize() * 2];
        System.arraycopy(singleKeyLeafNodeRepresentation, 0, writingBytes, 0, singleKeyLeafNodeRepresentation.length);
        System.arraycopy(singleKeyInternalNodeRepresentation, 0, writingBytes, engineConfig.getPaddedSize(), singleKeyInternalNodeRepresentation.length);
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
    public void canReadNodeSuccessfully() throws ExecutionException, InterruptedException, IOException {

        HeaderManager headerManager = new InMemoryHeaderManager(header);

        FileIndexStorageManager fileIndexStorageManager = new FileIndexStorageManager(dbPath, headerManager, engineConfig);
        try {
            CompletableFuture<IndexStorageManager.NodeData> future = fileIndexStorageManager.readNode(1, 0, 0);

            IndexStorageManager.NodeData nodeData = future.get();
            Assertions.assertEquals(engineConfig.getPaddedSize(), nodeData.bytes().length);

            BaseTreeNode treeNode = BaseTreeNode.fromBytes(nodeData.bytes());

            Iterator<Long> keys = treeNode.getKeys(2);

            Assertions.assertTrue(treeNode.isRoot());
            Assertions.assertTrue(keys.hasNext());
            Assertions.assertEquals(15, keys.next());


            future = fileIndexStorageManager.readNode(1, engineConfig.getPaddedSize(), 0);
            nodeData = future.get();
            Assertions.assertEquals(engineConfig.getPaddedSize(), nodeData.bytes().length);

            treeNode = BaseTreeNode.fromBytes(nodeData.bytes());

            Iterator<InternalTreeNode.KeyPointers> children = ((InternalTreeNode) treeNode).getKeyPointers(2);

            Assertions.assertTrue(children.hasNext());
            InternalTreeNode.KeyPointers keyPointers = children.next();
            Assertions.assertTrue(keyPointers.getLeft().isNodePointer());
            Assertions.assertEquals(1, keyPointers.getLeft().getPosition());
            Assertions.assertEquals(1, keyPointers.getLeft().getChunk());
        } finally {
            fileIndexStorageManager.close();
        }

    }

    @Test
    public void canReadAndUpdateNodeSuccessfully() throws IOException, ExecutionException, InterruptedException {
        HeaderManager headerManager = new InMemoryHeaderManager(header);

        FileIndexStorageManager fileIndexStorageManager = new FileIndexStorageManager(dbPath, headerManager, engineConfig);
        try {
            CompletableFuture<IndexStorageManager.NodeData> future = fileIndexStorageManager.readNode(1, 0, 0);

            IndexStorageManager.NodeData nodeData = future.get();
            Assertions.assertEquals(engineConfig.getPaddedSize(), nodeData.bytes().length);

            LeafTreeNode leafTreeNode = (LeafTreeNode) BaseTreeNode.fromBytes(nodeData.bytes());
            leafTreeNode.setKeyValue(0, new LeafTreeNode.KeyValue(10L, new Pointer(Pointer.TYPE_DATA, 100, 100)));

            fileIndexStorageManager.updateNode(1, leafTreeNode.getData(), nodeData.pointer()).get();

            future = fileIndexStorageManager.readNode(1, 0, 0);
            nodeData = future.get();
            leafTreeNode = (LeafTreeNode) BaseTreeNode.fromBytes(nodeData.bytes());

            Assertions.assertTrue(leafTreeNode.getKeyValues(2).hasNext());

            Assertions.assertEquals(10L, leafTreeNode.getKeyValueList(2).get(0).key());
            Assertions.assertEquals(100, leafTreeNode.getKeyValueList(2).get(0).value().getChunk());
            Assertions.assertEquals(100, leafTreeNode.getKeyValueList(2).get(0).value().getPosition());

        } finally {
            fileIndexStorageManager.close();
        }
    }

    @Test
    public void canWriteNewNodeAndAllocate() throws IOException, ExecutionException, InterruptedException {
        HeaderManager headerManager = new InMemoryHeaderManager(header);

        FileIndexStorageManager fileIndexStorageManager = new FileIndexStorageManager(dbPath, headerManager, engineConfig);
        try {

            byte[] emptyNode = fileIndexStorageManager.getEmptyNode();
            BaseTreeNode baseTreeNode = BaseTreeNode.fromBytes(emptyNode, BaseTreeNode.Type.INTERNAL);
            baseTreeNode.setAsRoot();

            IndexStorageManager.NodeData nodeData = fileIndexStorageManager.writeNewNode(1, baseTreeNode.getData()).get();
            Assertions.assertEquals(engineConfig.getPaddedSize(), nodeData.bytes().length);
            Assertions.assertEquals(2L * engineConfig.getPaddedSize(), nodeData.pointer().getPosition());
            Assertions.assertEquals(0, nodeData.pointer().getChunk());

            nodeData = fileIndexStorageManager.writeNewNode(1, baseTreeNode.getData()).get();
            Assertions.assertEquals(engineConfig.getPaddedSize(), nodeData.bytes().length);
            Assertions.assertEquals(0, nodeData.pointer().getPosition());
            Assertions.assertEquals(1, nodeData.pointer().getChunk());

            nodeData = fileIndexStorageManager.writeNewNode(1, baseTreeNode.getData()).get();
            Assertions.assertEquals(engineConfig.getPaddedSize(), nodeData.bytes().length);
            Assertions.assertEquals(engineConfig.getPaddedSize(), nodeData.pointer().getPosition());
            Assertions.assertEquals(1, nodeData.pointer().getChunk());

        } finally {
            fileIndexStorageManager.close();
        }
    }

}
