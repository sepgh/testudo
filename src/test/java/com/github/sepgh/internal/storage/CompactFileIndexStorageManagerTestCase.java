package com.github.sepgh.internal.storage;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.AbstractTreeNode;
import com.github.sepgh.internal.index.tree.node.InternalTreeNode;
import com.github.sepgh.internal.index.tree.node.NodeFactory;
import com.github.sepgh.internal.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.data.NodeInnerObj;
import com.github.sepgh.internal.index.tree.node.data.PointerInnerObject;
import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;
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

import static com.github.sepgh.internal.index.tree.node.AbstractTreeNode.*;
import static com.github.sepgh.internal.index.tree.node.AbstractTreeNode.Type.INTERNAL;
import static com.github.sepgh.internal.storage.CompactFileIndexStorageManager.INDEX_FILE_NAME;

public class CompactFileIndexStorageManagerTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private Header header;
    private final byte[] singleKeyLeafNodeRepresentation = {
            ((byte) (0x00 | ROOT_BIT | TYPE_LEAF_NODE_BIT)), // Leaf

            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F,  // Key 1

            // >> Start pointer to child 1
            Pointer.TYPE_DATA,  // type
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,  // position
            0x00, 0x00, 0x00, 0x01, // chunk
            // >> End pointer to child 1
    };
    private final byte[] singleKeyInternalNodeRepresentation = {
            ((byte) (0x00 | ROOT_BIT | TYPE_INTERNAL_NODE_BIT)), // Not leaf

            // >> Start pointer to child 1
            Pointer.TYPE_NODE,  // type
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,  // position
            0x00, 0x00, 0x00, 0x01, // chunk
            // >> End pointer to child 1

            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F,  // Key

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
                .bTreeDegree(1)
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
        NodeFactory.ClusterNodeFactory<Long> nodeFactory = new NodeFactory.ClusterNodeFactory<>(NodeInnerObj.Strategy.LONG);


        CompactFileIndexStorageManager compactFileIndexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig);
        try {
            CompletableFuture<IndexStorageManager.NodeData> future = compactFileIndexStorageManager.readNode(1, 0, 0);

            IndexStorageManager.NodeData nodeData = future.get();
            Assertions.assertEquals(engineConfig.getPaddedSize(), nodeData.bytes().length);

            AbstractTreeNode<Long> treeNode = nodeFactory.fromBytes(nodeData.bytes());

            Iterator<Long> keys = treeNode.getKeys(2, PointerInnerObject.BYTES);

            Assertions.assertTrue(treeNode.isRoot());
            Assertions.assertTrue(keys.hasNext());
            Assertions.assertEquals(15, keys.next());


            future = compactFileIndexStorageManager.readNode(1, engineConfig.getPaddedSize(), 0);
            nodeData = future.get();
            Assertions.assertEquals(engineConfig.getPaddedSize(), nodeData.bytes().length);

            treeNode = nodeFactory.fromBytes(nodeData.bytes());

            Iterator<InternalTreeNode.ChildPointers<Long>> children = ((InternalTreeNode<Long>) treeNode).getChildPointers(2);

            Assertions.assertTrue(children.hasNext());
            InternalTreeNode.ChildPointers<Long> childPointers = children.next();
            Assertions.assertTrue(childPointers.getLeft().isNodePointer());
            Assertions.assertEquals(1, childPointers.getLeft().getPosition());
            Assertions.assertEquals(1, childPointers.getLeft().getChunk());
        } finally {
            compactFileIndexStorageManager.close();
        }

    }

    @Test
    public void canReadAndUpdateNodeSuccessfully() throws IOException, ExecutionException, InterruptedException {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        NodeFactory.ClusterNodeFactory<Long> nodeFactory = new NodeFactory.ClusterNodeFactory<>(NodeInnerObj.Strategy.LONG);


        CompactFileIndexStorageManager compactFileIndexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig);
        try {
            CompletableFuture<IndexStorageManager.NodeData> future = compactFileIndexStorageManager.readNode(1, 0, 0);

            IndexStorageManager.NodeData nodeData = future.get();
            Assertions.assertEquals(engineConfig.getPaddedSize(), nodeData.bytes().length);

            LeafClusterTreeNode<Long> leafTreeNode = (LeafClusterTreeNode) nodeFactory.fromBytes(nodeData.bytes());
            leafTreeNode.setKeyValue(0, new LeafClusterTreeNode.KeyValue(10L, new Pointer(Pointer.TYPE_DATA, 100, 100)));

            compactFileIndexStorageManager.updateNode(1, leafTreeNode.getData(), nodeData.pointer()).get();

            future = compactFileIndexStorageManager.readNode(1, 0, 0);
            nodeData = future.get();
            leafTreeNode = (LeafClusterTreeNode<Long>) nodeFactory.fromBytes(nodeData.bytes());

            Assertions.assertTrue(leafTreeNode.getKeyValues(2).hasNext());

            Assertions.assertEquals(10L, leafTreeNode.getKeyValueList(2).get(0).key());
            Assertions.assertEquals(100, leafTreeNode.getKeyValueList(2).get(0).value().getChunk());
            Assertions.assertEquals(100, leafTreeNode.getKeyValueList(2).get(0).value().getPosition());

        } finally {
            compactFileIndexStorageManager.close();
        }
    }

    @Test
    public void canWriteNewNodeAndAllocate() throws IOException, ExecutionException, InterruptedException {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        NodeFactory.ClusterNodeFactory<Long> nodeFactory = new NodeFactory.ClusterNodeFactory<>(NodeInnerObj.Strategy.LONG);

        CompactFileIndexStorageManager compactFileIndexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig);
        try {

            byte[] emptyNode = compactFileIndexStorageManager.getEmptyNode();
            AbstractTreeNode<Long> baseClusterTreeNode = nodeFactory.fromBytes(emptyNode, INTERNAL);
            baseClusterTreeNode.setAsRoot();

            IndexStorageManager.NodeData nodeData = compactFileIndexStorageManager.writeNewNode(1, baseClusterTreeNode.getData()).get();
            Assertions.assertEquals(engineConfig.getPaddedSize(), nodeData.bytes().length);
            Assertions.assertEquals(2L * engineConfig.getPaddedSize(), nodeData.pointer().getPosition());
            Assertions.assertEquals(0, nodeData.pointer().getChunk());

            nodeData = compactFileIndexStorageManager.writeNewNode(1, baseClusterTreeNode.getData()).get();
            Assertions.assertEquals(engineConfig.getPaddedSize(), nodeData.bytes().length);
            Assertions.assertEquals(0, nodeData.pointer().getPosition());
            Assertions.assertEquals(1, nodeData.pointer().getChunk());

            nodeData = compactFileIndexStorageManager.writeNewNode(1, baseClusterTreeNode.getData()).get();
            Assertions.assertEquals(engineConfig.getPaddedSize(), nodeData.bytes().length);
            Assertions.assertEquals(engineConfig.getPaddedSize(), nodeData.pointer().getPosition());
            Assertions.assertEquals(1, nodeData.pointer().getChunk());

        } finally {
            compactFileIndexStorageManager.close();
        }
    }

}
