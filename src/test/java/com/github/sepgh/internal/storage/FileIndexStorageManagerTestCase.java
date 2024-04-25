package com.github.sepgh.internal.storage;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.storage.exception.ChunkIsFullException;
import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;
import com.github.sepgh.internal.tree.Pointer;
import com.github.sepgh.internal.tree.node.AbstractTreeNode;
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
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.github.sepgh.internal.storage.FileIndexStorageManager.INDEX_FILE_NAME;

public class FileIndexStorageManagerTestCase {
    private static Path dbPath;
    private static EngineConfig engineConfig;
    private static Header header;
    private static final byte[] singleKeyLeafNodeRepresentation = {
            AbstractTreeNode.TYPE_LEAF_NODE, // Leaf

            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F,  // Key 1

            // >> Start pointer to child 1
            Pointer.TYPE_DATA,  // type
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,  // position
            0x00, 0x00, 0x00, 0x01, // chunk
            // >> End pointer to child 1
    };
    private static final byte[] singleKeyInternalNodeRepresentation = {
            AbstractTreeNode.TYPE_INTERNAL_NODE, // Not leaf

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

    @BeforeAll
    public static void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_IndexFileManagerTestCase");
        engineConfig = EngineConfig.builder()
                .bTreeNodeMaxKey(1)
                .bTreeGrowthNodeAllocationCount(2)
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
    public void canReadNodeSuccessfully() throws ExecutionException, InterruptedException {

        HeaderManager headerManager = new InMemoryHeaderManager(header);

        FileIndexStorageManager fileIndexStorageManager = new FileIndexStorageManager(dbPath, engineConfig, headerManager);
        try {
            Future<byte[]> future = fileIndexStorageManager.readNode(1, 0, 0);

            byte[] bytes = future.get();
            System.out.println(BaseEncoding.base16().lowerCase().encode(bytes));
            Assertions.assertEquals(engineConfig.getPaddedSize(), bytes.length);

            AbstractTreeNode treeNode = new LeafTreeNode(bytes);

            Iterator<Long> keys = treeNode.keys();

            Assertions.assertTrue(keys.hasNext());
            Assertions.assertEquals(15, keys.next());


            future = fileIndexStorageManager.readNode(1, engineConfig.getPaddedSize(), 0);
            bytes = future.get();
            System.out.println(BaseEncoding.base16().lowerCase().encode(bytes));
            Assertions.assertEquals(engineConfig.getPaddedSize(), bytes.length);

            treeNode = new InternalTreeNode(bytes);

            Iterator<Pointer> children = ((InternalTreeNode) treeNode).children();

            Assertions.assertTrue(children.hasNext());
            Pointer pointer = children.next();
            Assertions.assertTrue(pointer.isNodePointer());
            Assertions.assertEquals(1, pointer.position());
            Assertions.assertEquals(1, pointer.chunk());
        } finally {
            fileIndexStorageManager.close();
        }

    }

    @Test
    public void canAllocateSpaceAndWriteSuccessfully() throws IOException, ExecutionException, InterruptedException {
        HeaderManager headerManager = new InMemoryHeaderManager(header);

        FileIndexStorageManager fileIndexStorageManager = new FileIndexStorageManager(dbPath, engineConfig, headerManager);
        try {
            /* First allocation would add to end of the file since there is no space, but we didn't reach max */
            Future<IndexStorageManager.AllocationResult> allocationResultFuture = fileIndexStorageManager.allocateForNewNode(1, 0);
            IndexStorageManager.AllocationResult allocationResult = allocationResultFuture.get();

            Assertions.assertEquals(2L * engineConfig.getPaddedSize(), allocationResult.position());

            /* Second allocation call should return the same, since we didnt fill the area */
            allocationResultFuture = fileIndexStorageManager.allocateForNewNode(1, 0);
            allocationResult = allocationResultFuture.get();

            Assertions.assertEquals(2L * engineConfig.getPaddedSize(), allocationResult.position());


            /* Write a node */
            Future<Integer> writeFuture =fileIndexStorageManager.writeNode(1, singleKeyLeafNodeRepresentation, allocationResult.position(), 0);
            writeFuture.get();

            /* Third allocation call should return next spot */
            allocationResultFuture = fileIndexStorageManager.allocateForNewNode(1, 0);
            allocationResult = allocationResultFuture.get();
            Assertions.assertEquals(3L * engineConfig.getPaddedSize(), allocationResult.position());


            /* Write a second node */
            writeFuture =fileIndexStorageManager.writeNode(1, singleKeyLeafNodeRepresentation, allocationResult.position(), 0);
            writeFuture.get();

            /* Forth allocation call should throw exception since there is no space left in the chunk */
            Assertions.assertThrows(ChunkIsFullException.class, () -> {
               fileIndexStorageManager.allocateForNewNode(1, 0);
            });

            Assertions.assertFalse(fileIndexStorageManager.chunkHasSpaceForNode(0));

        } catch (ChunkIsFullException e) {
            throw new RuntimeException(e);
        } finally {
            fileIndexStorageManager.close();
        }
    }

}
