package com.github.sepgh.internal.index.tree;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.exception.IndexExistsException;
import com.github.sepgh.internal.exception.InternalOperationException;
import com.github.sepgh.internal.index.IndexManager;
import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.internal.index.tree.node.AbstractTreeNode;
import com.github.sepgh.internal.index.tree.node.InternalTreeNode;
import com.github.sepgh.internal.index.tree.node.NodeFactory;
import com.github.sepgh.internal.index.tree.node.cluster.ClusterBPlusTreeIndexManager;
import com.github.sepgh.internal.index.tree.node.data.*;
import com.github.sepgh.internal.storage.*;
import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.internal.index.tree.node.AbstractTreeNode.TYPE_LEAF_NODE_BIT;
import static com.github.sepgh.internal.storage.BaseFileIndexStorageManager.INDEX_FILE_NAME;

public class ImmutableBinaryObjectWrapperTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private Header header;
    private int degree = 4;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_BinaryObjectWrapperTestCase");
        engineConfig = EngineConfig.builder()
                .bTreeDegree(degree)
                .bTreeGrowthNodeAllocationCount(2)
                .build();
        engineConfig.setBTreeMaxFileSize(4L * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));

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
    }

    @Test
    public void test_IntegerIdentifier() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        CompactFileIndexStorageManager compactFileIndexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig, BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));

        IndexManager<Integer, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, compactFileIndexStorageManager, new IntegerImmutableBinaryObjectWrapper());

        for (int i = 0; i < 13; i ++){
            indexManager.addIndex(1, i, Pointer.empty());
        }

        for (int i = 0; i < 13; i ++){
            Assertions.assertTrue(indexManager.getIndex(1, i).isPresent());
        }

        for (int i = 0; i < 13; i ++){
            Assertions.assertTrue(indexManager.removeIndex(1, i));
        }

        for (int i = 0; i < 13; i ++){
            Assertions.assertFalse(indexManager.getIndex(1, i).isPresent());
        }

    }
    @Test
    public void test_NoZeroIntegerIdentifier() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, InternalOperationException, IndexExistsException {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        CompactFileIndexStorageManager compactFileIndexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig, BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));

        IndexManager<Integer, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, compactFileIndexStorageManager, new NoZeroIntegerImmutableBinaryObjectWrapper());

        Assertions.assertThrows(ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue.class, () -> {
            indexManager.addIndex(1, 0, Pointer.empty());
        });

        for (int i = 1; i < 13; i ++){
            indexManager.addIndex(1, i, Pointer.empty());
        }

        for (int i = 1; i < 13; i ++){
            Assertions.assertTrue(indexManager.getIndex(1, i).isPresent());
        }

        for (int i = 1; i < 13; i ++){
            Assertions.assertTrue(indexManager.removeIndex(1, i));
        }

        for (int i = 1; i < 13; i ++){
            Assertions.assertFalse(indexManager.getIndex(1, i).isPresent());
        }

    }

    @Test
    public void test_NoZeroLongIdentifier() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, InternalOperationException, IndexExistsException {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        CompactFileIndexStorageManager compactFileIndexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig, BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));

        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, compactFileIndexStorageManager, new NoZeroLongImmutableBinaryObjectWrapper());

        Assertions.assertThrows(ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue.class, () -> {
            indexManager.addIndex(1, 0L, Pointer.empty());
        });

        for (long i = 1; i < 13; i ++){
            indexManager.addIndex(1, i, Pointer.empty());
        }

        for (long i = 1; i < 13; i ++){
            Assertions.assertTrue(indexManager.getIndex(1, i).isPresent());
        }

        for (long i = 1; i < 13; i ++){
            Assertions.assertTrue(indexManager.removeIndex(1, i));
        }

        for (long i = 1; i < 13; i ++){
            Assertions.assertFalse(indexManager.getIndex(1, i).isPresent());
        }

    }

    @Test
    public void test_CustomBinaryObjectWrapper() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        CompactFileIndexStorageManager compactFileIndexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig, new BTreeSizeCalculator(degree, 20, Pointer.BYTES).calculate());

        ImmutableBinaryObjectWrapper<String> keyImmutableBinaryObjectWrapper = new StringImmutableBinaryObjectWrapper();

        NodeFactory<String> nodeFactory = new NodeFactory<>() {
            @Override
            public AbstractTreeNode<String> fromBytes(byte[] bytes) {
                if ((bytes[0] & TYPE_LEAF_NODE_BIT) == TYPE_LEAF_NODE_BIT)
                    return new AbstractLeafTreeNode<>(bytes, keyImmutableBinaryObjectWrapper, new PointerImmutableBinaryObjectWrapper());
                return new InternalTreeNode<>(bytes, keyImmutableBinaryObjectWrapper);
            }

            @Override
            public AbstractTreeNode<String> fromBytes(byte[] bytes, AbstractTreeNode.Type type) {
                if (type.equals(AbstractTreeNode.Type.LEAF))
                    return new AbstractLeafTreeNode<>(bytes, keyImmutableBinaryObjectWrapper, new PointerImmutableBinaryObjectWrapper());
                return new InternalTreeNode<>(bytes, keyImmutableBinaryObjectWrapper);
            }
        };


        IndexManager<String, Pointer> indexManager = new BPlusTreeIndexManager<>(degree, compactFileIndexStorageManager, keyImmutableBinaryObjectWrapper, new PointerImmutableBinaryObjectWrapper(), nodeFactory);


        indexManager.addIndex(1, "AAA", Pointer.empty());
        indexManager.addIndex(1, "BBB", Pointer.empty());
        indexManager.addIndex(1, "CAB", Pointer.empty());
        indexManager.addIndex(1, "AAC", Pointer.empty());
        indexManager.addIndex(1, "BAC", Pointer.empty());
        indexManager.addIndex(1, "CAA", Pointer.empty());

        indexManager.addIndex(1, "AAB", Pointer.empty());
        indexManager.addIndex(1, "AAD", Pointer.empty());

        indexManager.addIndex(1, "ABA", Pointer.empty());
        indexManager.addIndex(1, "ABB", Pointer.empty());
        indexManager.addIndex(1, "ABC", Pointer.empty());

        indexManager.addIndex(1, "ACA", Pointer.empty());
        indexManager.addIndex(1, "ACB", Pointer.empty());
        indexManager.addIndex(1, "ACC", Pointer.empty());

        indexManager.addIndex(1, "BAA", Pointer.empty());
        indexManager.addIndex(1, "BAB", Pointer.empty());
        indexManager.addIndex(1, "BBA", Pointer.empty());
        indexManager.addIndex(1, "BBC", Pointer.empty());


        IndexStorageManager.NodeData rootNodeData = compactFileIndexStorageManager.getRoot(1).get().get();
        InternalTreeNode<String> rootInternalTreeNode = new InternalTreeNode<>(rootNodeData.bytes(), keyImmutableBinaryObjectWrapper);
        rootInternalTreeNode.setPointer(rootNodeData.pointer());


        InternalTreeNode<String> internalNode1 = (InternalTreeNode<String>) IndexTreeNodeIO.read(compactFileIndexStorageManager, 1, rootInternalTreeNode.getChildrenList().getFirst(), nodeFactory);

        AbstractLeafTreeNode<String, Pointer> leaf = (AbstractLeafTreeNode<String, Pointer>) IndexTreeNodeIO.read(compactFileIndexStorageManager, 1, internalNode1.getChildrenList().getFirst(), nodeFactory);
        Assertions.assertEquals("AAA", leaf.getKeyList(degree).getFirst());
        Assertions.assertEquals("AAB", leaf.getKeyList(degree).getLast());
        leaf = (AbstractLeafTreeNode<String, Pointer>) IndexTreeNodeIO.read(compactFileIndexStorageManager, 1, leaf.getNextSiblingPointer(degree).get(), nodeFactory);
        Assertions.assertEquals("AAC", leaf.getKeyList(degree).getFirst());
        Assertions.assertEquals("AAD", leaf.getKeyList(degree).getLast());
        leaf = (AbstractLeafTreeNode<String, Pointer>) IndexTreeNodeIO.read(compactFileIndexStorageManager, 1, leaf.getNextSiblingPointer(degree).get(), nodeFactory);
        Assertions.assertEquals("ABA", leaf.getKeyList(degree).getFirst());
        Assertions.assertEquals("ABB", leaf.getKeyList(degree).getLast());
        leaf = (AbstractLeafTreeNode<String, Pointer>) IndexTreeNodeIO.read(compactFileIndexStorageManager, 1, leaf.getNextSiblingPointer(degree).get(), nodeFactory);
        Assertions.assertEquals("ABC", leaf.getKeyList(degree).getFirst());
        Assertions.assertEquals("ACA", leaf.getKeyList(degree).getLast());
        leaf = (AbstractLeafTreeNode<String, Pointer>) IndexTreeNodeIO.read(compactFileIndexStorageManager, 1, leaf.getNextSiblingPointer(degree).get(), nodeFactory);
        Assertions.assertEquals("ACB", leaf.getKeyList(degree).getFirst());
        Assertions.assertEquals("ACC", leaf.getKeyList(degree).getLast());
        leaf = (AbstractLeafTreeNode<String, Pointer>) IndexTreeNodeIO.read(compactFileIndexStorageManager, 1, leaf.getNextSiblingPointer(degree).get(), nodeFactory);
        Assertions.assertEquals("BAA", leaf.getKeyList(degree).getFirst());
        Assertions.assertEquals("BAB", leaf.getKeyList(degree).getLast());
        leaf = (AbstractLeafTreeNode<String, Pointer>) IndexTreeNodeIO.read(compactFileIndexStorageManager, 1, leaf.getNextSiblingPointer(degree).get(), nodeFactory);
        Assertions.assertEquals("BAC", leaf.getKeyList(degree).getFirst());
        Assertions.assertEquals("BBA", leaf.getKeyList(degree).get(1));
        Assertions.assertEquals("BBC", leaf.getKeyList(degree).getLast());
        leaf = (AbstractLeafTreeNode<String, Pointer>) IndexTreeNodeIO.read(compactFileIndexStorageManager, 1, leaf.getNextSiblingPointer(degree).get(), nodeFactory);
        Assertions.assertEquals("BBB", leaf.getKeyList(degree).getFirst());
        Assertions.assertEquals("CAA", leaf.getKeyList(degree).get(1));
        Assertions.assertEquals("CAB", leaf.getKeyList(degree).getLast());


    }

    private static class StringImmutableBinaryObjectWrapper implements ImmutableBinaryObjectWrapper<String> {
        private final int BYTES = 20;
        private byte[] bytes;

        public StringImmutableBinaryObjectWrapper() {
        }

        public StringImmutableBinaryObjectWrapper(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public ImmutableBinaryObjectWrapper<String> load(String s) throws InvalidBinaryObjectWrapperValue {
            byte[] temp = s.getBytes(StandardCharsets.UTF_8);
            if (temp.length > BYTES) {
                throw new InvalidBinaryObjectWrapperValue(s, this.getClass());
            }

            byte[] result = new byte[BYTES];

            System.arraycopy(temp, 0, result, 0, temp.length);

            for (int i = temp.length; i < BYTES; i++) {
                result[i] = 0;
            }

            return new StringImmutableBinaryObjectWrapper(result);
        }

        @Override
        public ImmutableBinaryObjectWrapper<String> load(byte[] bytes, int beginning) {
            this.bytes = new byte[BYTES];
            System.arraycopy(
                    bytes,
                    beginning,
                    this.bytes,
                    0,
                    this.size()
            );
            return this;
        }

        @Override
        public String asObject() {
            int len = 0;
            while (len < bytes.length && bytes[len] != 0) {
                len++;
            }
            return new String(bytes, 0, len, StandardCharsets.UTF_8);
        }

        @Override
        public boolean hasValue() {
            for (byte aByte : this.bytes) {
                if (aByte != 0x00)
                    return true;
            }
            return false;
        }

        @Override
        public int size() {
            return BYTES;
        }

        @Override
        public byte[] getBytes() {
            return bytes;
        }
    }


}
