package com.github.sepgh.test.index.tree;

import com.github.sepgh.test.TestParams;
import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.ds.KVSize;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.index.data.PointerIndexBinaryObject;
import com.github.sepgh.testudo.index.tree.BPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.InternalTreeNode;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.serialization.CharArrSerializer;
import com.github.sepgh.testudo.serialization.IntegerSerializer;
import com.github.sepgh.testudo.storage.index.BTreeSizeCalculator;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.IndexTreeNodeIO;
import com.github.sepgh.testudo.storage.index.OrganizedFileIndexStorageManager;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.test.TestParams.DEFAULT_INDEX_BINARY_OBJECT_FACTORY;
import static com.github.sepgh.test.TestParams.FAKE_FIELD_SUPPLIER;
import static com.github.sepgh.testudo.index.tree.node.AbstractTreeNode.TYPE_LEAF_NODE_BIT;
import static com.github.sepgh.testudo.storage.index.BaseFileIndexStorageManager.INDEX_FILE_NAME;

public class IndexBinaryObjectTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private final int degree = 4;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_BinaryObjectWrapperTestCase");

        engineConfig = EngineConfig.builder()
                .baseDBPath(dbPath.toString())
                .bTreeDegree(degree)
                .bTreeGrowthNodeAllocationCount(2)
                .baseDBPath(dbPath.toString())
                .build();
        engineConfig.setBTreeMaxFileSize(4L * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size()));

        byte[] writingBytes = new byte[]{};
        Path indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    @AfterEach
    public void destroy() throws IOException {
        FileUtils.deleteDirectory(dbPath.toString());
    }

    private OrganizedFileIndexStorageManager getStorageManager() throws IOException, ExecutionException, InterruptedException {
        return new OrganizedFileIndexStorageManager(
                "test",
                new JsonIndexHeaderManager.SingletonFactory(),
                engineConfig,
                new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance())
        );
    }

    @Test
    public void test_IntegerIdentifier() throws IOException, ExecutionException, InterruptedException, InternalOperationException {
        OrganizedFileIndexStorageManager organizedFileIndexStorageManager = getStorageManager();

        UniqueTreeIndexManager<Integer, Pointer> uniqueTreeIndexManager = new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, organizedFileIndexStorageManager, new IntegerSerializer().getIndexBinaryObjectFactory(TestParams.FAKE_FIELD));


        Pointer P = new Pointer(Pointer.TYPE_DATA, 0, 0);
        for (int i = 0; i < 13; i ++){
            uniqueTreeIndexManager.addIndex(i, P);
        }

        for (int i = 0; i < 13; i ++){
            Assertions.assertTrue(uniqueTreeIndexManager.getIndex(i).isPresent(), "%d is not present".formatted(i));
        }

        for (int i = 0; i < 13; i ++){
            Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(i));
        }

        for (int i = 0; i < 13; i ++){
            Assertions.assertFalse(uniqueTreeIndexManager.getIndex(i).isPresent());
        }

    }

    @Test
    public void test_CustomBinaryObjectWrapper() throws IOException, ExecutionException, InterruptedException, InternalOperationException {
        OrganizedFileIndexStorageManager organizedFileIndexStorageManager = new OrganizedFileIndexStorageManager(
                "Test",
                new JsonIndexHeaderManager.SingletonFactory(),
                engineConfig,
                new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance())
        );

        Scheme.Field field = FAKE_FIELD_SUPPLIER.get();
        field.setMeta(Scheme.Meta.builder().maxLength(3).build());
        IndexBinaryObjectFactory<String> keyIndexBinaryObjectFactory = new CharArrSerializer().getIndexBinaryObjectFactory(field);

        NodeFactory<String> nodeFactory = new NodeFactory<>() {
            @Override
            public AbstractTreeNode<String> fromBytes(byte[] bytes) {
                if ((bytes[0] & TYPE_LEAF_NODE_BIT) == TYPE_LEAF_NODE_BIT)
                    return new AbstractLeafTreeNode<>(bytes, keyIndexBinaryObjectFactory, new PointerIndexBinaryObject.Factory());
                return new InternalTreeNode<>(bytes, keyIndexBinaryObjectFactory);
            }

            @Override
            public AbstractTreeNode<String> fromBytes(byte[] bytes, AbstractTreeNode.Type type) {
                if (type.equals(AbstractTreeNode.Type.LEAF))
                    return new AbstractLeafTreeNode<>(bytes, keyIndexBinaryObjectFactory, new PointerIndexBinaryObject.Factory());
                return new InternalTreeNode<>(bytes, keyIndexBinaryObjectFactory);
            }
        };


        UniqueTreeIndexManager<String, Pointer> uniqueTreeIndexManager = new BPlusTreeUniqueTreeIndexManager<>(1, degree, organizedFileIndexStorageManager, keyIndexBinaryObjectFactory, new PointerIndexBinaryObject.Factory(), nodeFactory);


        uniqueTreeIndexManager.addIndex("AAA", new Pointer(Pointer.TYPE_DATA, 0, 0));
        uniqueTreeIndexManager.addIndex("BBB", new Pointer(Pointer.TYPE_DATA, 0, 0));
        uniqueTreeIndexManager.addIndex("CAB", new Pointer(Pointer.TYPE_DATA, 0, 0));
        uniqueTreeIndexManager.addIndex("AAC", new Pointer(Pointer.TYPE_DATA, 0, 0));
        uniqueTreeIndexManager.addIndex("BAC", new Pointer(Pointer.TYPE_DATA, 0, 0));
        uniqueTreeIndexManager.addIndex("CAA", new Pointer(Pointer.TYPE_DATA, 0, 0));

        uniqueTreeIndexManager.addIndex("AAB", new Pointer(Pointer.TYPE_DATA, 0, 0));
        uniqueTreeIndexManager.addIndex("AAD", new Pointer(Pointer.TYPE_DATA, 0, 0));

        uniqueTreeIndexManager.addIndex("ABA", new Pointer(Pointer.TYPE_DATA, 0, 0));
        uniqueTreeIndexManager.addIndex("ABB", new Pointer(Pointer.TYPE_DATA, 0, 0));
        uniqueTreeIndexManager.addIndex("ABC", new Pointer(Pointer.TYPE_DATA, 0, 0));

        uniqueTreeIndexManager.addIndex("ACA", new Pointer(Pointer.TYPE_DATA, 0, 0));
        uniqueTreeIndexManager.addIndex("ACB", new Pointer(Pointer.TYPE_DATA, 0, 0));
        uniqueTreeIndexManager.addIndex("ACC", new Pointer(Pointer.TYPE_DATA, 0, 0));

        uniqueTreeIndexManager.addIndex("BAA", new Pointer(Pointer.TYPE_DATA, 0, 0));
        uniqueTreeIndexManager.addIndex("BAB", new Pointer(Pointer.TYPE_DATA, 0, 0));
        uniqueTreeIndexManager.addIndex("BBA", new Pointer(Pointer.TYPE_DATA, 0, 0));
        uniqueTreeIndexManager.addIndex("BBC", new Pointer(Pointer.TYPE_DATA, 0, 0));


        KVSize kvSize = new KVSize(20, PointerIndexBinaryObject.BYTES);
        IndexStorageManager.NodeData rootNodeData = organizedFileIndexStorageManager.getRoot(1, kvSize).get().get();
        InternalTreeNode<String> rootInternalTreeNode = new InternalTreeNode<>(rootNodeData.bytes(), keyIndexBinaryObjectFactory);
        rootInternalTreeNode.setPointer(rootNodeData.pointer());


        InternalTreeNode<String> internalNode1 = (InternalTreeNode<String>) IndexTreeNodeIO.read(organizedFileIndexStorageManager, 1, rootInternalTreeNode.getChildrenList().getFirst(), nodeFactory, kvSize);

        AbstractLeafTreeNode<String, Pointer> leaf = (AbstractLeafTreeNode<String, Pointer>) IndexTreeNodeIO.read(organizedFileIndexStorageManager, 1, internalNode1.getChildrenList().getFirst(), nodeFactory, kvSize);
        Assertions.assertEquals("AAA", leaf.getKeyList(degree).getFirst());
        Assertions.assertEquals("AAB", leaf.getKeyList(degree).getLast());
        leaf = (AbstractLeafTreeNode<String, Pointer>) IndexTreeNodeIO.read(organizedFileIndexStorageManager, 1, leaf.getNextSiblingPointer(degree).get(), nodeFactory, kvSize);
        Assertions.assertEquals("AAC", leaf.getKeyList(degree).getFirst());
        Assertions.assertEquals("AAD", leaf.getKeyList(degree).getLast());
        leaf = (AbstractLeafTreeNode<String, Pointer>) IndexTreeNodeIO.read(organizedFileIndexStorageManager, 1, leaf.getNextSiblingPointer(degree).get(), nodeFactory, kvSize);
        Assertions.assertEquals("ABA", leaf.getKeyList(degree).getFirst());
        Assertions.assertEquals("ABB", leaf.getKeyList(degree).getLast());
        leaf = (AbstractLeafTreeNode<String, Pointer>) IndexTreeNodeIO.read(organizedFileIndexStorageManager, 1, leaf.getNextSiblingPointer(degree).get(), nodeFactory, kvSize);
        Assertions.assertEquals("ABC", leaf.getKeyList(degree).getFirst());
        Assertions.assertEquals("ACA", leaf.getKeyList(degree).getLast());
        leaf = (AbstractLeafTreeNode<String, Pointer>) IndexTreeNodeIO.read(organizedFileIndexStorageManager, 1, leaf.getNextSiblingPointer(degree).get(), nodeFactory, kvSize);
        Assertions.assertEquals("ACB", leaf.getKeyList(degree).getFirst());
        Assertions.assertEquals("ACC", leaf.getKeyList(degree).getLast());
        leaf = (AbstractLeafTreeNode<String, Pointer>) IndexTreeNodeIO.read(organizedFileIndexStorageManager, 1, leaf.getNextSiblingPointer(degree).get(), nodeFactory, kvSize);
        Assertions.assertEquals("BAA", leaf.getKeyList(degree).getFirst());
        Assertions.assertEquals("BAB", leaf.getKeyList(degree).getLast());
        leaf = (AbstractLeafTreeNode<String, Pointer>) IndexTreeNodeIO.read(organizedFileIndexStorageManager, 1, leaf.getNextSiblingPointer(degree).get(), nodeFactory, kvSize);
        Assertions.assertEquals("BAC", leaf.getKeyList(degree).getFirst());
        Assertions.assertEquals("BBA", leaf.getKeyList(degree).get(1));
        Assertions.assertEquals("BBC", leaf.getKeyList(degree).getLast());
        leaf = (AbstractLeafTreeNode<String, Pointer>) IndexTreeNodeIO.read(organizedFileIndexStorageManager, 1, leaf.getNextSiblingPointer(degree).get(), nodeFactory, kvSize);
        Assertions.assertEquals("BBB", leaf.getKeyList(degree).getFirst());
        Assertions.assertEquals("CAA", leaf.getKeyList(degree).get(1));
        Assertions.assertEquals("CAB", leaf.getKeyList(degree).getLast());


    }


}
