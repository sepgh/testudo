package com.github.sepgh.testudo.index.tree;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.IndexMissingException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.AbstractUniqueTreeIndexManager;
import com.github.sepgh.testudo.index.KeyValue;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.InternalTreeNode;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.testudo.index.tree.node.data.IndexBinaryObject;
import com.github.sepgh.testudo.index.tree.node.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.session.ImmediateCommitIndexIOSession;
import com.github.sepgh.testudo.storage.index.session.IndexIOSession;
import com.github.sepgh.testudo.storage.index.session.IndexIOSessionFactory;
import com.github.sepgh.testudo.utils.KVSize;
import com.github.sepgh.testudo.utils.LockableIterator;
import lombok.SneakyThrows;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.testudo.index.tree.node.AbstractTreeNode.TYPE_LEAF_NODE_BIT;

public class BPlusTreeUniqueTreeIndexManager<K extends Comparable<K>, V> extends AbstractUniqueTreeIndexManager<K, V> {
    private final IndexStorageManager indexStorageManager;
    private final IndexIOSessionFactory indexIOSessionFactory;
    private final int degree;
    private final IndexBinaryObjectFactory<K> keyIndexBinaryObjectFactory;
    private final IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory;
    private final NodeFactory<K> nodeFactory;
    protected final KVSize kvSize;

    public BPlusTreeUniqueTreeIndexManager(int index, int degree, IndexStorageManager indexStorageManager, IndexIOSessionFactory indexIOSessionFactory, IndexBinaryObjectFactory<K> keyIndexBinaryObjectFactory, IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory, NodeFactory<K> nodeFactory) {
        super(index);
        this.degree = degree;
        this.indexStorageManager = indexStorageManager;
        this.indexIOSessionFactory = indexIOSessionFactory;
        this.keyIndexBinaryObjectFactory = keyIndexBinaryObjectFactory;
        this.valueIndexBinaryObjectFactory = valueIndexBinaryObjectFactory;
        this.nodeFactory = nodeFactory;
        this.kvSize = new KVSize(
                keyIndexBinaryObjectFactory.size(),
                valueIndexBinaryObjectFactory.size()
        );
    }

    public BPlusTreeUniqueTreeIndexManager(int index, int degree, IndexStorageManager indexStorageManager, IndexBinaryObjectFactory<K> keyIndexBinaryObjectFactory, IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory, NodeFactory<K> nodeFactory){
        this(index, degree, indexStorageManager, ImmediateCommitIndexIOSession.Factory.getInstance(), keyIndexBinaryObjectFactory, valueIndexBinaryObjectFactory, nodeFactory);
    }

    public BPlusTreeUniqueTreeIndexManager(int index, int degree, IndexStorageManager indexStorageManager, IndexIOSessionFactory indexIOSessionFactory, IndexBinaryObjectFactory<K> keyIndexBinaryObjectFactory, IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory){
        this(index, degree, indexStorageManager, indexIOSessionFactory, keyIndexBinaryObjectFactory, valueIndexBinaryObjectFactory, new NodeFactory<K>() {
            @Override
            public AbstractTreeNode<K> fromBytes(byte[] bytes) {
                if ((bytes[0] & TYPE_LEAF_NODE_BIT) == TYPE_LEAF_NODE_BIT)
                    return new AbstractLeafTreeNode<>(bytes, keyIndexBinaryObjectFactory, valueIndexBinaryObjectFactory);
                return new InternalTreeNode<>(bytes, keyIndexBinaryObjectFactory);
            }

            @Override
            public AbstractTreeNode<K> fromBytes(byte[] bytes, AbstractTreeNode.Type type) {
                if (type.equals(AbstractTreeNode.Type.LEAF))
                    return new AbstractLeafTreeNode<>(bytes, keyIndexBinaryObjectFactory, valueIndexBinaryObjectFactory);
                return new InternalTreeNode<>(bytes, keyIndexBinaryObjectFactory);
            }
        });

    }

    public BPlusTreeUniqueTreeIndexManager(int index, int degree, IndexStorageManager indexStorageManager, IndexBinaryObjectFactory<K> keyIndexBinaryObjectFactory, IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory){
        this(index, degree, indexStorageManager, ImmediateCommitIndexIOSession.Factory.getInstance(), keyIndexBinaryObjectFactory, valueIndexBinaryObjectFactory);
    }

    @Override
    public AbstractTreeNode<K> addIndex(K identifier, V value) throws InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject, IndexExistsException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, indexId, nodeFactory, kvSize);
        AbstractTreeNode<K> root = getRoot(indexIOSession);
        return new BPlusTreeIndexCreateOperation<>(degree, indexIOSession, keyIndexBinaryObjectFactory, valueIndexBinaryObjectFactory, this.kvSize).addIndex(root, identifier, value);
    }

    @Override
    public AbstractTreeNode<K> updateIndex(K identifier, V value) throws InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject, IndexMissingException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, indexId, nodeFactory, kvSize);

        AbstractLeafTreeNode<K, V> node = BPlusTreeUtils.getResponsibleNode(indexStorageManager, getRoot(indexIOSession), identifier, indexId, degree, nodeFactory, valueIndexBinaryObjectFactory);
        List<K> keyList = node.getKeyList(degree);
        if (!keyList.contains(identifier)) {
            throw new IndexMissingException();
        }

        node.setKeyValue(keyList.indexOf(identifier), new KeyValue<>(identifier, value));
        indexIOSession.update(node);
        indexIOSession.commit();
        return node;
    }

    @Override
    public Optional<V> getIndex(K identifier) throws InternalOperationException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, indexId, nodeFactory, kvSize);

        AbstractLeafTreeNode<K, V> baseTreeNode = BPlusTreeUtils.getResponsibleNode(indexStorageManager, getRoot(indexIOSession), identifier, indexId, degree, nodeFactory, valueIndexBinaryObjectFactory);
        for (KeyValue<K, V> entry : baseTreeNode.getKeyValueList(degree)) {
            if (entry.key() == identifier)
                return Optional.of(entry.value());
        }

        return Optional.empty();
    }

    @Override
    public boolean removeIndex(K identifier) throws InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, indexId, nodeFactory, kvSize);
        AbstractTreeNode<K> root = getRoot(indexIOSession);
        return new BPlusTreeIndexDeleteOperation<>(degree, indexId, indexIOSession, valueIndexBinaryObjectFactory, nodeFactory).removeIndex(root, identifier);
    }

    @Override
    public int size() throws InternalOperationException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, indexId, nodeFactory, kvSize);

        Optional<IndexStorageManager.NodeData> optionalNodeData = null;
        try {
            optionalNodeData = this.indexStorageManager.getRoot(indexId, kvSize).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new InternalOperationException(e);
        }

        if (optionalNodeData.isEmpty())
            return 0;

        AbstractTreeNode<K> root = nodeFactory.fromNodeData(optionalNodeData.get());
        if (root.isLeaf()){
            return root.getKeyList(degree, valueIndexBinaryObjectFactory.size()).size();
        }

        AbstractTreeNode<K> curr = root;
        while (!curr.isLeaf()) {
            curr = indexIOSession.read(((InternalTreeNode<K>) curr).getChildrenList().getFirst());
        }

        int size = curr.getKeyList(degree, valueIndexBinaryObjectFactory.size()).size();
        Optional<Pointer> optionalNext = ((LeafClusterTreeNode<K>) curr).getNextSiblingPointer(degree);
        while (optionalNext.isPresent()){
            AbstractTreeNode<K> nextNode = indexIOSession.read(optionalNext.get());
            size += nextNode.getKeyList(degree, valueIndexBinaryObjectFactory.size()).size();
            optionalNext = ((LeafClusterTreeNode<K>) nextNode).getNextSiblingPointer(degree);
        }

        return size;
    }

    @Override
    public LockableIterator<KeyValue<K, V>> getSortedIterator() throws InternalOperationException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, indexId, nodeFactory, kvSize);

        return new LockableIterator<>() {
            @Override
            public void lock() {
            }

            @Override
            public void unlock() {
            }

            private int keyIndex = 0;
            AbstractLeafTreeNode<K, V> currentLeaf = getFarLeftLeaf();

            @Override
            public boolean hasNext() {
                int size = currentLeaf.getKeyList(degree).size();
                if (keyIndex == size)
                    return currentLeaf.getNextSiblingPointer(degree).isPresent();
                return true;
            }

            @SneakyThrows
            @Override
            public KeyValue<K, V> next() {
                List<KeyValue<K, V>> keyValueList = currentLeaf.getKeyValueList(degree);

                if (keyIndex == keyValueList.size()){
                    currentLeaf = (AbstractLeafTreeNode<K, V>) indexIOSession.read(currentLeaf.getNextSiblingPointer(degree).get());
                    keyIndex = 0;
                    keyValueList = currentLeaf.getKeyValueList(degree);
                }

                KeyValue<K, V> output = keyValueList.get(keyIndex);
                keyIndex += 1;
                return output;
            }
        };
    }

    @Override
    public void purgeIndex() {
        if (this.indexStorageManager.supportsPurge()) {
            this.indexStorageManager.purgeIndex(indexId);
        }

        // Todo: traverse tree and remove nodes if storage manager doesnt support purge
    }

    protected AbstractLeafTreeNode<K, V> getFarLeftLeaf() throws InternalOperationException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, indexId, nodeFactory, kvSize);
        AbstractTreeNode<K> root = getRoot(indexIOSession);
        if (root.isLeaf())
            return (AbstractLeafTreeNode<K, V>) root;

        AbstractTreeNode<K> farLeftChild = root;

        while (!farLeftChild.isLeaf()){
            farLeftChild = indexIOSession.read(((InternalTreeNode<K>) farLeftChild).getChildAtIndex(0));
        }

        return (AbstractLeafTreeNode<K, V>) farLeftChild;
    }

    private AbstractTreeNode<K> getRoot(IndexIOSession<K> indexIOSession) throws InternalOperationException {
        Optional<AbstractTreeNode<K>> optionalRoot = indexIOSession.getRoot();
        if (optionalRoot.isPresent()){
            return optionalRoot.get();
        }

        byte[] emptyNode = indexStorageManager.getEmptyNode(kvSize);
        AbstractLeafTreeNode<K, ?> leafTreeNode = (AbstractLeafTreeNode<K, ?>) nodeFactory.fromBytes(emptyNode, AbstractTreeNode.Type.LEAF);
        leafTreeNode.setAsRoot();

        IndexStorageManager.NodeData nodeData = indexIOSession.write(leafTreeNode);
        leafTreeNode.setPointer(nodeData.pointer());
        return leafTreeNode;
    }

}
