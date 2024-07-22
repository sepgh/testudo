package com.github.sepgh.testudo.index.tree;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.InternalTreeNode;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.session.ImmediateCommitIndexIOSession;
import com.github.sepgh.testudo.storage.index.session.IndexIOSession;
import com.github.sepgh.testudo.storage.index.session.IndexIOSessionFactory;
import com.github.sepgh.testudo.utils.LockableIterator;
import lombok.SneakyThrows;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.testudo.index.tree.node.AbstractTreeNode.TYPE_LEAF_NODE_BIT;

public class BPlusTreeIndexManager<K extends Comparable<K>, V extends Comparable<V>> implements IndexManager<K, V> {
    private final IndexStorageManager indexStorageManager;
    private final IndexIOSessionFactory indexIOSessionFactory;
    private final int degree;
    private final ImmutableBinaryObjectWrapper<K> keyImmutableBinaryObjectWrapper;
    private final ImmutableBinaryObjectWrapper<V> valueImmutableBinaryObjectWrapper;
    private final NodeFactory<K> nodeFactory;

    public BPlusTreeIndexManager(int degree, IndexStorageManager indexStorageManager, IndexIOSessionFactory indexIOSessionFactory, ImmutableBinaryObjectWrapper<K> keyImmutableBinaryObjectWrapper, ImmutableBinaryObjectWrapper<V> valueImmutableBinaryObjectWrapper, NodeFactory<K> nodeFactory){
        this.degree = degree;
        this.indexStorageManager = indexStorageManager;
        this.indexIOSessionFactory = indexIOSessionFactory;
        this.keyImmutableBinaryObjectWrapper = keyImmutableBinaryObjectWrapper;
        this.valueImmutableBinaryObjectWrapper = valueImmutableBinaryObjectWrapper;
        this.nodeFactory = nodeFactory;
    }

    public BPlusTreeIndexManager(int degree, IndexStorageManager indexStorageManager, ImmutableBinaryObjectWrapper<K> keyImmutableBinaryObjectWrapper, ImmutableBinaryObjectWrapper<V> valueImmutableBinaryObjectWrapper, NodeFactory<K> nodeFactory){
        this(degree, indexStorageManager, ImmediateCommitIndexIOSession.Factory.getInstance(), keyImmutableBinaryObjectWrapper, valueImmutableBinaryObjectWrapper, nodeFactory);
    }

    public BPlusTreeIndexManager(int degree, IndexStorageManager indexStorageManager, IndexIOSessionFactory indexIOSessionFactory, ImmutableBinaryObjectWrapper<K> keyImmutableBinaryObjectWrapper, ImmutableBinaryObjectWrapper<V> valueImmutableBinaryObjectWrapper){
        this(degree, indexStorageManager, indexIOSessionFactory, keyImmutableBinaryObjectWrapper, valueImmutableBinaryObjectWrapper, new NodeFactory<K>() {
            @Override
            public AbstractTreeNode<K> fromBytes(byte[] bytes) {
                if ((bytes[0] & TYPE_LEAF_NODE_BIT) == TYPE_LEAF_NODE_BIT)
                    return new AbstractLeafTreeNode<>(bytes, keyImmutableBinaryObjectWrapper, valueImmutableBinaryObjectWrapper);
                return new InternalTreeNode<>(bytes, keyImmutableBinaryObjectWrapper);
            }

            @Override
            public AbstractTreeNode<K> fromBytes(byte[] bytes, AbstractTreeNode.Type type) {
                if (type.equals(AbstractTreeNode.Type.LEAF))
                    return new AbstractLeafTreeNode<>(bytes, keyImmutableBinaryObjectWrapper, valueImmutableBinaryObjectWrapper);
                return new InternalTreeNode<>(bytes, keyImmutableBinaryObjectWrapper);
            }
        });

    }

    public BPlusTreeIndexManager(int degree, IndexStorageManager indexStorageManager, ImmutableBinaryObjectWrapper<K> keyImmutableBinaryObjectWrapper, ImmutableBinaryObjectWrapper<V> valueImmutableBinaryObjectWrapper){
        this(degree, indexStorageManager, ImmediateCommitIndexIOSession.Factory.getInstance(), keyImmutableBinaryObjectWrapper, valueImmutableBinaryObjectWrapper);
    }

    @Override
    public AbstractTreeNode<K> addIndex(int index, K identifier, V value) throws InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, index, nodeFactory);
        AbstractTreeNode<K> root = getRoot(indexIOSession);
        return new BPlusTreeIndexCreateOperation<>(degree, indexIOSession, keyImmutableBinaryObjectWrapper, valueImmutableBinaryObjectWrapper).addIndex(root, identifier, value);
    }

    @Override
    public Optional<V> getIndex(int index, K identifier) throws InternalOperationException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, index, nodeFactory);

        AbstractLeafTreeNode<K, V> baseTreeNode = BPlusTreeUtils.getResponsibleNode(indexStorageManager, getRoot(indexIOSession), identifier, index, degree, nodeFactory, valueImmutableBinaryObjectWrapper);
        for (AbstractLeafTreeNode.KeyValue<K, V> entry : baseTreeNode.getKeyValueList(degree)) {
            if (entry.key() == identifier)
                return Optional.of(entry.value());
        }

        return Optional.empty();
    }

    @Override
    public boolean removeIndex(int index, K identifier) throws InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, index, nodeFactory);
        AbstractTreeNode<K> root = getRoot(indexIOSession);
        return new BPlusTreeIndexDeleteOperation<>(degree, index, indexIOSession, valueImmutableBinaryObjectWrapper, nodeFactory).removeIndex(root, identifier);
    }

    @Override
    public int size(int index) throws InternalOperationException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, index, nodeFactory);

        Optional<IndexStorageManager.NodeData> optionalNodeData = null;
        try {
            optionalNodeData = this.indexStorageManager.getRoot(index).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new InternalOperationException(e);
        }

        if (optionalNodeData.isEmpty())
            return 0;

        AbstractTreeNode<K> root = nodeFactory.fromNodeData(optionalNodeData.get());
        if (root.isLeaf()){
            return root.getKeyList(degree, valueImmutableBinaryObjectWrapper.size()).size();
        }

        AbstractTreeNode<K> curr = root;
        while (!curr.isLeaf()) {
            curr = indexIOSession.read(((InternalTreeNode<K>) curr).getChildrenList().getFirst());
        }

        int size = curr.getKeyList(degree, valueImmutableBinaryObjectWrapper.size()).size();
        Optional<Pointer> optionalNext = ((LeafClusterTreeNode<K>) curr).getNextSiblingPointer(degree);
        while (optionalNext.isPresent()){
            AbstractTreeNode<K> nextNode = indexIOSession.read(optionalNext.get());
            size += nextNode.getKeyList(degree, valueImmutableBinaryObjectWrapper.size()).size();
            optionalNext = ((LeafClusterTreeNode<K>) nextNode).getNextSiblingPointer(degree);
        }

        return size;
    }

    @Override
    public LockableIterator<AbstractLeafTreeNode.KeyValue<K, V>> getSortedIterator(int index) throws InternalOperationException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, index, nodeFactory);

        return new LockableIterator<AbstractLeafTreeNode.KeyValue<K, V>>() {
            @Override
            public void lock() {
            }

            @Override
            public void unlock() {
            }

            private int keyIndex = 0;
            AbstractLeafTreeNode<K, V> currentLeaf = getFarLeftLeaf(index);

            @Override
            public boolean hasNext() {
                int size = currentLeaf.getKeyList(degree).size();
                if (keyIndex == size)
                    return currentLeaf.getNextSiblingPointer(degree).isPresent();
                return true;
            }

            @SneakyThrows
            @Override
            public AbstractLeafTreeNode.KeyValue<K, V> next() {
                List<AbstractLeafTreeNode.KeyValue<K, V>> keyValueList = currentLeaf.getKeyValueList(degree);

                if (keyIndex == keyValueList.size()){
                    currentLeaf = (AbstractLeafTreeNode<K, V>) indexIOSession.read(currentLeaf.getNextSiblingPointer(degree).get());
                    keyIndex = 0;
                    keyValueList = currentLeaf.getKeyValueList(degree);
                }

                AbstractLeafTreeNode.KeyValue<K, V> output = keyValueList.get(keyIndex);
                keyIndex += 1;
                return output;
            }
        };
    }

    @Override
    public void purgeIndex(int index) {
        if (this.indexStorageManager.supportsPurge()) {
            this.indexStorageManager.purgeIndex(index);
        }

        // Todo: traverse tree and remove nodes if storage manager doesnt support purge
    }

    protected AbstractLeafTreeNode<K, V> getFarLeftLeaf(int index) throws InternalOperationException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, index, nodeFactory);
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

        byte[] emptyNode = indexStorageManager.getEmptyNode();
        AbstractLeafTreeNode<K, ?> leafTreeNode = (AbstractLeafTreeNode<K, ?>) nodeFactory.fromBytes(emptyNode, AbstractTreeNode.Type.LEAF);
        leafTreeNode.setAsRoot();

        IndexStorageManager.NodeData nodeData = indexIOSession.write(leafTreeNode);
        leafTreeNode.setPointer(nodeData.pointer());
        return leafTreeNode;
    }

}
