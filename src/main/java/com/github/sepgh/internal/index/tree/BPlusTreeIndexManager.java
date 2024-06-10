package com.github.sepgh.internal.index.tree;

import com.github.sepgh.internal.index.IndexManager;
import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.internal.index.tree.node.AbstractTreeNode;
import com.github.sepgh.internal.index.tree.node.InternalTreeNode;
import com.github.sepgh.internal.index.tree.node.NodeFactory;
import com.github.sepgh.internal.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.data.NodeInnerObj;
import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.IndexTreeNodeIO;
import com.github.sepgh.internal.storage.session.ImmediateCommitIndexIOSession;
import com.github.sepgh.internal.storage.session.IndexIOSession;
import com.github.sepgh.internal.storage.session.IndexIOSessionFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class BPlusTreeIndexManager<K extends Comparable<K>, V extends Comparable<V>> implements IndexManager<K, V> {
    private final IndexStorageManager indexStorageManager;
    private final IndexIOSessionFactory indexIOSessionFactory;
    private final int degree;
    private final NodeInnerObj.Strategy<K> keyStrategy;
    private final NodeInnerObj.Strategy<V> valueStrategy;
    private final NodeFactory<K> nodeFactory;

    public BPlusTreeIndexManager(int degree, IndexStorageManager indexStorageManager, IndexIOSessionFactory indexIOSessionFactory, NodeInnerObj.Strategy<K> keyStrategy, NodeInnerObj.Strategy<V> valueStrategy, NodeFactory<K> nodeFactory){
        this.degree = degree;
        this.indexStorageManager = indexStorageManager;
        this.indexIOSessionFactory = indexIOSessionFactory;
        this.keyStrategy = keyStrategy;
        this.valueStrategy = valueStrategy;
        this.nodeFactory = nodeFactory;
    }

    public BPlusTreeIndexManager(int degree, IndexStorageManager indexStorageManager, NodeInnerObj.Strategy<K> keyStrategy, NodeInnerObj.Strategy<V> valueStrategy, NodeFactory<K> nodeFactory){
        this(degree, indexStorageManager, ImmediateCommitIndexIOSession.Factory.getInstance(), keyStrategy, valueStrategy, nodeFactory);
    }

    @Override
    public AbstractTreeNode<K> addIndex(int table, K identifier, V value) throws ExecutionException, InterruptedException, IOException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, table, nodeFactory);
        AbstractTreeNode<K> root = getRoot(indexIOSession);
        return new BPlusTreeIndexCreateOperation<>(degree, indexIOSession, keyStrategy, valueStrategy).addIndex(root, identifier, value);
    }

    @Override
    public Optional<V> getIndex(int table, K identifier) throws ExecutionException, InterruptedException, IOException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, table, nodeFactory);

        AbstractLeafTreeNode<K, V> baseTreeNode = BPlusTreeUtils.getResponsibleNode(indexStorageManager, getRoot(indexIOSession), identifier, table, degree, nodeFactory, valueStrategy);
        for (AbstractLeafTreeNode.KeyValue<K, V> entry : baseTreeNode.getKeyValueList(degree)) {
            if (entry.key() == identifier)
                return Optional.of(entry.value());
        }

        return Optional.empty();
    }

    @Override
    public boolean removeIndex(int table, K identifier) throws ExecutionException, InterruptedException, IOException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, table, nodeFactory);
        AbstractTreeNode<K> root = getRoot(indexIOSession);
        return new BPlusTreeIndexDeleteOperation<>(degree, table, indexIOSession, valueStrategy, nodeFactory).removeIndex(root, identifier);
    }

    @Override
    public int size(int table) throws InterruptedException, ExecutionException, IOException {
        Optional<IndexStorageManager.NodeData> optionalNodeData = this.indexStorageManager.getRoot(table).get();
        if (optionalNodeData.isEmpty())
            return 0;

        AbstractTreeNode<K> root = nodeFactory.fromNodeData(optionalNodeData.get());
        if (root.isLeaf()){
            return root.getKeyList(degree, valueStrategy.size()).size();
        }

        AbstractTreeNode<K> curr = root;
        while (!curr.isLeaf()) {
            curr = IndexTreeNodeIO.read(indexStorageManager, table, ((InternalTreeNode<K>) curr).getChildrenList().getFirst(), nodeFactory);
        }

        int size = curr.getKeyList(degree, valueStrategy.size()).size();
        Optional<Pointer> optionalNext = ((LeafClusterTreeNode<K>) curr).getNextSiblingPointer(degree);
        while (optionalNext.isPresent()){
            AbstractTreeNode<K> nextNode = IndexTreeNodeIO.read(indexStorageManager, table, optionalNext.get(), nodeFactory);
            size += nextNode.getKeyList(degree, valueStrategy.size()).size();
            optionalNext = ((LeafClusterTreeNode<K>) nextNode).getNextSiblingPointer(degree);
        }

        return size;
    }

    private AbstractTreeNode<K> getRoot(IndexIOSession<K> indexIOSession) throws ExecutionException, InterruptedException, IOException {
        Optional<AbstractTreeNode<K>> optionalRoot = indexIOSession.getRoot();
        if (optionalRoot.isPresent()){
            return optionalRoot.get();
        }

        byte[] emptyNode = indexStorageManager.getEmptyNode();
        LeafClusterTreeNode<K> leafTreeNode = (LeafClusterTreeNode<K>) nodeFactory.fromBytes(emptyNode, AbstractTreeNode.Type.LEAF);
        leafTreeNode.setAsRoot();

        IndexStorageManager.NodeData nodeData = indexIOSession.write(leafTreeNode);
        leafTreeNode.setPointer(nodeData.pointer());
        return leafTreeNode;
    }

}
