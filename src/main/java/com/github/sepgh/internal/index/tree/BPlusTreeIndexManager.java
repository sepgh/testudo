package com.github.sepgh.internal.index.tree;

import com.github.sepgh.internal.index.IndexManager;
import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.cluster.BaseClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.cluster.ClusterIdentifier;
import com.github.sepgh.internal.index.tree.node.cluster.InternalClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.IndexTreeNodeIO;
import com.github.sepgh.internal.storage.session.ImmediateCommitIndexIOSession;
import com.github.sepgh.internal.storage.session.IndexIOSession;
import com.github.sepgh.internal.storage.session.IndexIOSessionFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class BPlusTreeIndexManager<K extends Comparable<K>> implements IndexManager<K> {
    private final IndexStorageManager indexStorageManager;
    private final IndexIOSessionFactory indexIOSessionFactory;
    private final int degree;
    private final ClusterIdentifier.Strategy<K> strategy;

    public BPlusTreeIndexManager(int degree, IndexStorageManager indexStorageManager, IndexIOSessionFactory indexIOSessionFactory, ClusterIdentifier.Strategy<K> strategy){
        this.degree = degree;
        this.indexStorageManager = indexStorageManager;
        this.indexIOSessionFactory = indexIOSessionFactory;
        this.strategy = strategy;
    }

    public BPlusTreeIndexManager(int degree, IndexStorageManager indexStorageManager, ClusterIdentifier.Strategy<K> strategy){
        this(degree, indexStorageManager, ImmediateCommitIndexIOSession.Factory.getInstance(), strategy);
    }

    @Override
    public BaseClusterTreeNode<K> addIndex(int table, K identifier, Pointer pointer) throws ExecutionException, InterruptedException, IOException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, table, strategy);
        BaseClusterTreeNode<K> root = getRoot(indexIOSession);
        return new BPlusTreeIndexCreateOperation<>(degree, indexIOSession, strategy).addIndex(root, identifier, pointer);
    }

    @Override
    public Optional<Pointer> getIndex(int table, K identifier) throws ExecutionException, InterruptedException, IOException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, table, strategy);

        LeafClusterTreeNode<K> baseTreeNode = BPlusTreeUtils.getResponsibleNode(indexStorageManager, getRoot(indexIOSession), identifier, table, degree, strategy);
        for (LeafClusterTreeNode.KeyValue<K> entry : baseTreeNode.getKeyValueList(degree)) {
            if (entry.key() == identifier)
                return Optional.of(entry.value());
        }

        return Optional.empty();
    }

    @Override
    public boolean removeIndex(int table, K identifier) throws ExecutionException, InterruptedException, IOException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, table, strategy);
        BaseClusterTreeNode<K> root = getRoot(indexIOSession);
        return new BPlusTreeIndexDeleteOperation<>(degree, table, indexIOSession, strategy).removeIndex(root, identifier);
    }

    @Override
    public int size(int table) throws InterruptedException, ExecutionException, IOException {
        Optional<IndexStorageManager.NodeData> optionalNodeData = this.indexStorageManager.getRoot(table).get();
        if (optionalNodeData.isEmpty())
            return 0;

        BaseClusterTreeNode<K> root = BaseClusterTreeNode.fromNodeData(optionalNodeData.get(), strategy);
        if (root.isLeaf()){
            return root.getKeyList(degree).size();
        }

        BaseClusterTreeNode<K> curr = root;
        while (!curr.isLeaf()) {
            curr = IndexTreeNodeIO.read(indexStorageManager, table, ((InternalClusterTreeNode<K>) curr).getChildrenList().getFirst(), strategy);
        }

        int size = curr.getKeyList(degree).size();
        Optional<Pointer> optionalNext = ((LeafClusterTreeNode<K>) curr).getNextSiblingPointer(degree);
        while (optionalNext.isPresent()){
            BaseClusterTreeNode<K> nextNode = IndexTreeNodeIO.read(indexStorageManager, table, optionalNext.get(), strategy);
            size += nextNode.getKeyList(degree).size();
            optionalNext = ((LeafClusterTreeNode<K>) nextNode).getNextSiblingPointer(degree);
        }

        return size;
    }

    private BaseClusterTreeNode<K> getRoot(IndexIOSession<K> indexIOSession) throws ExecutionException, InterruptedException, IOException {
        Optional<BaseClusterTreeNode<K>> optionalRoot = indexIOSession.getRoot();
        if (optionalRoot.isPresent()){
            return optionalRoot.get();
        }

        byte[] emptyNode = indexStorageManager.getEmptyNode();
        LeafClusterTreeNode<K> leafTreeNode = (LeafClusterTreeNode<K>) BaseClusterTreeNode.fromBytes(emptyNode, BaseClusterTreeNode.Type.LEAF, strategy);
        leafTreeNode.setAsRoot();

        IndexStorageManager.NodeData nodeData = indexIOSession.write(leafTreeNode);
        leafTreeNode.setPointer(nodeData.pointer());
        return leafTreeNode;
    }

}
