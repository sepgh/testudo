package com.github.sepgh.internal.index.tree;

import com.github.sepgh.internal.index.IndexManager;
import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.cluster.BaseClusterTreeNode;
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

public class BPlusTreeIndexManager implements IndexManager {
    private final IndexStorageManager indexStorageManager;
    private final IndexIOSessionFactory indexIOSessionFactory;
    private final int degree;

    public BPlusTreeIndexManager(int degree, IndexStorageManager indexStorageManager, IndexIOSessionFactory indexIOSessionFactory){
        this.degree = degree;
        this.indexStorageManager = indexStorageManager;
        this.indexIOSessionFactory = indexIOSessionFactory;
    }

    public BPlusTreeIndexManager(int degree, IndexStorageManager indexStorageManager){
        this(degree, indexStorageManager, ImmediateCommitIndexIOSession.Factory.getInstance());
    }

    @Override
    public BaseClusterTreeNode addIndex(int table, long identifier, Pointer pointer) throws ExecutionException, InterruptedException, IOException {
        IndexIOSession indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, table);
        BaseClusterTreeNode root = getRoot(indexIOSession);
        return new BPlusTreeIndexCreateOperation(degree, table, indexIOSession).addIndex(root, identifier, pointer);
    }

    @Override
    public Optional<Pointer> getIndex(int table, long identifier) throws ExecutionException, InterruptedException, IOException {
        IndexIOSession indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, table);

        LeafClusterTreeNode baseTreeNode = BPlusTreeUtils.getResponsibleNode(indexStorageManager, getRoot(indexIOSession), identifier, table, degree);
        for (LeafClusterTreeNode.KeyValue entry : baseTreeNode.getKeyValueList(degree)) {
            if (entry.key() == identifier)
                return Optional.of(entry.value());
        }

        return Optional.empty();
    }

    @Override
    public boolean removeIndex(int table, long identifier) throws ExecutionException, InterruptedException, IOException {
        IndexIOSession indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, table);
        BaseClusterTreeNode root = getRoot(indexIOSession);
        return new BPlusTreeIndexDeleteOperation(degree, table, indexIOSession).removeIndex(root, identifier);
    }

    @Override
    public int size(int table) throws InterruptedException, ExecutionException, IOException {
        Optional<IndexStorageManager.NodeData> optionalNodeData = this.indexStorageManager.getRoot(table).get();
        if (optionalNodeData.isEmpty())
            return 0;

        BaseClusterTreeNode root = BaseClusterTreeNode.fromNodeData(optionalNodeData.get());
        if (root.isLeaf()){
            return root.getKeyList(degree).size();
        }

        BaseClusterTreeNode curr = root;
        while (!curr.isLeaf()) {
            curr = IndexTreeNodeIO.read(indexStorageManager, table, ((InternalClusterTreeNode) curr).getChildrenList().getFirst());
        }

        int size = curr.getKeyList(degree).size();
        Optional<Pointer> optionalNext = ((LeafClusterTreeNode) curr).getNextSiblingPointer(degree);
        while (optionalNext.isPresent()){
            BaseClusterTreeNode nextNode = IndexTreeNodeIO.read(indexStorageManager, table, optionalNext.get());
            size += nextNode.getKeyList(degree).size();
            optionalNext = ((LeafClusterTreeNode) nextNode).getNextSiblingPointer(degree);
        }

        return size;
    }

    private BaseClusterTreeNode getRoot(IndexIOSession indexIOSession) throws ExecutionException, InterruptedException, IOException {
        Optional<BaseClusterTreeNode> optionalRoot = indexIOSession.getRoot();
        if (optionalRoot.isPresent()){
            return optionalRoot.get();
        }

        byte[] emptyNode = indexStorageManager.getEmptyNode();
        LeafClusterTreeNode leafTreeNode = (LeafClusterTreeNode) BaseClusterTreeNode.fromBytes(emptyNode, BaseClusterTreeNode.Type.LEAF);
        leafTreeNode.setAsRoot();

        IndexStorageManager.NodeData nodeData = indexIOSession.write(leafTreeNode);
        leafTreeNode.setPointer(nodeData.pointer());
        return leafTreeNode;
    }

}
