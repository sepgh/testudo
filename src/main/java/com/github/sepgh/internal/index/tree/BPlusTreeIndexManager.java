package com.github.sepgh.internal.index.tree;

import com.github.sepgh.internal.index.IndexManager;
import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.BaseTreeNode;
import com.github.sepgh.internal.index.tree.node.InternalTreeNode;
import com.github.sepgh.internal.index.tree.node.LeafTreeNode;
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
    public BaseTreeNode addIndex(int table, long identifier, Pointer pointer) throws ExecutionException, InterruptedException, IOException {
        IndexIOSession indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, table);
        BaseTreeNode root = getRoot(indexIOSession);
        return new BPlusTreeIndexCreateOperation(degree, table, indexIOSession).addIndex(root, identifier, pointer);
    }

    @Override
    public Optional<Pointer> getIndex(int table, long identifier) throws ExecutionException, InterruptedException, IOException {
        IndexIOSession indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, table);

        LeafTreeNode baseTreeNode = BPlusTreeUtils.getResponsibleNode(indexStorageManager, getRoot(indexIOSession), identifier, table, degree);
        for (LeafTreeNode.KeyValue entry : baseTreeNode.getKeyValueList(degree)) {
            if (entry.key() == identifier)
                return Optional.of(entry.value());
        }

        return Optional.empty();
    }

    @Override
    public boolean removeIndex(int table, long identifier) throws ExecutionException, InterruptedException, IOException {
        IndexIOSession indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, table);
        BaseTreeNode root = getRoot(indexIOSession);
        return new BPlusTreeIndexDeleteOperation(degree, table, indexIOSession).removeIndex(root, identifier);
    }

    @Override
    public int size(int table) throws InterruptedException, ExecutionException {
        Optional<IndexStorageManager.NodeData> optionalNodeData = this.indexStorageManager.getRoot(table).get();
        if (optionalNodeData.isEmpty())
            return 0;

        BaseTreeNode root = BaseTreeNode.fromNodeData(optionalNodeData.get());
        if (root.isLeaf()){
            return root.getKeyList(degree).size();
        }

        BaseTreeNode curr = root;
        while (!curr.isLeaf()) {
            curr = IndexTreeNodeIO.read(indexStorageManager, table, ((InternalTreeNode) curr).getChildrenList().getFirst());
        }

        int size = curr.getKeyList(degree).size();
        Optional<Pointer> optionalNext = ((LeafTreeNode) curr).getNextSiblingPointer(degree);
        while (optionalNext.isPresent()){
            BaseTreeNode nextNode = IndexTreeNodeIO.read(indexStorageManager, table, optionalNext.get());
            size += nextNode.getKeyList(degree).size();
            optionalNext = ((LeafTreeNode) nextNode).getNextSiblingPointer(degree);
        }

        return size;
    }

    private BaseTreeNode getRoot(IndexIOSession indexIOSession) throws ExecutionException, InterruptedException, IOException {
        Optional<BaseTreeNode> optionalRoot = indexIOSession.getRoot();
        if (optionalRoot.isPresent()){
            return optionalRoot.get();
        }

        byte[] emptyNode = indexStorageManager.getEmptyNode();
        LeafTreeNode leafTreeNode = (LeafTreeNode) BaseTreeNode.fromBytes(emptyNode, BaseTreeNode.Type.LEAF);
        leafTreeNode.setAsRoot();

        IndexStorageManager.NodeData nodeData = indexIOSession.write(leafTreeNode);
        leafTreeNode.setPointer(nodeData.pointer());
        return leafTreeNode;
    }

}
