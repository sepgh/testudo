package com.github.sepgh.internal.index.tree;

import com.github.sepgh.internal.index.IndexManager;
import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.BaseTreeNode;
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
        BaseTreeNode root = getRoot(table);
        IndexIOSession indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, table);
        return new BPlusTreeIndexCreateOperation(degree, table, indexIOSession).addIndex(root, identifier, pointer);
    }

    @Override
    public Optional<Pointer> getIndex(int table, long identifier) throws ExecutionException, InterruptedException {
        LeafTreeNode baseTreeNode = BPlusTreeUtils.getResponsibleNode(indexStorageManager, getRoot(table), identifier, table, degree);
        for (LeafTreeNode.KeyValue entry : baseTreeNode.getKeyValueList(degree)) {
            if (entry.key() == identifier)
                return Optional.of(entry.value());
        }

        return Optional.empty();
    }

    @Override
    public boolean removeIndex(int table, long identifier) throws ExecutionException, InterruptedException, IOException {
        BaseTreeNode root = getRoot(table);
        IndexIOSession indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, table);
        return new BPlusTreeIndexDeleteOperation(degree, table, indexIOSession).removeIndex(root, identifier);
    }

    private BaseTreeNode getRoot(int table) throws ExecutionException, InterruptedException {
        Optional<IndexStorageManager.NodeData> optionalNodeData = indexStorageManager.getRoot(table).get();
        if (optionalNodeData.isPresent()){
            return IndexTreeNodeIO.read(indexStorageManager, table, optionalNodeData.get().pointer());
        }

        byte[] emptyNode = indexStorageManager.getEmptyNode();
        LeafTreeNode leafTreeNode = (LeafTreeNode) BaseTreeNode.fromBytes(emptyNode, BaseTreeNode.Type.LEAF);
        leafTreeNode.setAsRoot();

        IndexStorageManager.NodeData nodeData = indexStorageManager.fillRoot(table, leafTreeNode.getData()).get();
        leafTreeNode.setPointer(nodeData.pointer());
        return leafTreeNode;
    }


}
