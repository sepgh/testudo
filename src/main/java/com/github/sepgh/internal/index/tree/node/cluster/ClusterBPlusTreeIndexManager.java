package com.github.sepgh.internal.index.tree.node.cluster;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.BPlusTreeIndexManager;
import com.github.sepgh.internal.index.tree.node.NodeFactory;
import com.github.sepgh.internal.index.tree.node.data.BinaryObjectWrapper;
import com.github.sepgh.internal.index.tree.node.data.PointerBinaryObjectWrapper;
import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.session.ImmediateCommitIndexIOSession;
import com.github.sepgh.internal.storage.session.IndexIOSessionFactory;

public class ClusterBPlusTreeIndexManager<K extends Comparable<K>, V extends Comparable<V>> extends BPlusTreeIndexManager<K, Pointer> {
    public ClusterBPlusTreeIndexManager(int degree, IndexStorageManager indexStorageManager, IndexIOSessionFactory indexIOSessionFactory, BinaryObjectWrapper<K> keyStrategy){
        super(degree, indexStorageManager, indexIOSessionFactory, keyStrategy, new PointerBinaryObjectWrapper(), new NodeFactory.ClusterNodeFactory<>(keyStrategy));

    }

    public ClusterBPlusTreeIndexManager(int degree, IndexStorageManager indexStorageManager, BinaryObjectWrapper<K> keyStrategy){
        this(degree, indexStorageManager, ImmediateCommitIndexIOSession.Factory.getInstance(), keyStrategy);
    }

}
