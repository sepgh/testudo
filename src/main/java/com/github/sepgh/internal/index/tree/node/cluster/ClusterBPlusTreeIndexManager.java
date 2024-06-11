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
    public ClusterBPlusTreeIndexManager(int degree, IndexStorageManager indexStorageManager, IndexIOSessionFactory indexIOSessionFactory, BinaryObjectWrapper<K> keyBinaryObjectWrapper){
        super(degree, indexStorageManager, indexIOSessionFactory, keyBinaryObjectWrapper, new PointerBinaryObjectWrapper(), new NodeFactory.ClusterNodeFactory<>(keyBinaryObjectWrapper));

    }

    public ClusterBPlusTreeIndexManager(int degree, IndexStorageManager indexStorageManager, BinaryObjectWrapper<K> keyBinaryObjectWrapper){
        this(degree, indexStorageManager, ImmediateCommitIndexIOSession.Factory.getInstance(), keyBinaryObjectWrapper);
    }

}
