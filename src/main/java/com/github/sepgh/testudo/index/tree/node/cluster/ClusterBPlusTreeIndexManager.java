package com.github.sepgh.testudo.index.tree.node.cluster;

import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.BPlusTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.index.tree.node.data.PointerImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.storage.IndexStorageManager;
import com.github.sepgh.testudo.storage.session.ImmediateCommitIndexIOSession;
import com.github.sepgh.testudo.storage.session.IndexIOSessionFactory;

public class ClusterBPlusTreeIndexManager<K extends Comparable<K>> extends BPlusTreeIndexManager<K, Pointer> {
    public ClusterBPlusTreeIndexManager(int degree, IndexStorageManager indexStorageManager, IndexIOSessionFactory indexIOSessionFactory, ImmutableBinaryObjectWrapper<K> keyImmutableBinaryObjectWrapper){
        super(degree, indexStorageManager, indexIOSessionFactory, keyImmutableBinaryObjectWrapper, new PointerImmutableBinaryObjectWrapper(), new NodeFactory.ClusterNodeFactory<>(keyImmutableBinaryObjectWrapper));
    }

    public ClusterBPlusTreeIndexManager(int degree, IndexStorageManager indexStorageManager, ImmutableBinaryObjectWrapper<K> keyImmutableBinaryObjectWrapper){
        this(degree, indexStorageManager, ImmediateCommitIndexIOSession.Factory.getInstance(), keyImmutableBinaryObjectWrapper);
    }

}
