package com.github.sepgh.testudo.index.tree.node.cluster;

import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.index.data.PointerIndexBinaryObject;
import com.github.sepgh.testudo.index.tree.BPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.session.ImmediateCommitIndexIOSession;
import com.github.sepgh.testudo.storage.index.session.IndexIOSessionFactory;

public class ClusterBPlusTreeUniqueTreeIndexManager<K extends Comparable<K>> extends BPlusTreeUniqueTreeIndexManager<K, Pointer> {
    public ClusterBPlusTreeUniqueTreeIndexManager(int index, int degree, IndexStorageManager indexStorageManager, IndexIOSessionFactory indexIOSessionFactory, IndexBinaryObjectFactory<K> keyIndexBinaryObjectFactory){
        super(index, degree, indexStorageManager, indexIOSessionFactory, keyIndexBinaryObjectFactory, new PointerIndexBinaryObject.Factory(), new NodeFactory.ClusterNodeFactory<>(keyIndexBinaryObjectFactory));
    }

    public ClusterBPlusTreeUniqueTreeIndexManager(int index, int degree, IndexStorageManager indexStorageManager, IndexBinaryObjectFactory<K> keyIndexBinaryObjectFactory){
        this(index, degree, indexStorageManager, ImmediateCommitIndexIOSession.Factory.getInstance(), keyIndexBinaryObjectFactory);
    }

}
