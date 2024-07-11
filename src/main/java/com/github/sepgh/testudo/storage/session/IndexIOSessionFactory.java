package com.github.sepgh.testudo.storage.session;

import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.storage.IndexStorageManager;

public abstract class IndexIOSessionFactory {
    public abstract <K extends Comparable<K>> IndexIOSession<K> create(IndexStorageManager indexStorageManager, int indexId, NodeFactory<K> nodeFactory);
}