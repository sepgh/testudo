package com.github.sepgh.internal.storage.session;

import com.github.sepgh.internal.index.tree.node.NodeFactory;
import com.github.sepgh.internal.storage.IndexStorageManager;

public abstract class IndexIOSessionFactory {
    public abstract <K extends Comparable<K>> IndexIOSession<K> create(IndexStorageManager indexStorageManager, int table, NodeFactory<K> nodeFactory);
}