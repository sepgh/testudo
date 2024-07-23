package com.github.sepgh.testudo.index;

public abstract class AbstractIndexManager<K extends Comparable<K>, V extends Comparable<V>> implements IndexManager<K,V> {
    protected final int indexId;

    public AbstractIndexManager(int indexId) {
        this.indexId = indexId;
    }

    @Override
    public int getIndexId() {
        return this.indexId;
    }
}
