package com.github.sepgh.testudo.index;

public class DuplicateIndexManagerDecorator<K extends Comparable<K>, V> extends IndexManagerDecorator<K, V> {

    public DuplicateIndexManagerDecorator(IndexManager<K, V> indexManager) {
        super(indexManager);
    }


}
