package com.github.sepgh.testudo.index;

public interface DuplicateQueryableIndex<K extends Comparable<K>, V extends Number & Comparable<V>>
        extends DuplicateIndexManager<K, V>, QueryableIndex<K, V>
{}
