package com.github.sepgh.testudo.index;

public interface UniqueQueryableIndex<K extends Comparable<K>, V>
        extends UniqueTreeIndexManager<K, V>, QueryableIndex<K, V>
{}
