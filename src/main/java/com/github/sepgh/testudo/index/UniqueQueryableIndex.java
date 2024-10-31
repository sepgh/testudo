package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.operation.query.Queryable;

public interface UniqueQueryableIndex<K extends Comparable<K>, V>
        extends UniqueTreeIndexManager<K, V>, Queryable<K, V>
{}
