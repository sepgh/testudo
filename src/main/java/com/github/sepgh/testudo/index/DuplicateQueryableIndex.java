package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.operation.query.Queryable;

public interface DuplicateQueryableIndex<K extends Comparable<K>, V extends Number & Comparable<V>>
        extends DuplicateIndexManager<K, V>, Queryable<K, V>
{}
