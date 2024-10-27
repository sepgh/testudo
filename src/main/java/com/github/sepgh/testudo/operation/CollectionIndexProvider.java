package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.index.DuplicateIndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.UniqueQueryableIndex;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.scheme.Scheme;


public interface CollectionIndexProvider {
    UniqueQueryableIndex<?, ? extends Number> getUniqueIndexManager(Scheme.Field field);
    DuplicateIndexManager<?, ? extends Number> getDuplicateIndexManager(Scheme.Field field);
    UniqueTreeIndexManager<?, Pointer> getClusterIndexManager();
}
