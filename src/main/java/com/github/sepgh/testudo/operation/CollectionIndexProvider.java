package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.index.DuplicateIndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.scheme.Scheme;


public interface CollectionIndexProvider {
    UniqueTreeIndexManager<?, ?> getUniqueIndexManager(Scheme.Field field);
    DuplicateIndexManager<?, ?> getDuplicateIndexManager(Scheme.Field field);
    UniqueTreeIndexManager<?, Pointer> getClusterIndexManager();
}
