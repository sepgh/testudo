package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.index.IndexManager;

public interface CollectionIndexManagerProvider {
    IndexManager<?, ?> getIndexManager(int collectionId);
}
