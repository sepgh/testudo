package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.scheme.Scheme;

public interface CollectionIndexProviderFactory {
    CollectionIndexProvider create(Scheme.Collection collection);
}
