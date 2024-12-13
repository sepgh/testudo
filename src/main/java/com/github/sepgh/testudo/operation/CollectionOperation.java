package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.scheme.Scheme;
import lombok.Getter;

@Getter
public abstract class CollectionOperation {
    protected final Scheme.Collection collection;

    protected CollectionOperation(Scheme.Collection collection) {
        this.collection = collection;
    }

    public abstract CollectionSelectOperation<?> select();
    public abstract CollectionUpdateOperation<?> update();
    public abstract CollectionDeleteOperation<?> delete();
    public abstract CollectionInsertOperation<?> insert();
}
