package com.github.sepgh.internal.storage.session;

import com.github.sepgh.internal.storage.IndexStorageManager;

public abstract class IndexIOSessionFactory {
    public abstract IndexIOSession create(IndexStorageManager indexStorageManager, int table);
}