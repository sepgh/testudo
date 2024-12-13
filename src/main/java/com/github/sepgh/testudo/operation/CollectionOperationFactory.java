package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManagerFactory;

public class CollectionOperationFactory {
    protected final Scheme scheme;
    protected final Scheme.Collection collection;
    protected final ReaderWriterLockPool readerWriterLockPool = ReaderWriterLockPool.getInstance();
    protected final CollectionIndexProviderFactory collectionIndexProviderFactory;
    protected final DatabaseStorageManagerFactory databaseStorageManagerFactory;

    public CollectionOperationFactory(Scheme scheme, Scheme.Collection collection, CollectionIndexProviderFactory collectionIndexProviderFactory, DatabaseStorageManagerFactory databaseStorageManagerFactory) {
        this.scheme = scheme;
        this.collection = collection;
        this.collectionIndexProviderFactory = collectionIndexProviderFactory;
        this.databaseStorageManagerFactory = databaseStorageManagerFactory;
    }

    public CollectionOperation create() {
        return new CollectionOperation(collection) {
            @Override
            public CollectionSelectOperation<?> select() {
                return new DefaultCollectionSelectOperation<>(
                        collection,
                        readerWriterLockPool.getReaderWriterLock(scheme, collection),
                        collectionIndexProviderFactory.create(collection),
                        databaseStorageManagerFactory.create()
                );
            }

            @Override
            public CollectionUpdateOperation<?> update() {
                return new DefaultCollectionUpdateOperation<>(
                        collection,
                        readerWriterLockPool.getReaderWriterLock(scheme, collection),
                        collectionIndexProviderFactory.create(collection),
                        databaseStorageManagerFactory.create()
                );
            }

            @Override
            public CollectionDeleteOperation<?> delete() {
                return new DefaultCollectionDeleteOperation<>(
                        collection,
                        readerWriterLockPool.getReaderWriterLock(scheme, collection),
                        collectionIndexProviderFactory.create(collection),
                        databaseStorageManagerFactory.create()
                );
            }

            @Override
            public CollectionInsertOperation<?> insert() {
                return new DefaultCollectionInsertOperation<>(
                        scheme,
                        collection,
                        readerWriterLockPool.getReaderWriterLock(scheme, collection),
                        collectionIndexProviderFactory.create(collection),
                        databaseStorageManagerFactory.create()
                );
            }
        };
    }
}
