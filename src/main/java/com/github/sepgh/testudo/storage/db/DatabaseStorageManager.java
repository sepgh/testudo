package com.github.sepgh.testudo.storage.db;

import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.functional.DBObjectUpdateConsumer;

import java.util.Optional;


public interface DatabaseStorageManager {
    Pointer store(int scheme, int collectionId, int version, byte[] data) throws InternalOperationException;
    void update(Pointer pointer, DBObjectUpdateConsumer<DBObject> dbObjectConsumer) throws InternalOperationException;
    void update(Pointer pointer, byte[] bytes) throws InternalOperationException;
    Optional<DBObject> select(Pointer pointer);
    void remove(Pointer pointer) throws InternalOperationException;
    default void close(){}
}
