package com.github.sepgh.testudo.storage.db;

import com.github.sepgh.testudo.exception.VerificationException;
import com.github.sepgh.testudo.index.Pointer;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;


public interface DatabaseStorageManager {
    Pointer store(int scheme, int collectionId, int version, byte[] data) throws IOException, InterruptedException, ExecutionException;
    void update(Pointer pointer, Consumer<DBObject> dbObjectConsumer) throws IOException, ExecutionException, InterruptedException;
    void update(Pointer pointer, byte[] bytes) throws IOException, ExecutionException, InterruptedException, VerificationException.InvalidDBObjectWrapper;
    Optional<DBObject> select(Pointer pointer);
    void remove(Pointer pointer) throws IOException, ExecutionException, InterruptedException;
    default void close(){}  // Todo: implementation is missing. not needed?
}
