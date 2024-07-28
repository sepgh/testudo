package com.github.sepgh.testudo.storage.db;

import com.github.sepgh.testudo.index.Pointer;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;


interface DatabaseStorageManager {
    Pointer store(int collectionId, byte[] data) throws IOException, InterruptedException, ExecutionException;
    void update(Pointer pointer, Consumer<DBObject> dbObjectConsumer) throws IOException, ExecutionException, InterruptedException;
    Optional<DBObject> select(Pointer pointer);
    void remove(Pointer pointer) throws IOException, ExecutionException, InterruptedException;
    default void close(){}
}
