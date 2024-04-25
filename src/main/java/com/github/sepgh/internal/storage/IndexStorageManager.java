package com.github.sepgh.internal.storage;

import com.github.sepgh.internal.storage.exception.ChunkIsFullException;
import com.github.sepgh.internal.tree.Pointer;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface IndexStorageManager {
    Optional<Pointer> getRoot(int table);
    default CompletableFuture<byte[]> readNode(int table, Pointer pointer) {
        return this.readNode(table, pointer.position(), pointer.chunk());
    }
    CompletableFuture<byte[]> readNode(int table, long position, int chunk);
    CompletableFuture<AllocationResult> allocateForNewNode(int table, int chunk) throws IOException, ChunkIsFullException;
    boolean chunkHasSpaceForNode(int chunk) throws IOException;

    CompletableFuture<Integer> writeNode(int table, byte[] data, long position, int chunk);

    void close();

    record AllocationResult(long position, int size) {
    }
}
