package com.github.sepgh.internal.storage;

import com.github.sepgh.internal.storage.exception.ChunkIsFullException;

import java.io.IOException;
import java.util.concurrent.Future;

public interface IndexStorageManager {
    Future<byte[]> readNode(int table, int position, int chunk);
    Future<Long> allocateForNewNode(int table, int chunk) throws IOException, ChunkIsFullException;
    boolean chunkHasSpaceForNode(int chunk) throws IOException;
    void close();
}
