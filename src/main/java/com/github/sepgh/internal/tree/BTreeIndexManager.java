package com.github.sepgh.internal.tree;

import lombok.AllArgsConstructor;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.Future;

@AllArgsConstructor
public class BTreeIndexManager implements IndexManager {

    private final Path path;
    private TreeNode head;

    @Override
    public Future<Void> addIndex(long identifier, Pointer pointer) {
        return null;
    }

    @Override
    public Future<Optional<Pointer>> getIndex(long identifier) {
        return null;
    }

    @Override
    public Future<Void> removeIndex(long identifier) {
        return null;
    }
}
