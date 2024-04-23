package com.github.sepgh.internal.tree;

import java.util.Optional;
import java.util.concurrent.Future;

public interface IndexManager {
    Future<Void> addIndex(long identifier, Pointer pointer);
    Future<Optional<Pointer>> getIndex(long identifier);
    Future<Void> removeIndex(long identifier);
}
