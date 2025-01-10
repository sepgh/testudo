package com.github.sepgh.testudo.storage.index.header;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public abstract class IndexHeaderManagerSingletonFactory {
    private final Map<Path, IndexHeaderManager> indexHeaderManagers = new HashMap<>();

    public final synchronized IndexHeaderManager getInstance(Path path) {
        return indexHeaderManagers.computeIfAbsent(path, this::create);
    }

    protected abstract IndexHeaderManager create(Path path);
}
