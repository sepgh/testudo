package com.github.sepgh.internal.tree;

import java.nio.file.Path;

public class BPlusTree {
    private final int nodeSize;
    private final int keySize;
    private final Path filePath;

    /**
     * @param nodeSize maximum number of keys per node
     * @param keySize size of each key in bytes (defaults to 4 for integer)
     */
    public BPlusTree(Path filePath, int nodeSize, int keySize) {
        this.nodeSize = nodeSize;
        this.keySize = keySize;
        this.filePath = filePath;
    }

    public BPlusTree(Path path, int nodeSize) {
        this(path, nodeSize, 4);
    }



}
