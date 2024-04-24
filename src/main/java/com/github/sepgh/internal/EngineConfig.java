package com.github.sepgh.internal;

import com.github.sepgh.internal.tree.Pointer;

public class EngineConfig {
    public static final int BTREE_NODE_MAX_KEY = 10;
    public static final int BTREE_NODE_SIZE = 1 + (BTREE_NODE_MAX_KEY * (Long.BYTES + Pointer.POINTER_SIZE)) + (3 * Pointer.POINTER_SIZE); //250
    public static final int BTREE_GROWTH_NODE_ALLOCATION_COUNT = 10;
    private static Integer cachedPaddingSize = null;

    public static int PaddedSize(){
        if (cachedPaddingSize != null){
            return cachedPaddingSize;
        }
        int i = (BTREE_NODE_SIZE + 8) % 8;
        if (i == 0){
            cachedPaddingSize = BTREE_NODE_SIZE;
            return cachedPaddingSize;
        }
        cachedPaddingSize = BTREE_NODE_SIZE + 8 - i;
        return cachedPaddingSize;
    }

    public static long maxIndexFileSize() {
        return Double.valueOf(Math.pow(1024, 3) * PaddedSize()).longValue();
    }

    public static int indexGrowthAllocationSize() {
        return BTREE_GROWTH_NODE_ALLOCATION_COUNT * PaddedSize();
    }
}
