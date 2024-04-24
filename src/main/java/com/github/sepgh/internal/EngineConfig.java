package com.github.sepgh.internal;

import com.github.sepgh.internal.tree.Pointer;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class EngineConfig {

    private final int bTreeNodeMaxKey;
    private final int bTreeNodeSize;
    private final int bTreeGrowthNodeAllocationCount;

    public EngineConfig(int bTreeNodeMaxKey, int bTreeNodeSize, int bTreeGrowthNodeAllocationCount) {
        this.bTreeNodeMaxKey = bTreeNodeMaxKey;
        this.bTreeNodeSize = bTreeNodeSize;
        this.bTreeGrowthNodeAllocationCount = bTreeGrowthNodeAllocationCount;
    }

    @Builder.Default
    private Integer cachedPaddingSize = null;

    public int getPaddedSize(){
        if (this.cachedPaddingSize != null){
            return this.cachedPaddingSize;
        }
        int i = (this.bTreeNodeSize + 8) % 8;
        if (i == 0){
            cachedPaddingSize = this.bTreeNodeSize;
            return cachedPaddingSize;
        }
        cachedPaddingSize = this.bTreeNodeSize + 8 - i;
        return cachedPaddingSize;
    }

    public long maxIndexFileSize() {
        return Double.valueOf(Math.pow(1024, 3) * getPaddedSize()).longValue();
    }

    public int indexGrowthAllocationSize() {
        return this.bTreeGrowthNodeAllocationCount * getPaddedSize();
    }

    public static class Default {
        public static final int DEFAULT_BTREE_NODE_MAX_KEY = 10;
        public static final int DEFAULT_BTREE_NODE_SIZE = 1 + (DEFAULT_BTREE_NODE_MAX_KEY * (Long.BYTES + Pointer.POINTER_SIZE)) + (3 * Pointer.POINTER_SIZE); //250
        public static final int DEFAULT_BTREE_GROWTH_NODE_ALLOCATION_COUNT = 10;

        public static EngineConfig getDefault(){
            return EngineConfig.builder()
                    .bTreeNodeMaxKey(DEFAULT_BTREE_NODE_SIZE)
                    .bTreeGrowthNodeAllocationCount(DEFAULT_BTREE_GROWTH_NODE_ALLOCATION_COUNT)
                    .bTreeNodeSize(DEFAULT_BTREE_NODE_SIZE)
                    .build();
        }
    }

}
