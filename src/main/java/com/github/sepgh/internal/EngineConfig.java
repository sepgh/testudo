package com.github.sepgh.internal;

import com.github.sepgh.internal.tree.Pointer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
@AllArgsConstructor
public class EngineConfig {

    private final int bTreeNodeMaxKey;
    private final int bTreeGrowthNodeAllocationCount;
    private long bTreeMaxFileSize = -1L;


    public EngineConfig(int bTreeNodeMaxKey, int bTreeGrowthNodeAllocationCount) {
        this.bTreeNodeMaxKey = bTreeNodeMaxKey;
        this.bTreeGrowthNodeAllocationCount = bTreeGrowthNodeAllocationCount;
    }

    public EngineConfig(int bTreeNodeMaxKey, int bTreeGrowthNodeAllocationCount, long bTreeMaxFileSize) {
        this.bTreeNodeMaxKey = bTreeNodeMaxKey;
        this.bTreeGrowthNodeAllocationCount = bTreeGrowthNodeAllocationCount;
        this.bTreeMaxFileSize = bTreeMaxFileSize;
    }

    @Builder.Default
    private Integer cachedPaddingSize = null;

    public int getPaddedSize(){
        if (this.cachedPaddingSize != null){
            return this.cachedPaddingSize;
        }
        int i = (this.bTreeNodeSize() + 8) % 8;
        if (i == 0){
            cachedPaddingSize = this.bTreeNodeSize();
            return cachedPaddingSize;
        }
        cachedPaddingSize = this.bTreeNodeSize() + 8 - i;
        return cachedPaddingSize;
    }

    public long getBTreeMaxFileSize() {
        if (this.bTreeMaxFileSize == -1)
            return Double.valueOf(Math.pow(1024, 3) * getPaddedSize()).longValue();
        else
            return this.bTreeMaxFileSize;
    }

    public int indexGrowthAllocationSize() {
        return this.bTreeGrowthNodeAllocationCount * getPaddedSize();
    }

    public int bTreeNodeSize(){
        return 1 + (this.getBTreeNodeMaxKey() * (Long.BYTES + Pointer.POINTER_SIZE)) + (3 * Pointer.POINTER_SIZE);
    }

    public static class Default {
        public static final int DEFAULT_BTREE_NODE_MAX_KEY = 10;
        public static final int DEFAULT_BTREE_GROWTH_NODE_ALLOCATION_COUNT = 10;

        public static EngineConfig getDefault(){
            return EngineConfig.builder()
                    .bTreeNodeMaxKey(DEFAULT_BTREE_NODE_MAX_KEY)
                    .bTreeGrowthNodeAllocationCount(DEFAULT_BTREE_GROWTH_NODE_ALLOCATION_COUNT)
                    .build();
        }
    }

}
