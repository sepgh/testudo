package com.github.sepgh.internal;

import com.github.sepgh.internal.index.Pointer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@AllArgsConstructor
public class EngineConfig {

    private final int bTreeDegree;
    private final int bTreeGrowthNodeAllocationCount;
    @Builder.Default
    private long bTreeMaxFileSize = -1L;


    public EngineConfig(int bTreeDegree, int bTreeGrowthNodeAllocationCount) {
        this.bTreeDegree = bTreeDegree;
        this.bTreeGrowthNodeAllocationCount = bTreeGrowthNodeAllocationCount;
    }

    public EngineConfig(int bTreeDegree, int bTreeGrowthNodeAllocationCount, long bTreeMaxFileSize) {
        this.bTreeDegree = bTreeDegree;
        this.bTreeGrowthNodeAllocationCount = bTreeGrowthNodeAllocationCount;
        this.bTreeMaxFileSize = bTreeMaxFileSize;
    }

    @Builder.Default
    private Integer cachedPaddingSize = null;

    public int getPaddedSize(){
        if (this.cachedPaddingSize != null){
            return this.cachedPaddingSize;
        }
        int i = this.bTreeNodeSize() % 8;
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
        return 1 + (this.getBTreeDegree() * (Long.BYTES + Pointer.BYTES)) + (2 * Pointer.BYTES);
    }

    public static class Default {
        public static final int DEFAULT_BTREE_NODE_DEGREE = 4;
        public static final int DEFAULT_BTREE_GROWTH_NODE_ALLOCATION_COUNT = 10;

        public static EngineConfig getDefault(){
            return EngineConfig.builder()
                    .bTreeDegree(DEFAULT_BTREE_NODE_DEGREE)
                    .bTreeGrowthNodeAllocationCount(DEFAULT_BTREE_GROWTH_NODE_ALLOCATION_COUNT)
                    .build();
        }
    }

}
