package com.github.sepgh.internal;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.data.Identifier;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.concurrent.TimeUnit;

@Builder
@Data
@AllArgsConstructor
public class EngineConfig {

    private final int bTreeDegree;
    private final int bTreeGrowthNodeAllocationCount;
    @Builder.Default
    private int fileAcquireTimeout = 10;
    @Builder.Default
    private TimeUnit fileAcquireUnit = TimeUnit.SECONDS;
    @Builder.Default
    private long bTreeMaxFileSize = -1L;

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
        return 1 + (this.getBTreeDegree() * (Identifier.BYTES + Pointer.BYTES)) + (2 * Pointer.BYTES);
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
