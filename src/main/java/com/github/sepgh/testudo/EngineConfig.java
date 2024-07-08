package com.github.sepgh.testudo;

import com.github.sepgh.testudo.index.tree.node.data.IntegerImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.index.tree.node.data.LongImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.index.tree.node.data.NoZeroIntegerImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.index.tree.node.data.NoZeroLongImmutableBinaryObjectWrapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import java.util.concurrent.TimeUnit;

@Builder
@Data
@AllArgsConstructor
public class EngineConfig {
    public static int BTREE_UNLIMITED_FILE_SIZE = -1;
    private final int bTreeDegree;
    private final int bTreeGrowthNodeAllocationCount;
    @Builder.Default
    private int fileAcquireTimeout = 10;
    @Builder.Default
    private TimeUnit fileAcquireUnit = TimeUnit.SECONDS;
    @Builder.Default
    private long bTreeMaxFileSize = -1L;
    @Builder.Default
    private boolean indexCache = false;
    @Builder.Default
    private int indexCacheSizePerTable = 1000;
    @Builder.Default
    private IndexIOSessionStrategy indexIOSessionStrategy = IndexIOSessionStrategy.IMMEDIATE;
    @Builder.Default
    private IndexStorageManagerStrategy indexStorageManagerStrategy = IndexStorageManagerStrategy.COMPACT;
    @Builder.Default
    private FileHandlerStrategy fileHandlerStrategy = FileHandlerStrategy.UNLIMITED;
    @Builder.Default
    private int fileHandlerPoolMax = -1;
    @Builder.Default
    private int fileHandlerPoolThreads = -1;
    @Builder.Default
    private String baseDBPath = "/temp";
    @Builder.Default
    private ClusterIndexKeyStrategy clusterIndexKeyStrategy = ClusterIndexKeyStrategy.LONG;
    @Builder.Default
    private OperationMode operationMode = OperationMode.ASYNC;

    public enum OperationMode {
        ASYNC, SYNC
    }

    @Getter
    public enum ClusterIndexKeyStrategy {
        LONG(LongImmutableBinaryObjectWrapper.BYTES),
        LONG_NO_ZERO(NoZeroLongImmutableBinaryObjectWrapper.BYTES),
        INTEGER(IntegerImmutableBinaryObjectWrapper.BYTES),
        INTEGER_NO_ZERO(NoZeroIntegerImmutableBinaryObjectWrapper.BYTES);

        private final int size;

        ClusterIndexKeyStrategy(int size) {
            this.size = size;
        }
    }

    public enum FileHandlerStrategy {
        LIMITED, UNLIMITED
    }
    public enum IndexIOSessionStrategy {
        IMMEDIATE, MEMORY_SNAPSHOT, RECOVERABLE_DISK_SNAPSHOT
    }

    public enum IndexStorageManagerStrategy {
        EXTENDED, COMPACT
    }

    public static class Default {
        public static final int DEFAULT_BTREE_NODE_DEGREE = 4;
        public static final int DEFAULT_BTREE_GROWTH_NODE_ALLOCATION_COUNT = 50;

        public static EngineConfig getDefault(){
            return EngineConfig.builder()
                    .bTreeDegree(DEFAULT_BTREE_NODE_DEGREE)
                    .bTreeGrowthNodeAllocationCount(DEFAULT_BTREE_GROWTH_NODE_ALLOCATION_COUNT)
                    .build();
        }
    }

}
