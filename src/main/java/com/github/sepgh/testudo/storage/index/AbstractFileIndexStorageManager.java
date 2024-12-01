package com.github.sepgh.testudo.storage.index;

import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.utils.KVSize;

import java.nio.file.Path;

public abstract class AbstractFileIndexStorageManager implements IndexStorageManager {
    protected final Path path;
    protected final EngineConfig engineConfig;
    protected final String ROOT_UPDATE_RUNTIME_ERR_STR = "Failed to update root after writing a node. Your header at %s may be broken for: %s";

    public AbstractFileIndexStorageManager(
            EngineConfig engineConfig
    ) {
        this.path = Path.of(engineConfig.getBaseDBPath());
        this.engineConfig = engineConfig;
    }

    protected int getBinarySpace(KVSize size){
        return new BTreeSizeCalculator(this.engineConfig.getBTreeDegree(), size.keySize(), size.valueSize()).calculate();
    }

    protected int getIndexGrowthAllocationSize(KVSize size){
        return engineConfig.getBTreeGrowthNodeAllocationCount() * this.getBinarySpace(size);
    }

    protected Path getHeaderPath() {
        return Path.of(path.toString(), "header.bin");
    }

    @Override
    public byte[] getEmptyNode(KVSize kvSize) {
        return new byte[this.getBinarySpace(kvSize)];
    }
}
