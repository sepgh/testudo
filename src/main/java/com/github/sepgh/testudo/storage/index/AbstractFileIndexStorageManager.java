package com.github.sepgh.testudo.storage.index;

import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManager;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManagerFactory;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.FileHandlerPool;
import com.github.sepgh.testudo.storage.pool.ManagedFileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import com.github.sepgh.testudo.utils.FileUtils;
import com.github.sepgh.testudo.utils.KVSize;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.testudo.index.tree.node.AbstractTreeNode.TYPE_INTERNAL_NODE_BIT;
import static com.github.sepgh.testudo.index.tree.node.AbstractTreeNode.TYPE_LEAF_NODE_BIT;

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
