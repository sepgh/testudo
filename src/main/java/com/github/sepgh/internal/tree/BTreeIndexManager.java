package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.exception.ChunkIsFullException;
import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;
import com.github.sepgh.internal.tree.exception.IllegalNodeAccess;
import com.github.sepgh.internal.tree.node.AbstractTreeNode;
import com.github.sepgh.internal.tree.node.LeafTreeNode;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class BTreeIndexManager implements IndexManager {

    private final IndexStorageManager indexStorageManager;
    private final HeaderManager headerManager;
    private final int table;
    private AbstractTreeNode root;

    public BTreeIndexManager(int table, HeaderManager headerManager, IndexStorageManager indexStorageManager){
        this.table = table;
        this.indexStorageManager = indexStorageManager;
        this.headerManager = headerManager;
        initialize();
    }

    @SneakyThrows
    private void initialize(){
        Optional<Pointer> optionalRootPointer = this.indexStorageManager.getRoot(table);
        if (optionalRootPointer.isEmpty())
            return;

        Pointer pointer = optionalRootPointer.get();
        Future<byte[]> node = indexStorageManager.readNode(table, pointer);
        byte[] bytes = node.get();
        this.root = AbstractTreeNode.fromBytes(bytes);
        this.root.setNodePointer(pointer);
    }

    @Override
    public CompletableFuture<AbstractTreeNode> addIndex(long identifier, Pointer pointer) {

        if (this.root == null){
            return this.fillRoot(identifier, pointer);
        }

        return null;
    }

    @Override
    public CompletableFuture<Optional<AbstractTreeNode>> getIndex(long identifier) {
        return null;
    }

    @Override
    public CompletableFuture<Void> removeIndex(long identifier) {
        return null;
    }


    private CompletableFuture<AbstractTreeNode> fillRoot(long identifier, Pointer pointer) {
        CompletableFuture<AbstractTreeNode> answer = new CompletableFuture<>();

        List<Header.IndexChunk> chunks = this.headerManager.getHeader().getTableOfId(table).get().getChunks();
        for (int i = 0; i < chunks.size(); i++){
            CompletableFuture<IndexStorageManager.AllocationResult> allocationFuture = null;

            try {
                allocationFuture = this.indexStorageManager.allocateForNewNode(this.table, chunks.get(i).getChunk());
            } catch (IOException e) {
                answer.completeExceptionally(e);
                return answer;
            } catch (ChunkIsFullException e) {
                continue;
            }

            int finalChunk = i;
            allocationFuture.whenComplete((allocationResult, throwable) -> {
                if (throwable != null){
                    answer.completeExceptionally(throwable);
                    return;
                }

                LeafTreeNode treeNode = new LeafTreeNode(new byte[allocationResult.size()]);
                treeNode.setType(AbstractTreeNode.TYPE_LEAF_NODE);
                try {
                    treeNode.setKeyValue(0, identifier, pointer);
                } catch (IllegalNodeAccess e) {
                    answer.completeExceptionally(e);
                }

                indexStorageManager.writeNode(table, treeNode.toBytes(), allocationResult.position(), finalChunk).whenComplete((integer, throwable1) -> {
                    if (throwable1 != null){
                        answer.completeExceptionally(throwable1);
                        return;
                    }
                    root = treeNode;
                    answer.complete(treeNode);
                });

            });
            return answer;
        }

        // Todo: make new chunk and try again (at the specific chunk)?
        return null;
    }
}
