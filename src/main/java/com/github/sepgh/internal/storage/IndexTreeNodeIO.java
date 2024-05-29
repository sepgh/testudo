package com.github.sepgh.internal.storage;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.BaseTreeNode;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class IndexTreeNodeIO {
    public static CompletableFuture<IndexStorageManager.NodeData> write(IndexStorageManager indexStorageManager, int table, BaseTreeNode node) throws IOException, ExecutionException, InterruptedException {
        IndexStorageManager.NodeData nodeData = new IndexStorageManager.NodeData(node.getPointer(), node.getData());
        if (!node.isModified() && node.getPointer() != null){
            return CompletableFuture.completedFuture(nodeData);
        }
        CompletableFuture<IndexStorageManager.NodeData> output = new CompletableFuture<>();

        if (node.getPointer() == null){
            indexStorageManager.writeNewNode(table, node.getData(), node.isRoot()).whenComplete((nodeData1, throwable) -> {
                if (throwable != null){
                    output.completeExceptionally(throwable);
                    return;
                }
                node.setPointer(nodeData1.pointer());
                output.complete(nodeData1);
            });
        } else {
            indexStorageManager.updateNode(table, node.getData(), node.getPointer(), node.isRoot()).whenComplete((integer, throwable) -> {
                if (throwable != null){
                    output.completeExceptionally(throwable);
                    return;
                }
                output.complete(nodeData);
            });
        }
        return output;
    }

    public static BaseTreeNode read(IndexStorageManager indexStorageManager, int table, Pointer pointer) throws ExecutionException, InterruptedException {
        return BaseTreeNode.fromNodeData(indexStorageManager.readNode(table, pointer).get());
    }

    public static void update(IndexStorageManager indexStorageManager, int table, BaseTreeNode... nodes) throws InterruptedException, IOException {
        CountDownLatch latch = new CountDownLatch(nodes.length);
        for (BaseTreeNode node : nodes) {
            indexStorageManager.updateNode(table, node.getData(), node.getPointer(), node.isRoot()).whenComplete((integer, throwable) -> latch.countDown());
        }
        latch.await();
    }

    public static void remove(IndexStorageManager indexStorageManager, int table, BaseTreeNode node) throws ExecutionException, InterruptedException {
        remove(indexStorageManager, table, node.getPointer());
    }

    public static void remove(IndexStorageManager indexStorageManager, int table, Pointer pointer) throws ExecutionException, InterruptedException {
        indexStorageManager.removeNode(table, pointer).get();
    }
}
