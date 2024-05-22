package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.tree.node.BaseTreeNode;
import com.github.sepgh.internal.tree.node.InternalTreeNode;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class TreeNodeIO {
    public static CompletableFuture<IndexStorageManager.NodeData> write(BaseTreeNode node, IndexStorageManager indexStorageManager, int table) throws IOException, ExecutionException, InterruptedException {
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
            indexStorageManager.updateNode(table, node.getData(), node.getPointer()).whenComplete((integer, throwable) -> {
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

    public static void update(IndexStorageManager indexStorageManager, int table, BaseTreeNode... nodes) throws InterruptedException {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        CountDownLatch latch = new CountDownLatch(nodes.length);
        for (BaseTreeNode node : nodes) {
            indexStorageManager.updateNode(table, node.getData(), node.getPointer()).whenComplete((integer, throwable) -> {
                if (throwable != null) {
                    completableFuture.completeExceptionally(throwable);
                }

                latch.countDown();
            });
        }
        latch.await();
    }

    public static void remove(IndexStorageManager indexStorageManager, int table, BaseTreeNode node) throws ExecutionException, InterruptedException {
        indexStorageManager.removeNode(table, node.getPointer()).get();
    }
}
