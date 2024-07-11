package com.github.sepgh.testudo.storage;

import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class IndexTreeNodeIO {
    public static CompletableFuture<IndexStorageManager.NodeData> write(IndexStorageManager indexStorageManager, int indexId, AbstractTreeNode<?> node) throws IOException, ExecutionException, InterruptedException {
        IndexStorageManager.NodeData nodeData = new IndexStorageManager.NodeData(node.getPointer(), node.getData());
        if (!node.isModified() && node.getPointer() != null){
            return CompletableFuture.completedFuture(nodeData);
        }
        CompletableFuture<IndexStorageManager.NodeData> output = new CompletableFuture<>();

        if (node.getPointer() == null){
            indexStorageManager.writeNewNode(indexId, node.getData(), node.isRoot()).whenComplete((nodeData1, throwable) -> {
                if (throwable != null){
                    output.completeExceptionally(throwable);
                    return;
                }
                node.setPointer(nodeData1.pointer());
                output.complete(nodeData1);
            });
        } else {
            indexStorageManager.updateNode(indexId, node.getData(), node.getPointer(), node.isRoot()).whenComplete((integer, throwable) -> {
                if (throwable != null){
                    output.completeExceptionally(throwable);
                    return;
                }
                output.complete(nodeData);
            });
        }
        return output;
    }

    // Todo: apparently `indexStorageManager.readNode(indexId, pointer).get()` can return empty byte[] in case file doesnt exist,
    //       in that case fromBytes() method of the node factory throw "ArrayIndexOutOfBoundsException: Index 0 out of bounds for length 0" during construction
    public static <K extends Comparable<K>> AbstractTreeNode<K> read(IndexStorageManager indexStorageManager, int indexId, Pointer pointer, NodeFactory<K> nodeFactory) throws ExecutionException, InterruptedException, IOException {
        return nodeFactory.fromNodeData(indexStorageManager.readNode(indexId, pointer).get());
    }

    public static <K extends Comparable<K>> void update(IndexStorageManager indexStorageManager, int indexId, AbstractTreeNode<K> node) throws InterruptedException, IOException, ExecutionException {
        indexStorageManager.updateNode(indexId, node.getData(), node.getPointer(), node.isRoot()).get();
    }

    public static <E extends Comparable<E>> void remove(IndexStorageManager indexStorageManager, int indexId, AbstractTreeNode<E> node) throws ExecutionException, InterruptedException {
        remove(indexStorageManager, indexId, node.getPointer());
    }

    public static void remove(IndexStorageManager indexStorageManager, int indexId, Pointer pointer) throws ExecutionException, InterruptedException {
        indexStorageManager.removeNode(indexId, pointer).get();
    }
}
