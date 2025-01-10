package com.github.sepgh.testudo.utils;

import com.github.sepgh.testudo.ds.KVSize;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.InternalTreeNode;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.google.common.hash.HashCode;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

public class TreeExplainer<K extends Comparable<K>, V extends Comparable<V>> {

    private final int indexId;
    private final IndexStorageManager indexStorageManager;
    private final int degree;
    private final IndexBinaryObjectFactory<K> keyIndexBinaryObjectFactory;
    private final IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory;
    private final KVSize kvSize;
    private final NodeFactory<K> nodeFactory;

    public TreeExplainer(int indexId, IndexStorageManager indexStorageManager, int degree, IndexBinaryObjectFactory<K> keyIndexBinaryObjectFactory, IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory) {
        this.indexId = indexId;
        this.indexStorageManager = indexStorageManager;
        this.degree = degree;
        this.keyIndexBinaryObjectFactory = keyIndexBinaryObjectFactory;
        this.valueIndexBinaryObjectFactory = valueIndexBinaryObjectFactory;
        this.kvSize = new KVSize(keyIndexBinaryObjectFactory.size(), valueIndexBinaryObjectFactory.size());
        this.nodeFactory = new NodeFactory.DefaultNodeFactory<>(keyIndexBinaryObjectFactory, valueIndexBinaryObjectFactory);
    }

    public void explain() throws InternalOperationException, ExecutionException, InterruptedException {
        Queue<AbstractTreeNode<K>> queue = new LinkedList<>();

        queue.add(getRoot());

        while (!queue.isEmpty()) {
            AbstractTreeNode<K> node = queue.remove();

            explain(node, degree);

            if (node instanceof InternalTreeNode) {
                List<Pointer> childrenList = ((InternalTreeNode<K>) node).getChildrenList();
                for (Pointer pointer : childrenList) {
                    queue.add(
                            nodeFactory.fromNodeData(
                                    indexStorageManager.readNode(
                                            indexId,
                                            pointer,
                                            kvSize
                                    ).get()
                            )
                    );
                }
            }
        }

    }

    public static <K extends Comparable<K>, V extends Comparable<V>> void explain(AbstractTreeNode<K> node, int degree) {
        System.out.println();

        System.out.println("EXPLAINING Pointer: " + node.getPointer());
        System.out.println("    Hash: " + HashCode.fromBytes(node.toBytes()));
        System.out.println("    Root: " + node.isRoot());
        System.out.println("    Type: " + node.getType());

        if (node.getType().equals(AbstractTreeNode.Type.INTERNAL)){
            System.out.println("    Keys: " + ((InternalTreeNode<K>) node).getKeyList(degree));
            System.out.println("    Children: " + ((InternalTreeNode<K>) node).getChildrenList());
        } else {
            System.out.println("    Key Values: " + ((AbstractLeafTreeNode<K, V>) node).getKeyValueList(degree));
            System.out.println("    Previous: " + ((AbstractLeafTreeNode<K, V>) node).getPreviousSiblingPointer(degree));
            System.out.println("    Next: " + ((AbstractLeafTreeNode<K, V>) node).getNextSiblingPointer(degree));
        }

        System.out.println();
    }

    private AbstractTreeNode<K> getRoot() throws InternalOperationException, ExecutionException, InterruptedException {
        IndexStorageManager.NodeData nodeDataRoot = indexStorageManager.getRoot(1, kvSize).get().get();
        return nodeFactory.fromNodeData(nodeDataRoot);
    }

}
