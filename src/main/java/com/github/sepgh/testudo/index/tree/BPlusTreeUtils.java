package com.github.sepgh.testudo.index.tree;

import com.github.sepgh.testudo.ds.KeyValue;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.InternalTreeNode;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.IndexTreeNodeIO;
import com.github.sepgh.testudo.storage.index.session.IndexIOSession;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BPlusTreeUtils {

    public static <K extends Comparable<K>> void getPathToResponsibleNode(IndexIOSession<K> indexIOSession, List<AbstractTreeNode<K>> path, AbstractTreeNode<K> node, K identifier, int degree) throws InternalOperationException {
        path.addFirst(node);

        if (node.getType() == AbstractTreeNode.Type.LEAF){
            return;
        }

        List<InternalTreeNode.ChildPointers<K>> childPointersList = ((InternalTreeNode<K>) node).getChildPointersList(degree);
        int i = Collections.binarySearch(childPointersList, InternalTreeNode.ChildPointers.getChildPointerOfKey(identifier));

        if (i >= 0) {
            getPathToResponsibleNode(
                    indexIOSession,
                    path,
                    indexIOSession.read(childPointersList.get(i).getRight()),
                    identifier,
                    degree
            );
        } else {
            i = -(i + 1);
            if (i == 0) {
                getPathToResponsibleNode(
                        indexIOSession,
                        path,
                        indexIOSession.read(childPointersList.getFirst().getLeft()),
                        identifier,
                        degree
                );
            } else {
                getPathToResponsibleNode(
                        indexIOSession,
                        path,
                        indexIOSession.read(childPointersList.get(i - 1).getRight()),
                        identifier,
                        degree
                );
            }

        }

    }

    public static <K extends Comparable<K>, V> AbstractLeafTreeNode<K, V> getResponsibleNode(IndexStorageManager indexStorageManager, AbstractTreeNode<K> node, K identifier, int index, int degree, NodeFactory<K> nodeFactory) throws InternalOperationException {
        if (node.isLeaf()){
            return (AbstractLeafTreeNode<K, V>) node;
        }


        List<InternalTreeNode.ChildPointers<K>> childPointersList = ((InternalTreeNode<K>) node).getChildPointersList(degree);
        int i = Collections.binarySearch(childPointersList, InternalTreeNode.ChildPointers.getChildPointerOfKey(identifier));

        try {

            if (i >= 0) {
                return getResponsibleNode(
                        indexStorageManager,
                        IndexTreeNodeIO.read(indexStorageManager, index, childPointersList.get(i).getRight(), nodeFactory, node.getKVSize()),
                        identifier,
                        index,
                        degree,
                        nodeFactory
                );
            } else {
                i = -(i + 1);
                if (i == 0) {
                    return getResponsibleNode(
                            indexStorageManager,
                            IndexTreeNodeIO.read(indexStorageManager, index, childPointersList.getFirst().getLeft(), nodeFactory, node.getKVSize()),
                            identifier,
                            index,
                            degree,
                            nodeFactory
                    );
                } else {
                    return getResponsibleNode(
                            indexStorageManager,
                            IndexTreeNodeIO.read(indexStorageManager, index, childPointersList.get(i - 1).getRight(), nodeFactory, node.getKVSize()),
                            identifier,
                            index,
                            degree,
                            nodeFactory
                    );
                }
            }
        }  catch (ExecutionException | InterruptedException | IOException e) {
            throw new InternalOperationException(e);
        }

    }

    public static <K extends Comparable<K>, V> AbstractLeafTreeNode<K, V> getFarLeftLeaf(IndexIOSession<K> indexIOSession, AbstractTreeNode<K> root) throws InternalOperationException {

        if (root.isLeaf())
            return (AbstractLeafTreeNode<K, V>) root;

        AbstractTreeNode<K> farLeftChild = root;

        while (!farLeftChild.isLeaf()){
            farLeftChild = indexIOSession.read(((InternalTreeNode<K>) farLeftChild).getChildAtIndex(0));
        }

        return (AbstractLeafTreeNode<K, V>) farLeftChild;
    }

    public static <K extends Comparable<K>, V> AbstractLeafTreeNode<K, V> getFarRightLeaf(IndexIOSession<K> indexIOSession, AbstractTreeNode<K> root) throws InternalOperationException {
        if (root.isLeaf())
            return (AbstractLeafTreeNode<K, V>) root;

        AbstractTreeNode<K> farRightChild = root;

        while (!farRightChild.isLeaf()){
            farRightChild = indexIOSession.read(((InternalTreeNode<K>) farRightChild).getChildrenList().getLast());
        }

        return (AbstractLeafTreeNode<K, V>) farRightChild;
    }

    public static <K extends Comparable<K>, V> Iterator<KeyValue<K, V>> getAscendingIterator(IndexIOSession<K> indexIOSession, AbstractTreeNode<K> root, int degree) throws InternalOperationException {
        return new Iterator<>() {

            private int keyIndex = 0;
            AbstractLeafTreeNode<K, V> currentLeaf = getFarLeftLeaf(indexIOSession, root);

            @Override
            public boolean hasNext() {
                int size = currentLeaf.getKeyList(degree).size();
                if (keyIndex == size)
                    return currentLeaf.getNextSiblingPointer(degree).isPresent();
                return true;
            }

            @SneakyThrows
            @Override
            public KeyValue<K, V> next() {
                List<KeyValue<K, V>> keyValueList = currentLeaf.getKeyValueList(degree);

                if (keyIndex == keyValueList.size()){
                    currentLeaf = (AbstractLeafTreeNode<K, V>) indexIOSession.read(currentLeaf.getNextSiblingPointer(degree).get());
                    keyIndex = 0;
                    keyValueList = currentLeaf.getKeyValueList(degree);
                }

                KeyValue<K, V> output = keyValueList.get(keyIndex);
                keyIndex += 1;
                return output;
            }
        };
    }

    public static <K extends Comparable<K>, V> Iterator<KeyValue<K, V>> getDescendingIterator(IndexIOSession<K> indexIOSession, AbstractTreeNode<K> root, int degree) throws InternalOperationException {
        return new Iterator<KeyValue<K, V>>() {

            private AbstractLeafTreeNode<K, V> currentLeaf = getFarRightLeaf(indexIOSession, root);
            private int keyIndex = currentLeaf.getKeyList(degree).size() - 1;

            @Override
            public boolean hasNext() {
                if (keyIndex == -1) {
                    return currentLeaf.getPreviousSiblingPointer(degree).isPresent();
                }
                return keyIndex >= 0;
            }

            @SneakyThrows
            @Override
            public KeyValue<K, V> next() {
                List<KeyValue<K, V>> keyValueList = currentLeaf.getKeyValueList(degree);

                if (keyIndex == -1){
                    currentLeaf = (AbstractLeafTreeNode<K, V>) indexIOSession.read(currentLeaf.getPreviousSiblingPointer(degree).get());
                    keyIndex = currentLeaf.getKeyList(degree).size() - 1;
                    keyValueList = currentLeaf.getKeyValueList(degree);
                }

                KeyValue<K, V> output = keyValueList.get(keyIndex);
                keyIndex -= 1;
                return output;
            }
        };
    }


}
