package com.github.sepgh.testudo.index.tree;

import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.InternalTreeNode;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.IndexTreeNodeIO;
import com.github.sepgh.testudo.storage.index.session.IndexIOSession;

import java.io.IOException;
import java.util.Collections;
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
                        indexIOSession.read(childPointersList.get(0).getLeft()),
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

}
