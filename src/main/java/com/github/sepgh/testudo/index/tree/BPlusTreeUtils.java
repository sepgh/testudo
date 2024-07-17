package com.github.sepgh.testudo.index.tree;

import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.InternalTreeNode;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.IndexTreeNodeIO;
import com.github.sepgh.testudo.storage.index.session.IndexIOSession;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BPlusTreeUtils {

    public static <K extends Comparable<K>> void getPathToResponsibleNode(IndexIOSession<K> indexIOSession, List<AbstractTreeNode<K>> path, AbstractTreeNode<K> node, K identifier, int degree) throws InternalOperationException {
        if (node.getType() == AbstractTreeNode.Type.LEAF){
            path.addFirst(node);
            return;
        }

        List<InternalTreeNode.ChildPointers<K>> childPointersList = ((InternalTreeNode<K>) node).getChildPointersList(degree);
        for (int i = 0; i < childPointersList.size(); i++){
            InternalTreeNode.ChildPointers<K> childPointers = childPointersList.get(i);
            if (childPointers.getKey().compareTo(identifier) > 0 && childPointers.getLeft() != null){
                path.addFirst(node);
                getPathToResponsibleNode(
                        indexIOSession,
                        path,
                        indexIOSession.read(childPointers.getLeft()),
                        identifier,
                        degree
                );
                return;
            }
            if (i == childPointersList.size() - 1 && childPointers.getRight() != null){
                path.addFirst(node);
                getPathToResponsibleNode(
                        indexIOSession,
                        path,
                        indexIOSession.read(childPointers.getRight()),
                        identifier,
                        degree
                );
                return;
            }
        }
    }

    public static <K extends Comparable<K>, V extends Comparable<V>> AbstractLeafTreeNode<K, V> getResponsibleNode(IndexStorageManager indexStorageManager, AbstractTreeNode<K> node, K identifier, int index, int degree, NodeFactory<K> nodeFactory, ImmutableBinaryObjectWrapper<V> valueImmutableBinaryObjectWrapper) throws InternalOperationException {
        if (node.isLeaf()){
            return (AbstractLeafTreeNode<K, V>) node;
        }

        List<Pointer> childrenList = ((InternalTreeNode<K>) node).getChildrenList();
        List<K> keys = node.getKeyList(degree, valueImmutableBinaryObjectWrapper.size());
        int i;
        K keyAtIndex;
        boolean flag = false;
        for (i = 0; i < keys.size(); i++){
            keyAtIndex = keys.get(i);
            if (identifier.compareTo(keyAtIndex) < 0){
                flag = true;
                break;
            }
        }

        try {

            if (flag) {
                return getResponsibleNode(
                        indexStorageManager,
                        IndexTreeNodeIO.read(indexStorageManager, index, childrenList.get(i), nodeFactory),
                        identifier,
                        index,
                        degree,
                        nodeFactory,
                        valueImmutableBinaryObjectWrapper
                );
            } else {
                return getResponsibleNode(
                        indexStorageManager,
                        IndexTreeNodeIO.read(indexStorageManager, index, childrenList.getLast(), nodeFactory),
                        identifier,
                        index,
                        degree,
                        nodeFactory,
                        valueImmutableBinaryObjectWrapper
                );
            }
        } catch (ExecutionException | InterruptedException | IOException e) {
            throw new InternalOperationException(e);
        }

    }

}
