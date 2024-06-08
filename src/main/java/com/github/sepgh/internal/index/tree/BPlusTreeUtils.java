package com.github.sepgh.internal.index.tree;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.cluster.BaseClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.cluster.ClusterIdentifier;
import com.github.sepgh.internal.index.tree.node.cluster.InternalClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.IndexTreeNodeIO;
import com.github.sepgh.internal.storage.session.IndexIOSession;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BPlusTreeUtils {

    public static <K extends Comparable<K>> void getPathToResponsibleNode(IndexIOSession<K> indexIOSession, List<BaseClusterTreeNode<K>> path, BaseClusterTreeNode<K> node, K identifier, int degree) throws ExecutionException, InterruptedException, IOException {
        if (node.getType() == BaseClusterTreeNode.Type.LEAF){
            path.addFirst(node);
            return;
        }

        List<InternalClusterTreeNode.ChildPointers<K>> childPointersList = ((InternalClusterTreeNode<K>) node).getChildPointersList(degree);
        for (int i = 0; i < childPointersList.size(); i++){
            InternalClusterTreeNode.ChildPointers<K> childPointers = childPointersList.get(i);
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

    public static <K extends Comparable<K>> LeafClusterTreeNode<K> getResponsibleNode(IndexStorageManager indexStorageManager, BaseClusterTreeNode<K> node, K identifier, int table, int degree, ClusterIdentifier.Strategy<K> strategy) throws ExecutionException, InterruptedException, IOException {
        if (node.isLeaf()){
            return (LeafClusterTreeNode<K>) node;
        }

        List<Pointer> childrenList = ((InternalClusterTreeNode<K>) node).getChildrenList();
        List<K> keys = node.getKeyList(degree);
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

        if (flag) {
            return getResponsibleNode(
                    indexStorageManager,
                    IndexTreeNodeIO.read(indexStorageManager, table, childrenList.get(i), strategy),
                    identifier,
                    table,
                    degree,
                    strategy
            );
        } else {
            return getResponsibleNode(
                    indexStorageManager,
                    IndexTreeNodeIO.read(indexStorageManager, table, childrenList.getLast(), strategy),
                    identifier,
                    table,
                    degree,
                    strategy
            );
        }

    }

}
