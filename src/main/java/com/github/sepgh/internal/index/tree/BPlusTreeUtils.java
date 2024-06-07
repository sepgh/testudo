package com.github.sepgh.internal.index.tree;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.cluster.BaseClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.cluster.InternalClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.IndexTreeNodeIO;
import com.github.sepgh.internal.storage.session.IndexIOSession;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BPlusTreeUtils {

    public static void getPathToResponsibleNode(IndexIOSession indexIOSession, List<BaseClusterTreeNode> path, BaseClusterTreeNode node, long identifier, int degree) throws ExecutionException, InterruptedException, IOException {
        if (node.getType() == BaseClusterTreeNode.Type.LEAF){
            path.addFirst(node);
            return;
        }

        List<InternalClusterTreeNode.ChildPointers> childPointersList = ((InternalClusterTreeNode) node).getChildPointersList(degree);
        for (int i = 0; i < childPointersList.size(); i++){
            InternalClusterTreeNode.ChildPointers childPointers = childPointersList.get(i);
            if (childPointers.getKey() > identifier && childPointers.getLeft() != null){
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

    public static LeafClusterTreeNode getResponsibleNode(IndexStorageManager indexStorageManager, BaseClusterTreeNode node, long identifier, int table, int degree) throws ExecutionException, InterruptedException, IOException {
        if (node.isLeaf()){
            return (LeafClusterTreeNode) node;
        }

        List<Pointer> childrenList = ((InternalClusterTreeNode) node).getChildrenList();
        List<Long> keys = node.getKeyList(degree);
        int i;
        long keyAtIndex;
        boolean flag = false;
        for (i = 0; i < keys.size(); i++){
            keyAtIndex = keys.get(i);
            if (identifier < keyAtIndex){
                flag = true;
                break;
            }
        }

        if (flag) {
            return getResponsibleNode(
                    indexStorageManager,
                    IndexTreeNodeIO.read(indexStorageManager, table, childrenList.get(i)),
                    identifier,
                    table,
                    degree
            );
        } else {
            return getResponsibleNode(
                    indexStorageManager,
                    IndexTreeNodeIO.read(indexStorageManager, table, childrenList.getLast()),
                    identifier,
                    table,
                    degree
            );
        }

    }

}
