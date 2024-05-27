package com.github.sepgh.internal.index.tree;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.BaseTreeNode;
import com.github.sepgh.internal.index.tree.node.InternalTreeNode;
import com.github.sepgh.internal.index.tree.node.LeafTreeNode;
import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.IndexTreeNodeIO;
import com.github.sepgh.internal.storage.session.IndexIOSession;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class BPlusTreeUtils {

    public static void getPathToResponsibleNode(IndexIOSession indexIOSession, List<BaseTreeNode> path, BaseTreeNode node, long identifier, int degree) throws ExecutionException, InterruptedException {
        if (node.getType() == BaseTreeNode.Type.LEAF){
            path.addFirst(node);
            return;
        }

        List<InternalTreeNode.ChildPointers> childPointersList = ((InternalTreeNode) node).getChildPointersList(degree);
        for (int i = 0; i < childPointersList.size(); i++){
            InternalTreeNode.ChildPointers childPointers = childPointersList.get(i);
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

    public static LeafTreeNode getResponsibleNode(IndexStorageManager indexStorageManager, BaseTreeNode node, long identifier, int table, int degree) throws ExecutionException, InterruptedException {
        if (node.isLeaf()){
            return (LeafTreeNode) node;
        }

        List<Pointer> childrenList = ((InternalTreeNode) node).getChildrenList();
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
