package com.github.sepgh.internal.index.tree;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.BaseTreeNode;
import com.github.sepgh.internal.index.tree.node.InternalTreeNode;
import com.github.sepgh.internal.index.tree.node.LeafTreeNode;
import com.github.sepgh.internal.storage.session.IndexIOSession;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class BPlusTreeIndexCreateOperation {
    private final int degree;
    private final int table;
    private final IndexIOSession indexIOSession;

    public BPlusTreeIndexCreateOperation(int degree, int table, IndexIOSession indexIOSession) {
        this.degree = degree;
        this.table = table;
        this.indexIOSession = indexIOSession;
    }

    public BaseTreeNode addIndex(BaseTreeNode root, long identifier, Pointer pointer) throws ExecutionException, InterruptedException, IOException {
        List<BaseTreeNode> path = new LinkedList<>();
        BPlusTreeUtils.getPathToResponsibleNode(indexIOSession, path, root, identifier, degree);


        /* variables to fill and use in while */
        long idForParentToStore = identifier;
        BaseTreeNode newChildForParent = null;
        BaseTreeNode answer = null;

        for (int i = 0; i < path.size(); i++){
            BaseTreeNode currentNode = path.get(i);

            if (i == 0){
                /* current node is a leaf which should handle storing the data */

                /* If current node has space, store and exit */
                if (currentNode.getKeyList(degree).size() < (degree - 1)){
                    ((LeafTreeNode) currentNode).addKeyValue(identifier, pointer, degree);
                    indexIOSession.write(currentNode);
                    indexIOSession.commit();
                    return currentNode;
                }

                /* Current node didn't have any space, so let's create a sibling and split */
                LeafTreeNode newSiblingLeafNode = new LeafTreeNode(indexIOSession.getIndexStorageManager().getEmptyNode());
                List<LeafTreeNode.KeyValue> passingKeyValues = ((LeafTreeNode) currentNode).split(identifier, pointer, degree);
                newSiblingLeafNode.setKeyValues(passingKeyValues, degree);
                indexIOSession.write(newSiblingLeafNode); // we want the node to have a pointer so that we can fix siblings
                /* Fix sibling pointers */
                fixSiblingPointers((LeafTreeNode) currentNode, newSiblingLeafNode, table);
                indexIOSession.write(newSiblingLeafNode);
                indexIOSession.write(currentNode);

                answer = currentNode.getKeyList(degree).contains(identifier) ? currentNode : newSiblingLeafNode;

                /* this leaf doesn't have a parent! create one and deal with it right here! */
                if (path.size() == 1) {
                    currentNode.unsetAsRoot();
                    InternalTreeNode newRoot = new InternalTreeNode(indexIOSession.getIndexStorageManager().getEmptyNode());
                    newRoot.setAsRoot();
                    newRoot.addChildPointers(
                            passingKeyValues.getFirst().key(),
                            currentNode.getPointer(),
                            newSiblingLeafNode.getPointer(),
                            degree,
                            false
                    );
                    indexIOSession.write(newRoot);
                    indexIOSession.write(currentNode);
                    indexIOSession.commit();
                    return answer;
                }

                newChildForParent = newSiblingLeafNode;
                idForParentToStore = passingKeyValues.getFirst().key();
            } else {

                /* current node is an internal node */

                InternalTreeNode currentInternalTreeNode = (InternalTreeNode) currentNode;
                if (currentInternalTreeNode.getKeyList(degree).size() < degree - 1) {
                    /* current internal node can store the key */
                    int indexOfAddedKey = currentInternalTreeNode.addKey(idForParentToStore, degree);
                    if (newChildForParent.getKeyList(degree).getFirst() < idForParentToStore){
                        currentInternalTreeNode.addChildAtIndex(indexOfAddedKey, newChildForParent.getPointer());
                    } else {
                        currentInternalTreeNode.addChildAtIndex(indexOfAddedKey + 1, newChildForParent.getPointer());
                    }
                    indexIOSession.write(currentInternalTreeNode);
                    indexIOSession.commit();
                    return answer;
                }


                /* current internal node cant store the key, split and ask parent */
                List<InternalTreeNode.ChildPointers> passingChildPointers = currentInternalTreeNode.addAndSplit(idForParentToStore, newChildForParent.getPointer(), degree);
                InternalTreeNode.ChildPointers firstPassingChildPointers = passingChildPointers.getFirst();
                idForParentToStore = firstPassingChildPointers.getKey();
                passingChildPointers.removeFirst();

                InternalTreeNode newInternalSibling = new InternalTreeNode(indexIOSession.getIndexStorageManager().getEmptyNode());
                newInternalSibling.setChildPointers(passingChildPointers, degree, true);
                indexIOSession.write(newInternalSibling);

                // Current node was root and needs a new parent
                if (currentInternalTreeNode.isRoot()){
                    currentInternalTreeNode.unsetAsRoot();
                    InternalTreeNode newRoot = new InternalTreeNode(indexIOSession.getIndexStorageManager().getEmptyNode());
                    newRoot.setAsRoot();

                    newRoot.addChildPointers(
                            idForParentToStore,
                            currentNode.getPointer(),
                            newInternalSibling.getPointer(),
                            degree,
                            false
                    );
                    indexIOSession.write(newRoot);
                    indexIOSession.write(currentInternalTreeNode);
                    indexIOSession.commit();
                    return answer;
                } else {
                    indexIOSession.write(currentInternalTreeNode);
                    indexIOSession.commit();
                }

            }
        }

        throw new RuntimeException("Logic error: probably failed to store index?");
    }

    private void fixSiblingPointers(LeafTreeNode currentNode, LeafTreeNode newLeafTreeNode, int table) throws ExecutionException, InterruptedException, IOException {
        Optional<Pointer> currentNodeNextSiblingPointer = currentNode.getNextSiblingPointer(degree);
        currentNode.setNextSiblingPointer(newLeafTreeNode.getPointer(), degree);
        newLeafTreeNode.setPreviousSiblingPointer(currentNode.getPointer(), degree);
        if (currentNodeNextSiblingPointer.isPresent()){
            newLeafTreeNode.setNextSiblingPointer(currentNodeNextSiblingPointer.get(), degree);

            LeafTreeNode currentNextSibling = (LeafTreeNode) indexIOSession.read(currentNodeNextSiblingPointer.get());
            currentNextSibling.setPreviousSiblingPointer(newLeafTreeNode.getPointer(), degree);
            indexIOSession.write(currentNextSibling);
        }
    }

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

}
