package com.github.sepgh.internal.index.tree;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.cluster.BaseClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.cluster.InternalClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.cluster.LeafClusterTreeNode;
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

    public BaseClusterTreeNode addIndex(BaseClusterTreeNode root, long identifier, Pointer pointer) throws ExecutionException, InterruptedException, IOException {
        List<BaseClusterTreeNode> path = new LinkedList<>();
        BPlusTreeUtils.getPathToResponsibleNode(indexIOSession, path, root, identifier, degree);


        /* variables to fill and use in while */
        long idForParentToStore = identifier;
        BaseClusterTreeNode newChildForParent = null;
        BaseClusterTreeNode answer = null;

        for (int i = 0; i < path.size(); i++){
            BaseClusterTreeNode currentNode = path.get(i);

            if (i == 0){
                /* current node is a leaf which should handle storing the data */

                /* If current node has space, store and exit */
                if (currentNode.getKeyList(degree).size() < (degree - 1)){
                    ((LeafClusterTreeNode) currentNode).addKeyValue(identifier, pointer, degree);
                    indexIOSession.write(currentNode);
                    indexIOSession.commit();
                    return currentNode;
                }

                /* Current node didn't have any space, so let's create a sibling and split */
                LeafClusterTreeNode newSiblingLeafNode = new LeafClusterTreeNode(indexIOSession.getIndexStorageManager().getEmptyNode());
                List<LeafClusterTreeNode.KeyValue> passingKeyValues = ((LeafClusterTreeNode) currentNode).split(identifier, pointer, degree);
                newSiblingLeafNode.setKeyValues(passingKeyValues, degree);
                indexIOSession.write(newSiblingLeafNode); // we want the node to have a pointer so that we can fix siblings
                /* Fix sibling pointers */
                fixSiblingPointers((LeafClusterTreeNode) currentNode, newSiblingLeafNode, table);
                indexIOSession.write(newSiblingLeafNode);
                indexIOSession.write(currentNode);

                answer = currentNode.getKeyList(degree).contains(identifier) ? currentNode : newSiblingLeafNode;

                /* this leaf doesn't have a parent! create one and deal with it right here! */
                if (path.size() == 1) {
                    currentNode.unsetAsRoot();
                    InternalClusterTreeNode newRoot = new InternalClusterTreeNode(indexIOSession.getIndexStorageManager().getEmptyNode());
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

                InternalClusterTreeNode currentInternalTreeNode = (InternalClusterTreeNode) currentNode;
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
                List<InternalClusterTreeNode.ChildPointers> passingChildPointers = currentInternalTreeNode.addAndSplit(idForParentToStore, newChildForParent.getPointer(), degree);
                InternalClusterTreeNode.ChildPointers firstPassingChildPointers = passingChildPointers.getFirst();
                idForParentToStore = firstPassingChildPointers.getKey();
                passingChildPointers.removeFirst();

                InternalClusterTreeNode newInternalSibling = new InternalClusterTreeNode(indexIOSession.getIndexStorageManager().getEmptyNode());
                newInternalSibling.setChildPointers(passingChildPointers, degree, true);
                indexIOSession.write(newInternalSibling);

                // Current node was root and needs a new parent
                if (currentInternalTreeNode.isRoot()){
                    currentInternalTreeNode.unsetAsRoot();
                    InternalClusterTreeNode newRoot = new InternalClusterTreeNode(indexIOSession.getIndexStorageManager().getEmptyNode());
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

    private void fixSiblingPointers(LeafClusterTreeNode currentNode, LeafClusterTreeNode newLeafTreeNode, int table) throws ExecutionException, InterruptedException, IOException {
        Optional<Pointer> currentNodeNextSiblingPointer = currentNode.getNextSiblingPointer(degree);
        currentNode.setNextSiblingPointer(newLeafTreeNode.getPointer(), degree);
        newLeafTreeNode.setPreviousSiblingPointer(currentNode.getPointer(), degree);
        if (currentNodeNextSiblingPointer.isPresent()){
            newLeafTreeNode.setNextSiblingPointer(currentNodeNextSiblingPointer.get(), degree);

            LeafClusterTreeNode currentNextSibling = (LeafClusterTreeNode) indexIOSession.read(currentNodeNextSiblingPointer.get());
            currentNextSibling.setPreviousSiblingPointer(newLeafTreeNode.getPointer(), degree);
            indexIOSession.write(currentNextSibling);
        }
    }

}
