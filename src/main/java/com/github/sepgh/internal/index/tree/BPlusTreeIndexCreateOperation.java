package com.github.sepgh.internal.index.tree;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.cluster.BaseClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.cluster.ClusterIdentifier;
import com.github.sepgh.internal.index.tree.node.cluster.InternalClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.internal.storage.session.IndexIOSession;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class BPlusTreeIndexCreateOperation<K extends Comparable<K>> {
    private final int degree;
    private final IndexIOSession<K> indexIOSession;
    private final ClusterIdentifier.Strategy<K> strategy;

    public BPlusTreeIndexCreateOperation(int degree, IndexIOSession<K> indexIOSession, ClusterIdentifier.Strategy<K> strategy) {
        this.degree = degree;
        this.indexIOSession = indexIOSession;
        this.strategy = strategy;
    }

    public BaseClusterTreeNode<K> addIndex(BaseClusterTreeNode<K> root, K identifier, Pointer pointer) throws ExecutionException, InterruptedException, IOException {
        List<BaseClusterTreeNode<K>> path = new LinkedList<>();
        BPlusTreeUtils.getPathToResponsibleNode(indexIOSession, path, root, identifier, degree);


        /* variables to fill and use in while */
        K idForParentToStore = identifier;
        BaseClusterTreeNode<K> newChildForParent = null;
        BaseClusterTreeNode<K> answer = null;

        for (int i = 0; i < path.size(); i++){
            BaseClusterTreeNode<K> currentNode = path.get(i);

            if (i == 0){
                /* current node is a leaf which should handle storing the data */

                /* If current node has space, store and exit */
                if (currentNode.getKeyList(degree).size() < (degree - 1)){
                    ((LeafClusterTreeNode<K>) currentNode).addKeyValue(identifier, pointer, degree);
                    indexIOSession.write(currentNode);
                    indexIOSession.commit();
                    return currentNode;
                }

                /* Current node didn't have any space, so let's create a sibling and split */
                LeafClusterTreeNode<K> newSiblingLeafNode = new LeafClusterTreeNode<>(indexIOSession.getIndexStorageManager().getEmptyNode(), strategy);
                List<LeafClusterTreeNode.KeyValue<K>> passingKeyValues = ((LeafClusterTreeNode<K>) currentNode).split(identifier, pointer, degree);
                newSiblingLeafNode.setKeyValues(passingKeyValues, degree);
                indexIOSession.write(newSiblingLeafNode); // we want the node to have a pointer so that we can fix siblings
                /* Fix sibling pointers */
                fixSiblingPointers((LeafClusterTreeNode<K>) currentNode, newSiblingLeafNode);
                indexIOSession.write(newSiblingLeafNode);
                indexIOSession.write(currentNode);

                answer = currentNode.getKeyList(degree).contains(identifier) ? currentNode : newSiblingLeafNode;

                /* this leaf doesn't have a parent! create one and deal with it right here! */
                if (path.size() == 1) {
                    currentNode.unsetAsRoot();
                    InternalClusterTreeNode<K> newRoot = new InternalClusterTreeNode<K>(indexIOSession.getIndexStorageManager().getEmptyNode(), strategy);
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

                InternalClusterTreeNode<K> currentInternalTreeNode = (InternalClusterTreeNode<K>) currentNode;
                if (currentInternalTreeNode.getKeyList(degree).size() < degree - 1) {
                    /* current internal node can store the key */
                    int indexOfAddedKey = currentInternalTreeNode.addKey(idForParentToStore, degree);
                    if (newChildForParent.getKeyList(degree).getFirst().compareTo(idForParentToStore) < 0){
                        currentInternalTreeNode.addChildAtIndex(indexOfAddedKey, newChildForParent.getPointer());
                    } else {
                        currentInternalTreeNode.addChildAtIndex(indexOfAddedKey + 1, newChildForParent.getPointer());
                    }
                    indexIOSession.write(currentInternalTreeNode);
                    indexIOSession.commit();
                    return answer;
                }


                /* current internal node cant store the key, split and ask parent */
                List<InternalClusterTreeNode.ChildPointers<K>> passingChildPointers = currentInternalTreeNode.addAndSplit(idForParentToStore, newChildForParent.getPointer(), degree);
                InternalClusterTreeNode.ChildPointers<K> firstPassingChildPointers = passingChildPointers.getFirst();
                idForParentToStore = firstPassingChildPointers.getKey();
                passingChildPointers.removeFirst();

                InternalClusterTreeNode<K> newInternalSibling = new InternalClusterTreeNode<K>(indexIOSession.getIndexStorageManager().getEmptyNode(), strategy);
                newInternalSibling.setChildPointers(passingChildPointers, degree, true);
                indexIOSession.write(newInternalSibling);

                // Current node was root and needs a new parent
                if (currentInternalTreeNode.isRoot()){
                    currentInternalTreeNode.unsetAsRoot();
                    InternalClusterTreeNode<K> newRoot = new InternalClusterTreeNode<K>(indexIOSession.getIndexStorageManager().getEmptyNode(), strategy);
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

    private void fixSiblingPointers(LeafClusterTreeNode<K> currentNode, LeafClusterTreeNode<K> newLeafTreeNode) throws ExecutionException, InterruptedException, IOException {
        Optional<Pointer> currentNodeNextSiblingPointer = currentNode.getNextSiblingPointer(degree);
        currentNode.setNextSiblingPointer(newLeafTreeNode.getPointer(), degree);
        newLeafTreeNode.setPreviousSiblingPointer(currentNode.getPointer(), degree);
        if (currentNodeNextSiblingPointer.isPresent()){
            newLeafTreeNode.setNextSiblingPointer(currentNodeNextSiblingPointer.get(), degree);

            LeafClusterTreeNode<K> currentNextSibling = (LeafClusterTreeNode<K>) indexIOSession.read(currentNodeNextSiblingPointer.get());
            currentNextSibling.setPreviousSiblingPointer(newLeafTreeNode.getPointer(), degree);
            indexIOSession.write(currentNextSibling);
        }
    }

}
