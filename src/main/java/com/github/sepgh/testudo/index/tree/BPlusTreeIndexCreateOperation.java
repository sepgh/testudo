package com.github.sepgh.testudo.index.tree;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.KeyValue;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.data.IndexBinaryObject;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.InternalTreeNode;
import com.github.sepgh.testudo.storage.index.session.IndexIOSession;
import com.github.sepgh.testudo.utils.KVSize;
import com.google.common.hash.HashCode;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class BPlusTreeIndexCreateOperation<K extends Comparable<K>, V> {
    private final int degree;
    private final IndexIOSession<K> indexIOSession;
    private final IndexBinaryObjectFactory<K> kIndexBinaryObjectFactory;
    private final IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory;
    private final KVSize kvSize;

    public BPlusTreeIndexCreateOperation(int degree, IndexIOSession<K> indexIOSession, IndexBinaryObjectFactory<K> kIndexBinaryObjectFactory, IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory, KVSize kvSize) {
        this.degree = degree;
        this.indexIOSession = indexIOSession;
        this.kIndexBinaryObjectFactory = kIndexBinaryObjectFactory;
        this.valueIndexBinaryObjectFactory = valueIndexBinaryObjectFactory;
        this.kvSize = kvSize;
    }

    public AbstractTreeNode<K> addIndex(AbstractTreeNode<K> root, K identifier, V value) throws InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject, IndexExistsException {
        List<AbstractTreeNode<K>> path = new LinkedList<>();
        BPlusTreeUtils.getPathToResponsibleNode(indexIOSession, path, root, identifier, degree);


        /* variables to fill and use in while */
        K idForParentToStore = identifier;
        AbstractTreeNode<K> newChildForParent = null;
        AbstractTreeNode<K> answer = null;

        for (int i = 0; i < path.size(); i++){
            AbstractTreeNode<K> currentNode = path.get(i);

            if (i == 0){
                /* current node is a leaf which should handle storing the data */

                List<K> currentNodeKeyList = currentNode.getKeyList(degree, valueIndexBinaryObjectFactory.size());

                if (currentNodeKeyList.contains(identifier))
                    throw new IndexExistsException();

                /* If current node has space, store and exit */
                if (currentNodeKeyList.size() < (degree - 1)){
                    ((AbstractLeafTreeNode<K, V>) currentNode).addKeyValue(identifier, value, degree);
                    indexIOSession.write(currentNode);
                    indexIOSession.commit();
                    return currentNode;
                }

                /* Current node didn't have any space, so let's create a sibling and split */
                AbstractLeafTreeNode<K, V> newSiblingLeafNode = new AbstractLeafTreeNode<>(indexIOSession.getIndexStorageManager().getEmptyNode(this.kvSize), kIndexBinaryObjectFactory, valueIndexBinaryObjectFactory);
                List<KeyValue<K, V>> passingKeyValues = ((AbstractLeafTreeNode<K, V>) currentNode).addAndSplit(identifier, value, degree);
                newSiblingLeafNode.setKeyValues(passingKeyValues, degree);
                indexIOSession.write(newSiblingLeafNode); // we want the node to have a value so that we can fix siblings
                /* Fix sibling pointers */
                fixSiblingPointers((AbstractLeafTreeNode<K, V>) currentNode, newSiblingLeafNode);
                indexIOSession.write(newSiblingLeafNode);
                indexIOSession.write(currentNode);

                answer = currentNodeKeyList.contains(identifier) ? currentNode : newSiblingLeafNode;

                /* this leaf doesn't have a parent! create one and deal with it right here! */
                if (path.size() == 1) {
                    currentNode.unsetAsRoot();
                    InternalTreeNode<K> newRoot = new InternalTreeNode<>(indexIOSession.getIndexStorageManager().getEmptyNode(this.kvSize), kIndexBinaryObjectFactory);
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

                InternalTreeNode<K> currentInternalTreeNode = (InternalTreeNode<K>) currentNode;
                if (currentInternalTreeNode.getKeyList(degree).size() < degree - 1) {
                    /* current internal node can store the key */
                    int indexOfAddedKey = currentInternalTreeNode.addKey(idForParentToStore, degree);
                    if (newChildForParent.getKeyList(degree, valueIndexBinaryObjectFactory.size()).getFirst().compareTo(idForParentToStore) < 0){
                        currentInternalTreeNode.addChildAtIndex(indexOfAddedKey, newChildForParent.getPointer());
                    } else {
                        currentInternalTreeNode.addChildAtIndex(indexOfAddedKey + 1, newChildForParent.getPointer());
                    }
                    indexIOSession.write(currentInternalTreeNode);
                    indexIOSession.commit();
                    return answer;
                }


                /* current internal node cant store the key, split and ask parent */
                List<InternalTreeNode.ChildPointers<K>> passingChildPointers = currentInternalTreeNode.addAndSplit(idForParentToStore, newChildForParent.getPointer(), degree);
                InternalTreeNode.ChildPointers<K> firstPassingChildPointers = passingChildPointers.getFirst();
                idForParentToStore = firstPassingChildPointers.getKey();
                passingChildPointers.removeFirst();

                InternalTreeNode<K> newInternalSibling = new InternalTreeNode<K>(indexIOSession.getIndexStorageManager().getEmptyNode(this.kvSize), kIndexBinaryObjectFactory);
                newInternalSibling.setChildPointers(passingChildPointers, degree, true);
                indexIOSession.write(newInternalSibling);

                // Current node was root and needs a new parent
                if (currentInternalTreeNode.isRoot()){
                    currentInternalTreeNode.unsetAsRoot();
                    InternalTreeNode<K> newRoot = new InternalTreeNode<K>(indexIOSession.getIndexStorageManager().getEmptyNode(this.kvSize), kIndexBinaryObjectFactory);
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
                }

            }
        }

        throw new RuntimeException("Logic error: probably failed to store index?");
    }

    private void fixSiblingPointers(AbstractLeafTreeNode<K, V> currentNode, AbstractLeafTreeNode<K, V> newLeafTreeNode) throws InternalOperationException {
        Optional<Pointer> currentNodeNextSiblingPointer = currentNode.getNextSiblingPointer(degree);
        currentNode.setNextSiblingPointer(newLeafTreeNode.getPointer(), degree);
        newLeafTreeNode.setPreviousSiblingPointer(currentNode.getPointer(), degree);
        if (currentNodeNextSiblingPointer.isPresent()){
            newLeafTreeNode.setNextSiblingPointer(currentNodeNextSiblingPointer.get(), degree);

            AbstractLeafTreeNode<K, V> currentNextSibling = (AbstractLeafTreeNode<K, V>) indexIOSession.read(currentNodeNextSiblingPointer.get());
            currentNextSibling.setPreviousSiblingPointer(newLeafTreeNode.getPointer(), degree);
            indexIOSession.write(currentNextSibling);
        }
    }

}
