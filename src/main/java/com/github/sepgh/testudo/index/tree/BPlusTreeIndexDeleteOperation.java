package com.github.sepgh.testudo.index.tree;

import com.github.sepgh.testudo.ds.KeyValue;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.InternalTreeNode;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.storage.index.session.IndexIOSession;

import javax.annotation.Nullable;
import java.util.*;

public class BPlusTreeIndexDeleteOperation<K extends Comparable<K>, V> {
    private final int degree;
    private final int indexId;
    private final IndexIOSession<K> indexIOSession;
    private final int minKeys;
    private final IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory;
    private final NodeFactory<K> nodeFactory;

    public BPlusTreeIndexDeleteOperation(int degree, int indexId, IndexIOSession<K> indexIOSession, IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory, NodeFactory<K> nodeFactory) {
        this.degree = degree;
        this.indexId = indexId;
        this.indexIOSession = indexIOSession;
        this.minKeys = (degree - 1) / 2;
        this.valueIndexBinaryObjectFactory = valueIndexBinaryObjectFactory;
        this.nodeFactory = nodeFactory;
    }

    public boolean removeIndex(AbstractTreeNode<K> root, K identifier) throws InternalOperationException {
        List<AbstractTreeNode<K>> path = new LinkedList<>();
        BPlusTreeUtils.getPathToResponsibleNode(indexIOSession, path, root, identifier, degree);
        boolean result = false;

        for (int i = 0; i < path.size(); i++){
            AbstractTreeNode<K> currentNode = path.get(i);

            if (i == 0){   // Leaf
                AbstractLeafTreeNode<K, ?> leafNode = (AbstractLeafTreeNode<K, ?>) currentNode;
                result = leafNode.removeKeyValue(identifier, degree);

                indexIOSession.update(leafNode);

                if (result && !leafNode.isRoot() && leafNode.getKeyList(degree).size() < minKeys){   // Under filled
                    InternalTreeNode<K> parentNode = (InternalTreeNode<K>) path.get(i + 1);
                    this.fillNode(leafNode, parentNode, parentNode.getIndexOfChild(currentNode.getPointer()));
                }
            } else {  // internal
                this.checkInternalNode((InternalTreeNode<K>) path.get(i), path, i, identifier);
            }
        }
        indexIOSession.commit();

        return result;
    }


    private void deleteInternalNode(InternalTreeNode<K> parent, InternalTreeNode<K> node, int idx) throws InternalOperationException {
        List<Pointer> childrenList = node.getChildrenList();
        if (idx != 0){
            AbstractTreeNode<K> leftIDXChild = indexIOSession.read(childrenList.get(idx - 1));
            int keyListSize = leftIDXChild.isLeaf() ? leftIDXChild.getKeyList(degree, valueIndexBinaryObjectFactory.size()).size() :
                    ((InternalTreeNode<K>) leftIDXChild).getKeyList(degree).size();
            if (keyListSize >= minKeys){
                K pred = this.getPredecessor(node, idx);
                node.setKey(idx, pred);
                indexIOSession.update(node);
            }
        } else {
            AbstractTreeNode<K> rightIDXChild = indexIOSession.read(childrenList.get(idx + 1));
            int keyListSize = rightIDXChild.isLeaf() ? rightIDXChild.getKeyList(degree, valueIndexBinaryObjectFactory.size()).size() :
                    ((InternalTreeNode<K>) rightIDXChild).getKeyList(degree).size();
            if (keyListSize >= minKeys) {
                K succ = getSuccessor(node, idx);
                node.setKey(idx, succ);
                indexIOSession.update(node);
            } else {
                merge(parent, node, idx);
            }
        }

    }

    private K getPredecessor(InternalTreeNode<K> node, int idx) throws InternalOperationException {
        AbstractTreeNode<K> cur = indexIOSession.read(node.getChildrenList().get(idx));
        while (!cur.isLeaf()) {
            cur = indexIOSession.read(node.getChildrenList().get(cur.getKeyList(degree, valueIndexBinaryObjectFactory.size()).size()));
        }
        return cur.getKeyList(degree, valueIndexBinaryObjectFactory.size()).getLast();
    }

    private K getSuccessor(InternalTreeNode<K> node, int idx) throws InternalOperationException {
        AbstractTreeNode<K> cur = indexIOSession.read(node.getChildrenList().get(idx + 1));
        while (!cur.isLeaf()) {
            cur = indexIOSession.read(((InternalTreeNode<K>) cur).getChildrenList().getFirst());
        }
        return cur.getKeyList(degree, valueIndexBinaryObjectFactory.size()).getFirst();
    }

    private void checkInternalNode(InternalTreeNode<K> internalTreeNode, List<AbstractTreeNode<K>> path, int nodeIndex, K identifier) throws InternalOperationException {
        List<K> keyList = internalTreeNode.getKeyList(degree);

        // Checking an old root, which should already been handled, so return
        if (nodeIndex == path.size() - 1 && keyList.isEmpty())
            return;

        int indexOfKey = keyList.indexOf(identifier);

        // Check if identifier is available in the node and remove it
        if (indexOfKey != -1){
            if (internalTreeNode.isRoot()){
                this.fillRootAtIndex(internalTreeNode, indexOfKey, identifier);
            } else {
                this.deleteInternalNode((InternalTreeNode<K>) path.get(nodeIndex + 1), internalTreeNode, indexOfKey);
            }
        }


        int nodeKeySize = internalTreeNode.getKeyList(degree).size();  // shouldn't replace getList() with keyList var, since list has changed in binary format
        // Check if node is underflowed
        if (nodeKeySize < minKeys && !internalTreeNode.isRoot()){
            InternalTreeNode<K> parent = (InternalTreeNode<K>) path.get(nodeIndex + 1);
            this.fillNode(internalTreeNode, parent, parent.getIndexOfChild(internalTreeNode.getPointer()));
        }
    }

    private void fillRootAtIndex(InternalTreeNode<K> internalTreeNode, int indexOfKey, K identifier) throws InternalOperationException {
        AbstractLeafTreeNode<K, ?> leafTreeNode = BPlusTreeUtils.getResponsibleNode(
                indexIOSession.getIndexStorageManager(),
                indexIOSession.read(internalTreeNode.getChildAtIndex(indexOfKey + 1)),
                identifier,
                indexId,
                degree,
                nodeFactory
        );

        internalTreeNode.setKey(indexOfKey, leafTreeNode.getKeyList(degree).getLast());
        indexIOSession.update(internalTreeNode);
    }

    private void fillNode(AbstractTreeNode<K> currentNode, InternalTreeNode<K> parentNode, int idx) throws InternalOperationException {
        boolean borrowed;
        if (idx == 0){  // Leaf was at the beginning, check if we can borrow from right
            borrowed = tryBorrowRight(parentNode, idx, currentNode);

        } else {
            // Leaf was not at the beginning, check if we can borrow from left first
            borrowed = tryBorrowLeft(parentNode, idx, currentNode);

            if (!borrowed && idx < parentNode.getKeyList(degree).size() - 1){
                // we may still be able to borrow from right despite leaf not being at beginning, only if idx is not for last node
                borrowed = tryBorrowRight(parentNode, idx, currentNode);
            }
        }

        // We could not borrow from neither sides. Merge nodes
        if (!borrowed){
            merge(parentNode, currentNode, idx);
        }
    }

    private boolean tryBorrowRight(InternalTreeNode<K> parentNode, int idx, AbstractTreeNode<K> child) throws InternalOperationException {
        AbstractTreeNode<K> sibling = indexIOSession.read(parentNode.getChildrenList().get(idx + 1));
        int keyCount = sibling.isLeaf() ? sibling.getKeyList(degree, valueIndexBinaryObjectFactory.size()).size() : ((InternalTreeNode<K>) sibling).getKeyList(degree).size();
        if (keyCount > minKeys){
            this.borrowFromNext(parentNode, idx, child);
            return true;
        }
        return false;
    }

    private boolean tryBorrowLeft(InternalTreeNode<K> parentNode, int idx, AbstractTreeNode<K> child) throws InternalOperationException {
        AbstractTreeNode<K> sibling = indexIOSession.read(parentNode.getChildrenList().get(idx - 1));
        int keyCount = sibling.isLeaf() ? sibling.getKeyList(degree, valueIndexBinaryObjectFactory.size()).size() : ((InternalTreeNode<K>) sibling).getKeyList(degree).size();
        if (keyCount > minKeys){
            this.borrowFromPrev(parentNode, idx, child);
            return true;
        }
        return false;
    }

    /**
     * Borrow a node from sibling at left (previous)
     * We remove sibling last child pointer and pass it to child (current node)
     * If child (and sibling) is an internal node, then we be careful that the child may already "LOOK EMPTY" in terms of keys,
     *    but still include child pointers, so the position of sibling first child entrance to child's children is important
     *
     * @param parent node
     * @param idx index of child in parent
     * @param optionalChild nullable child node, if not provided it will be calculated based on idx
     */
    private void borrowFromPrev(InternalTreeNode<K> parent, int idx, @Nullable AbstractTreeNode<K> optionalChild) throws InternalOperationException {
        AbstractTreeNode<K> child = optionalChild != null ? optionalChild : indexIOSession.read(parent.getChildrenList().get(idx));
        AbstractTreeNode<K> sibling = indexIOSession.read(parent.getChildrenList().get(idx - 1));

        if (!child.isLeaf()){
            InternalTreeNode<K> siblingInternalNode = (InternalTreeNode<K>) sibling;
            List<InternalTreeNode.ChildPointers<K>> childPointersList = new ArrayList<>(siblingInternalNode.getChildPointersList(degree));
            InternalTreeNode.ChildPointers<K> siblingLastChildPointer = childPointersList.removeLast();
            siblingInternalNode.setChildPointers(childPointersList, degree, true);

            K currKey = parent.getKeyList(degree).get(idx - 1);
            parent.setKey(idx - 1, siblingLastChildPointer.getKey());

            // we put removed sibling child at left side of newly added key
            // if child looks empty it definitely still has aa child pointer despite keys being empty
            // this happens due to merge calls on lower level or removal of the key in internal node
            InternalTreeNode<K> childInternalNode = (InternalTreeNode<K>) child;
            if (!childInternalNode.getKeyList(degree).isEmpty()){
                ArrayList<InternalTreeNode.ChildPointers<K>> childPointersList2 = new ArrayList<>(childInternalNode.getChildPointersList(degree));
                siblingLastChildPointer.setRight(childPointersList2.getFirst().getLeft());
                siblingLastChildPointer.setKey(currKey);
                siblingLastChildPointer.setLeft(siblingLastChildPointer.getRight());
                childPointersList2.add(siblingLastChildPointer);
                childInternalNode.setChildPointers(childPointersList2, degree, false);
            } else {
                Pointer first = childInternalNode.getChildrenList().getFirst();
                childInternalNode.addChildPointers(currKey, siblingLastChildPointer.getRight(), first, degree, true);
            }

        } else {
            AbstractLeafTreeNode<K, V> siblingLeafNode = (AbstractLeafTreeNode<K, V>) sibling;
            AbstractLeafTreeNode<K, V> childLeafNode = (AbstractLeafTreeNode<K, V>) child;

            List<KeyValue<K, V>> keyValueList = new ArrayList<>(siblingLeafNode.getKeyValueList(degree));
            KeyValue<K, V> keyValue = keyValueList.removeLast();
            siblingLeafNode.setKeyValues(keyValueList, degree);

            parent.setKey(idx - 1, keyValue.key());

            childLeafNode.addKeyValue(keyValue, degree);
        }
        indexIOSession.update(parent);
        indexIOSession.update(child);
        indexIOSession.update(sibling);
    }

    /**
     * Borrow a key from sibling at right (next)
     * We remove sibling first child pointer and pass it to child (current node)
     * If child (and sibling) is an internal node, then we be careful that the child may already "LOOK EMPTY" in terms of keys,
     *      but still include child pointers, so the position of sibling first child entrance to child's children is important
     *
     * @param parent node
     * @param idx index of child in parent
     * @param optionalChild nullable child node, if not provided it will be calculated based on idx
     */
    private void borrowFromNext(InternalTreeNode<K> parent, int idx, @Nullable AbstractTreeNode<K> optionalChild) throws InternalOperationException {
        AbstractTreeNode<K> child = optionalChild != null ? optionalChild : indexIOSession.read(parent.getChildrenList().get(idx));
        AbstractTreeNode<K> sibling = indexIOSession.read(parent.getChildrenList().get(idx + 1));

        if (!child.isLeaf()){
            InternalTreeNode<K> siblingInternalNode = (InternalTreeNode<K>) sibling;
            List<InternalTreeNode.ChildPointers<K>> siblingPointersList = new ArrayList<>(siblingInternalNode.getChildPointersList(degree));
            InternalTreeNode.ChildPointers<K> siblingFirstChildPointer = siblingPointersList.removeFirst();
            siblingInternalNode.setChildPointers(siblingPointersList, degree, true);

            K currKey = parent.getKeyList(degree).get(idx);
            parent.setKey(idx, siblingFirstChildPointer.getKey());

            InternalTreeNode<K> childInternalNode = (InternalTreeNode<K>) child;

            // we put removed sibling child at right side of newly added key
            // if child looks empty it definitely still has a child pointer despite keys being empty
            // this happens due to merge calls on lower level or removal of the key in internal node
            if (!childInternalNode.getKeyList(degree).isEmpty()){
                ArrayList<InternalTreeNode.ChildPointers<K>> childPointersList2 = new ArrayList<>(childInternalNode.getChildPointersList(degree));
                siblingFirstChildPointer.setRight(siblingFirstChildPointer.getLeft());
                siblingFirstChildPointer.setKey(currKey);
                siblingFirstChildPointer.setLeft(childPointersList2.getLast().getRight());
                childPointersList2.add(siblingFirstChildPointer);

                childInternalNode.setChildPointers(childPointersList2, degree, false);
            } else {
                Pointer first = childInternalNode.getChildrenList().getFirst();
                childInternalNode.addChildPointers(currKey, first, siblingFirstChildPointer.getLeft(), degree, true);
            }
        } else {
            AbstractLeafTreeNode<K, V> siblingLeafNode = (AbstractLeafTreeNode<K, V>) sibling;
            AbstractLeafTreeNode<K, V> childLeafNode = (AbstractLeafTreeNode<K, V>) child;

            List<KeyValue<K, V>> keyValueList = new ArrayList<>(siblingLeafNode.getKeyValueList(degree));
            KeyValue<K, V> keyValue = keyValueList.removeFirst();
            siblingLeafNode.setKeyValues(keyValueList, degree);

            parent.setKey(idx, keyValueList.getFirst().key());
            childLeafNode.addKeyValue(keyValue, degree);
        }


        indexIOSession.update(parent);
        indexIOSession.update(child);
        indexIOSession.update(sibling);
    }

    /**
     * Merges the child node at idx with its next sibling.
     * Next sibling may be left or right depending on which one is available (right has priority)
     * If sibling has more keys than current node we merge current to sibling (not sibling to current)
     * @param parent The current node parent.
     * @param child The current node. We will merge into child (we may switch it with sibling first)
     * @param idx The index of the child parent to merge.
     */
    private void merge(InternalTreeNode<K> parent, AbstractTreeNode<K> child, int idx) throws InternalOperationException {
        int toRemoveIndex = idx + 1;
        if (idx == parent.getChildrenList().size() - 1){
            toRemoveIndex = idx - 1;
        }
        AbstractTreeNode<K> toKeep = child;
        AbstractTreeNode<K> toRemove = indexIOSession.read(parent.getChildrenList().get(toRemoveIndex));

        if (toRemove.getKeyList(degree, valueIndexBinaryObjectFactory.size()).size() > toKeep.getKeyList(degree, valueIndexBinaryObjectFactory.size()).size()){
            // Sibling (node to remove) has more keys than child (one to keep), lets merge from child to sibling and remove child

            toKeep = toRemove;
            toRemove = child;
            int tempSib = toRemoveIndex;
            toRemoveIndex = idx;
            idx = tempSib;
        }


        /*
         *   Before we execute below if statements and move children / key-value list from sibling to child
         *   we create empty list of those types and process the movement later after we dealt possibly adding parent removed key to child
         *   Reason:
         *      if we move the children first (child pointers) we would create a possibly empty byte array as key before last child
         *      the empty byte array will be detected as "0" or a valid key because TreeNodeUtils.hasKeyAtIndex() relies on checking empty byte-array to say if a value is present or not
         *      as a result, when we later add removed key from parent to the child, it will move to an invalid position
         *
         *   {VALIDATION OF CHILD MOVEMENT}
         */

        ArrayList<KeyValue<K, V>> keyValueListToMove = null;
        ArrayList<Pointer> childPointersToMove = null;

        /*
         *  For internal node we grab all sibling keys and add them to child
         *  We also add sibling 'child pointers' at the beginning or end of child's 'child pointers' depending on their position
         *  If sibling is after current child, its pointer are appended, otherwise they prepend
         */
        if (!toKeep.isLeaf()){

            InternalTreeNode<K> internalTreeNodeToKeep = (InternalTreeNode<K>) toKeep;
            List<K> childKeyList = new ArrayList<>(internalTreeNodeToKeep.getKeyList(degree));
            childKeyList.addAll(toRemove.getKeyList(degree, valueIndexBinaryObjectFactory.size()));
            childKeyList.sort(K::compareTo);
            internalTreeNodeToKeep.setKeys(childKeyList);

            childPointersToMove = new ArrayList<>(internalTreeNodeToKeep.getChildrenList());
            // Prepend or append
            if (idx > toRemoveIndex){
                childPointersToMove.addAll(0, ((InternalTreeNode<K>) toRemove).getChildrenList());
            } else {
                childPointersToMove.addAll(((InternalTreeNode<K>) toRemove).getChildrenList());
            }

        } else {
            AbstractLeafTreeNode<K, V> leafTreeNodeToKeep = (AbstractLeafTreeNode<K, V>) toKeep;
            keyValueListToMove = new ArrayList<>(leafTreeNodeToKeep.getKeyValueList(degree));
            keyValueListToMove.addAll(((AbstractLeafTreeNode<K, V>) toRemove).getKeyValueList(degree));
            Collections.sort(keyValueListToMove);
        }

        int keyToRemoveIndex = toRemoveIndex == 0 ? toRemoveIndex : toRemoveIndex - 1;
        K parentKeyAtIndex = parent.getKeyList(degree).get(keyToRemoveIndex);

        parent.removeKey(keyToRemoveIndex, degree);
        parent.removeChild(toRemoveIndex, degree);

        /*
         *   We may have removed the only key remaining in the parent
         *   In such case, we mark parent as removed and move it's key to child
         *   If it was root, child would be new root
         */
        if (parent.getKeyList(degree).isEmpty()){
            if (!toKeep.isLeaf()) {
                assert toKeep instanceof InternalTreeNode;
                ((InternalTreeNode<K>) toKeep).addKey(parentKeyAtIndex, degree);
            }
            if (parent.isRoot()){
                toKeep.setAsRoot();
                parent.unsetAsRoot();
            }
            indexIOSession.remove(parent);
        }else{
            indexIOSession.update(parent);
        }

        // {VALIDATION OF CHILD MOVEMENT}   (search for this phrase to see related section)
        if (toKeep.isLeaf()){
            ((AbstractLeafTreeNode<K, V>) toKeep).setKeyValues(keyValueListToMove, degree);
        } else {
            ((InternalTreeNode<K>) toKeep).setChildren(childPointersToMove);
        }

        indexIOSession.update(toKeep);

        if (toRemove.isLeaf()) {
            assert toRemove instanceof AbstractLeafTreeNode<K, ?>;
            this.connectSiblings((AbstractLeafTreeNode<K, V>) toRemove);
        }

        indexIOSession.remove(toRemove);
    }

    private void connectSiblings(AbstractLeafTreeNode<K, V> node) throws InternalOperationException {
        Optional<Pointer> optionalNextSiblingPointer = node.getNextSiblingPointer(degree);
        Optional<Pointer> optionalPreviousSiblingPointer = node.getPreviousSiblingPointer(degree);
        if (optionalNextSiblingPointer.isPresent()){
            AbstractLeafTreeNode<K, V> nextNode = (AbstractLeafTreeNode<K, V>) indexIOSession.read(optionalNextSiblingPointer.get());
            if (optionalPreviousSiblingPointer.isPresent()){
                nextNode.setPreviousSiblingPointer(optionalPreviousSiblingPointer.get(), degree);
            } else {
                nextNode.setPreviousSiblingPointer(Pointer.empty(), degree);
            }
            indexIOSession.update(nextNode);
        }

        if (optionalPreviousSiblingPointer.isPresent()){
            AbstractLeafTreeNode<K, ?> previousNode = (AbstractLeafTreeNode<K, ?>) indexIOSession.read(optionalPreviousSiblingPointer.get());
            if (optionalNextSiblingPointer.isPresent()){
                previousNode.setNextSiblingPointer(optionalNextSiblingPointer.get(), degree);
            } else {
                previousNode.setNextSiblingPointer(Pointer.empty(), degree);
            }
            indexIOSession.update(previousNode);
        }

    }

}
