package com.github.sepgh.internal.index.tree;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.cluster.BaseClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.cluster.ClusterIdentifier;
import com.github.sepgh.internal.index.tree.node.cluster.InternalClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.internal.storage.session.IndexIOSession;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class BPlusTreeIndexDeleteOperation<K extends Comparable<K>> {
    private final int degree;
    private final int table;
    private final IndexIOSession<K> indexIOSession;
    private final int minKeys;
    private final ClusterIdentifier.Strategy<K> strategy;

    public BPlusTreeIndexDeleteOperation(int degree, int table, IndexIOSession<K> indexIOSession, ClusterIdentifier.Strategy<K> strategy) {
        this.degree = degree;
        this.table = table;
        this.indexIOSession = indexIOSession;
        this.strategy = strategy;
        this.minKeys = (degree - 1) / 2;
    }

    public boolean removeIndex(BaseClusterTreeNode<K> root, K identifier) throws ExecutionException, InterruptedException, IOException {

        List<BaseClusterTreeNode<K>> path = new LinkedList<>();
        BPlusTreeUtils.getPathToResponsibleNode(indexIOSession, path, root, identifier, degree);
        boolean result = false;

        for (int i = 0; i < path.size(); i++){
            BaseClusterTreeNode<K> currentNode = path.get(i);

            if (i == 0){   // Leaf
                LeafClusterTreeNode<K> leafNode = (LeafClusterTreeNode<K>) currentNode;
                result = leafNode.removeKeyValue(identifier, degree);
                indexIOSession.update(leafNode);

                if (result && !leafNode.isRoot() && leafNode.getKeyList(degree).size() < minKeys){   // Under filled
                    InternalClusterTreeNode<K> parentNode = (InternalClusterTreeNode<K>) path.get(i + 1);
                    this.fillNode(leafNode, parentNode, parentNode.getIndexOfChild(currentNode.getPointer()));
                }
            } else {  // internal
                this.checkInternalNode((InternalClusterTreeNode<K>) path.get(i), path, i, identifier);
            }
        }
        indexIOSession.commit();

        return result;
    }


    private void deleteInternalNode(InternalClusterTreeNode<K> parent, InternalClusterTreeNode<K> node, int idx) throws ExecutionException, InterruptedException, IOException {
        List<Pointer> childrenList = node.getChildrenList();
        if (idx != 0){
            BaseClusterTreeNode<K> leftIDXChild = indexIOSession.read(childrenList.get(idx - 1));
            if (leftIDXChild.getKeyList(degree).size() >= minKeys){
                K pred = this.getPredecessor(node, idx);
                node.setKey(idx, pred);
                indexIOSession.update(node);
            }
        } else {
            BaseClusterTreeNode<K> rightIDXChild = indexIOSession.read(childrenList.get(idx + 1));
            if (rightIDXChild.getKeyList(degree).size() >= minKeys) {
                K succ = getSuccessor(node, idx);
                node.setKey(idx, succ);
                indexIOSession.update(node);
            } else {
                merge(parent, node, idx);
            }
        }

    }

    private K getPredecessor(InternalClusterTreeNode<K> node, int idx) throws ExecutionException, InterruptedException, IOException {
        BaseClusterTreeNode<K> cur = indexIOSession.read(node.getChildrenList().get(idx));
        while (!cur.isLeaf()) {
            cur = indexIOSession.read(node.getChildrenList().get(cur.getKeyList(degree).size()));
        }
        return cur.getKeyList(degree).getLast();
    }

    private K getSuccessor(InternalClusterTreeNode<K> node, int idx) throws ExecutionException, InterruptedException, IOException {
        BaseClusterTreeNode<K> cur = indexIOSession.read(node.getChildrenList().get(idx + 1));
        while (!cur.isLeaf()) {
            cur = indexIOSession.read(((InternalClusterTreeNode<K>) cur).getChildrenList().getFirst());
        }
        return cur.getKeyList(degree).getFirst();
    }

    private void checkInternalNode(InternalClusterTreeNode<K> internalTreeNode, List<BaseClusterTreeNode<K>> path, int nodeIndex, K identifier) throws ExecutionException, InterruptedException, IOException {
        List<K> keyList = internalTreeNode.getKeyList(degree);
        if (nodeIndex == path.size() - 1 && keyList.isEmpty())
            return;
        int indexOfKey = keyList.indexOf(identifier);
        if (indexOfKey != -1){
            if (internalTreeNode.isRoot()){
                this.fillRootAtIndex(internalTreeNode, indexOfKey, identifier);
            } else {
                this.deleteInternalNode((InternalClusterTreeNode<K>) path.get(nodeIndex + 1), internalTreeNode, indexOfKey);
            }
        }

        int nodeKeySize = internalTreeNode.getKeyList(degree).size();
        if (nodeKeySize < minKeys && !internalTreeNode.isRoot()){
            InternalClusterTreeNode<K> parent = (InternalClusterTreeNode<K>) path.get(nodeIndex + 1);
            this.fillNode(internalTreeNode, parent, parent.getIndexOfChild(internalTreeNode.getPointer()));
        }
    }

    private void fillRootAtIndex(InternalClusterTreeNode<K> internalTreeNode, int indexOfKey, K identifier) throws ExecutionException, InterruptedException, IOException {
        LeafClusterTreeNode<K> leafTreeNode = BPlusTreeUtils.getResponsibleNode(
                indexIOSession.getIndexStorageManager(),
                indexIOSession.read(internalTreeNode.getChildAtIndex(indexOfKey + 1)),
                identifier,
                table,
                degree,
                strategy
        );

        internalTreeNode.setKey(indexOfKey, leafTreeNode.getKeyList(degree).getLast());
        indexIOSession.update(internalTreeNode);
    }

    private void fillNode(BaseClusterTreeNode<K> currentNode, InternalClusterTreeNode<K> parentNode, int idx) throws IOException, ExecutionException, InterruptedException {
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

    private boolean tryBorrowRight(InternalClusterTreeNode<K> parentNode, int idx, BaseClusterTreeNode<K> child) throws ExecutionException, InterruptedException, IOException {
        BaseClusterTreeNode<K> sibling = indexIOSession.read(parentNode.getChildrenList().get(idx + 1));
        if (sibling.getKeyList(degree).size() > minKeys){
            this.borrowFromNext(parentNode, idx, child);
            return true;
        }
        return false;
    }

    private boolean tryBorrowLeft(InternalClusterTreeNode<K> parentNode, int idx, BaseClusterTreeNode<K> child) throws ExecutionException, InterruptedException, IOException {
        BaseClusterTreeNode<K> sibling = indexIOSession.read(parentNode.getChildrenList().get(idx - 1));
        if (sibling.getKeyList(degree).size() > minKeys){
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
    private void borrowFromPrev(InternalClusterTreeNode<K> parent, int idx, @Nullable BaseClusterTreeNode<K> optionalChild) throws ExecutionException, InterruptedException, IOException {
        BaseClusterTreeNode<K> child = optionalChild != null ? optionalChild : indexIOSession.read(parent.getChildrenList().get(idx));
        BaseClusterTreeNode<K> sibling = indexIOSession.read(parent.getChildrenList().get(idx - 1));

        if (!child.isLeaf()){
            InternalClusterTreeNode<K> siblingInternalNode = (InternalClusterTreeNode<K>) sibling;
            List<InternalClusterTreeNode.ChildPointers<K>> childPointersList = new ArrayList<>(siblingInternalNode.getChildPointersList(degree));
            InternalClusterTreeNode.ChildPointers<K> siblingLastChildPointer = childPointersList.removeLast();
            siblingInternalNode.setChildPointers(childPointersList, degree, true);

            K currKey = parent.getKeyList(degree).get(idx - 1);
            parent.setKey(idx - 1, siblingLastChildPointer.getKey());

            // we put removed sibling child at left side of newly added key
            // if child looks empty it definitely still has aa child pointer despite keys being empty
            // this happens due to merge calls on lower level or removal of the key in internal node
            InternalClusterTreeNode<K> childInternalNode = (InternalClusterTreeNode<K>) child;
            if (!childInternalNode.getKeyList(degree).isEmpty()){
                ArrayList<InternalClusterTreeNode.ChildPointers<K>> childPointersList2 = new ArrayList<>(childInternalNode.getChildPointersList(degree));
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
            LeafClusterTreeNode<K> siblingLeafNode = (LeafClusterTreeNode<K>) sibling;
            LeafClusterTreeNode<K> childLeafNode = (LeafClusterTreeNode<K>) child;

            List<LeafClusterTreeNode.KeyValue<K>> keyValueList = new ArrayList<>(siblingLeafNode.getKeyValueList(degree));
            LeafClusterTreeNode.KeyValue<K> keyValue = keyValueList.removeLast();
            siblingLeafNode.setKeyValues(keyValueList, degree);


            parent.setKey(idx - 1, keyValue.key());

            childLeafNode.addKeyValue(keyValue, degree);
        }
        indexIOSession.update(parent, child, sibling);
    }

    /**
     * Borrow a node from sibling at right (next)
     * We remove sibling first child pointer and pass it to child (current node)
     * If child (and sibling) is an internal node, then we be careful that the child may already "LOOK EMPTY" in terms of keys,
     *      but still include child pointers, so the position of sibling first child entrance to child's children is important
     *
     * @param parent node
     * @param idx index of child in parent
     * @param optionalChild nullable child node, if not provided it will be calculated based on idx
     */
    private void borrowFromNext(InternalClusterTreeNode<K> parent, int idx, @Nullable BaseClusterTreeNode<K> optionalChild) throws ExecutionException, InterruptedException, IOException {
        BaseClusterTreeNode<K> child = optionalChild != null ? optionalChild : indexIOSession.read(parent.getChildrenList().get(idx));
        BaseClusterTreeNode<K> sibling = indexIOSession.read(parent.getChildrenList().get(idx + 1));

        if (!child.isLeaf()){
            InternalClusterTreeNode<K> siblingInternalNode = (InternalClusterTreeNode<K>) sibling;
            List<InternalClusterTreeNode.ChildPointers<K>> siblingPointersList = new ArrayList<>(siblingInternalNode.getChildPointersList(degree));
            InternalClusterTreeNode.ChildPointers<K> siblingFirstChildPointer = siblingPointersList.removeFirst();
            siblingInternalNode.setChildPointers(siblingPointersList, degree, true);

            K currKey = parent.getKeyList(degree).get(idx);
            parent.setKey(idx, siblingFirstChildPointer.getKey());

            InternalClusterTreeNode<K> childInternalNode = (InternalClusterTreeNode<K>) child;

            // we put removed sibling child at right side of newly added key
            // if child looks empty it definitely still has aa child pointer despite keys being empty
            // this happens due to merge calls on lower level or removal of the key in internal node
            if (!childInternalNode.getKeyList(degree).isEmpty()){
                ArrayList<InternalClusterTreeNode.ChildPointers<K>> childPointersList2 = new ArrayList<>(childInternalNode.getChildPointersList(degree));
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
            LeafClusterTreeNode<K> siblingLeafNode = (LeafClusterTreeNode<K>) sibling;
            LeafClusterTreeNode<K> childLeafNode = (LeafClusterTreeNode<K>) child;

            List<LeafClusterTreeNode.KeyValue<K>> keyValueList = new ArrayList<>(siblingLeafNode.getKeyValueList(degree));
            LeafClusterTreeNode.KeyValue<K> keyValue = keyValueList.removeFirst();
            siblingLeafNode.setKeyValues(keyValueList, degree);
            parent.setKey(idx, keyValueList.getFirst().key());
            childLeafNode.addKeyValue(keyValue, degree);

        }

        indexIOSession.update(parent, child, sibling);
    }

    /**
     * Merges the child node at idx with its next sibling.
     * Next sibling may be left or right depending on which one  is available (right has priority)
     * If sibling has more keys than current node we merge current to sibling (not sibling to current)
     * @param parent The current node parent.
     * @param child The current node. We will merge into child (we may switch it with sibling first)
     * @param idx The index of the child parent to merge.
     */
    private void merge(InternalClusterTreeNode<K> parent, BaseClusterTreeNode<K> child, int idx) throws ExecutionException, InterruptedException, IOException {
        int siblingIndex = idx + 1;
        if (idx == parent.getChildrenList().size() - 1){
            siblingIndex = idx - 1;
        }
        BaseClusterTreeNode<K> sibling = indexIOSession.read(parent.getChildrenList().get(siblingIndex));
        BaseClusterTreeNode<K> toRemove = sibling;

        if (sibling.getKeyList(degree).size() > child.getKeyList(degree).size()){
            // Sibling has more keys, lets merge from child to sibling and remove child
            BaseClusterTreeNode<K> temp = child;
            child = sibling;
            sibling = temp;
            toRemove = sibling;
            int tempSib = siblingIndex;
            siblingIndex = idx;
            idx = tempSib;
        }

        /*
         *  For internal node we grab all sibling keys and add them to child
         *  We also add sibling 'child pointers' at the beginning or end of child's 'child pointers' depending on their position
         *  If sibling is after current child, its pointer are appended, otherwise they prepend
         */
        if (!child.isLeaf()){

            InternalClusterTreeNode<K> childInternalTreeNode = (InternalClusterTreeNode<K>) child;
            List<K> childKeyList = new ArrayList<>(childInternalTreeNode.getKeyList(degree));
            childKeyList.addAll(sibling.getKeyList(degree));
            childKeyList.sort(K::compareTo);
            childInternalTreeNode.setKeys(childKeyList);

            ArrayList<Pointer> childPointers = new ArrayList<>(childInternalTreeNode.getChildrenList());
            // Prepend or append
            if (idx > siblingIndex){
                childPointers.addAll(0, ((InternalClusterTreeNode<K>) sibling).getChildrenList());
            } else {
                childPointers.addAll(((InternalClusterTreeNode<K>) sibling).getChildrenList());
            }
            childInternalTreeNode.setChildren(childPointers);

        } else {
            LeafClusterTreeNode<K> childLeafTreeNode = (LeafClusterTreeNode<K>) child;
            ArrayList<LeafClusterTreeNode.KeyValue<K>> keyValueList = new ArrayList<>(childLeafTreeNode.getKeyValueList(degree));
            keyValueList.addAll(((LeafClusterTreeNode<K>) sibling).getKeyValueList(degree));
            Collections.sort(keyValueList);
            ((LeafClusterTreeNode<K>) child).setKeyValues(keyValueList, degree);
        }

        int keyToRemoveIndex = siblingIndex == 0 ? siblingIndex : siblingIndex - 1;
        K parentKeyAtIndex = parent.getKeyList(degree).get(keyToRemoveIndex);

        parent.removeKey(keyToRemoveIndex, degree);
        parent.removeChild(siblingIndex, degree);

        /*
         *   We may have removed the only key remaining in the parent
         *   In such case, we mark parent as removed and move it's key to child
         *   If it was root, child would be new root
         */
        if (parent.getKeyList(degree).isEmpty()){
            if (!child.isLeaf()) {
                assert child instanceof InternalClusterTreeNode;
                ((InternalClusterTreeNode<K>) child).addKey(parentKeyAtIndex, degree);
            }
            if (parent.isRoot()){
                child.setAsRoot();
                parent.unsetAsRoot();
            }
            indexIOSession.update(child);
            indexIOSession.remove(parent);
        }else{
            indexIOSession.update(parent, child);
        }
        if (toRemove.isLeaf()) {
            assert toRemove instanceof LeafClusterTreeNode<K>;
            this.connectSiblings((LeafClusterTreeNode<K>) toRemove);
        }
        indexIOSession.remove(toRemove);
    }

    private void connectSiblings(LeafClusterTreeNode<K> node) throws ExecutionException, InterruptedException, IOException {
        Optional<Pointer> optionalNextSiblingPointer = node.getNextSiblingPointer(degree);
        Optional<Pointer> optionalPreviousSiblingPointer = node.getPreviousSiblingPointer(degree);
        if (optionalNextSiblingPointer.isPresent()){
            LeafClusterTreeNode<K> nextNode = (LeafClusterTreeNode<K>) indexIOSession.read(optionalNextSiblingPointer.get());
            if (optionalPreviousSiblingPointer.isPresent()){
                nextNode.setPreviousSiblingPointer(optionalPreviousSiblingPointer.get(), degree);
            } else {
                nextNode.setPreviousSiblingPointer(Pointer.empty(), degree);
            }
            indexIOSession.update(nextNode);
        }

        if (optionalPreviousSiblingPointer.isPresent()){
            LeafClusterTreeNode<K> previousNode = (LeafClusterTreeNode<K>) indexIOSession.read(optionalPreviousSiblingPointer.get());
            if (optionalNextSiblingPointer.isPresent()){
                previousNode.setNextSiblingPointer(optionalNextSiblingPointer.get(), degree);
            } else {
                previousNode.setNextSiblingPointer(Pointer.empty(), degree);
            }
            indexIOSession.update(previousNode);
        }

    }


}
