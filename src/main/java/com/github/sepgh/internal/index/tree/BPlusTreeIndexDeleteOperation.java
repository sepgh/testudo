package com.github.sepgh.internal.index.tree;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.BaseTreeNode;
import com.github.sepgh.internal.index.tree.node.InternalTreeNode;
import com.github.sepgh.internal.index.tree.node.LeafTreeNode;
import com.github.sepgh.internal.storage.session.IndexIOSession;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class BPlusTreeIndexDeleteOperation {
    private final int degree;
    private final int table;
    private final IndexIOSession indexIOSession;
    private final int minKeys;

    public BPlusTreeIndexDeleteOperation(int degree, int table, IndexIOSession indexIOSession) {
        this.degree = degree;
        this.table = table;
        this.indexIOSession = indexIOSession;
        this.minKeys = (degree - 1) / 2;
    }

    public boolean removeIndex(BaseTreeNode root, long identifier) throws ExecutionException, InterruptedException, IOException {

        List<BaseTreeNode> path = new LinkedList<>();
        BPlusTreeUtils.getPathToResponsibleNode(indexIOSession, path, root, identifier, degree);

        boolean result = false;

        for (int i = 0; i < path.size(); i++){
            BaseTreeNode currentNode = path.get(i);

            if (i == 0){   // Leaf
                LeafTreeNode leafNode = (LeafTreeNode) currentNode;
                result = leafNode.removeKeyValue(identifier, degree);
                indexIOSession.update(leafNode);

                if (result && !leafNode.isRoot() && leafNode.getKeyList(degree).size() < minKeys){   // Under filled
                    InternalTreeNode parentNode = (InternalTreeNode) path.get(i + 1);
                    this.fillNode(leafNode, parentNode, parentNode.getIndexOfChild(currentNode.getPointer()));
                }
            } else {  // internal
                this.checkInternalNode((InternalTreeNode) path.get(i), path, i, identifier);
            }
        }
        indexIOSession.commit();

        return result;
    }


    private void deleteInternalNode(InternalTreeNode parent, InternalTreeNode node, int idx) throws ExecutionException, InterruptedException, IOException {
        List<Pointer> childrenList = node.getChildrenList();
        if (idx != 0){
            BaseTreeNode leftIDXChild = indexIOSession.read(childrenList.get(idx - 1));
            if (leftIDXChild.getKeyList(degree).size() >= minKeys){
                long pred = this.getPredecessor(node, idx);
                node.setKey(idx, pred);
                indexIOSession.update(node);
            }
        } else {
            BaseTreeNode rightIDXChild = indexIOSession.read(childrenList.get(idx + 1));
            if (rightIDXChild.getKeyList(degree).size() >= minKeys) {
                long succ = getSuccessor(node, idx);
                node.setKey(idx, succ);
                indexIOSession.update(node);
            } else {
                merge(parent, node, idx);
            }
        }

    }

    private long getPredecessor(InternalTreeNode node, int idx) throws ExecutionException, InterruptedException {
        BaseTreeNode cur = indexIOSession.read(node.getChildrenList().get(idx));
        while (!cur.isLeaf()) {
            cur = indexIOSession.read(node.getChildrenList().get(cur.getKeyList(degree).size()));
        }
        return cur.getKeyList(degree).getLast();
    }

    private long getSuccessor(InternalTreeNode node, int idx) throws ExecutionException, InterruptedException {
        BaseTreeNode cur = indexIOSession.read(node.getChildrenList().get(idx + 1));
        while (!cur.isLeaf()) {
            cur = indexIOSession.read(((InternalTreeNode) cur).getChildrenList().getFirst());
        }
        return cur.getKeyList(degree).getFirst();
    }

    private void checkInternalNode(InternalTreeNode internalTreeNode, List<BaseTreeNode> path, int nodeIndex, long identifier) throws ExecutionException, InterruptedException, IOException {
        List<Long> keyList = internalTreeNode.getKeyList(degree);
        if (nodeIndex == path.size() - 1 && keyList.isEmpty())
            return;
        int indexOfKey = keyList.indexOf(identifier);
        if (indexOfKey != -1){
            if (internalTreeNode.isRoot()){
                this.fillRootAtIndex(internalTreeNode, indexOfKey, identifier);
            } else {
                this.deleteInternalNode((InternalTreeNode) path.get(nodeIndex + 1), internalTreeNode, indexOfKey);
            }
        }

        int nodeKeySize = internalTreeNode.getKeyList(degree).size();
        if (nodeKeySize < minKeys && !internalTreeNode.isRoot()){
            InternalTreeNode parent = (InternalTreeNode) path.get(nodeIndex + 1);
            this.fillNode(internalTreeNode, parent, parent.getIndexOfChild(internalTreeNode.getPointer()));
        }
    }

    private void fillRootAtIndex(InternalTreeNode internalTreeNode, int indexOfKey, long identifier) throws ExecutionException, InterruptedException, IOException {
        LeafTreeNode leafTreeNode = BPlusTreeUtils.getResponsibleNode(
                indexIOSession.getIndexStorageManager(),
                indexIOSession.read(internalTreeNode.getChildAtIndex(indexOfKey + 1)),
                identifier,
                table,
                degree
        );

        internalTreeNode.setKey(indexOfKey, leafTreeNode.getKeyList(degree).getLast());
        indexIOSession.update(internalTreeNode);
    }

    private void fillNode(BaseTreeNode currentNode, InternalTreeNode parentNode, int idx) throws IOException, ExecutionException, InterruptedException {
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

    private boolean tryBorrowRight(InternalTreeNode parentNode, int idx, BaseTreeNode child) throws ExecutionException, InterruptedException, IOException {
        BaseTreeNode sibling = indexIOSession.read(parentNode.getChildrenList().get(idx + 1));
        if (sibling.getKeyList(degree).size() > minKeys){
            this.borrowFromNext(parentNode, idx, child);
            return true;
        }
        return false;
    }

    private boolean tryBorrowLeft(InternalTreeNode parentNode, int idx, BaseTreeNode child) throws ExecutionException, InterruptedException, IOException {
        BaseTreeNode sibling = indexIOSession.read(parentNode.getChildrenList().get(idx - 1));
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
    private void borrowFromPrev(InternalTreeNode parent, int idx, @Nullable BaseTreeNode optionalChild) throws ExecutionException, InterruptedException, IOException {
        BaseTreeNode child = optionalChild != null ? optionalChild : indexIOSession.read(parent.getChildrenList().get(idx));
        BaseTreeNode sibling = indexIOSession.read(parent.getChildrenList().get(idx - 1));

        if (!child.isLeaf()){
            InternalTreeNode siblingInternalNode = (InternalTreeNode) sibling;
            List<InternalTreeNode.ChildPointers> childPointersList = new ArrayList<>(siblingInternalNode.getChildPointersList(degree));
            InternalTreeNode.ChildPointers siblingLastChildPointer = childPointersList.removeLast();
            siblingInternalNode.setChildPointers(childPointersList, degree, true);

            long currKey = parent.getKeyList(degree).get(idx - 1);
            parent.setKey(idx - 1, siblingLastChildPointer.getKey());

            // we put removed sibling child at left side of newly added key
            // if child looks empty it definitely still has aa child pointer despite keys being empty
            // this happens due to merge calls on lower level or removal of the key in internal node
            InternalTreeNode childInternalNode = (InternalTreeNode) child;
            if (!childInternalNode.getKeyList(degree).isEmpty()){
                ArrayList<InternalTreeNode.ChildPointers> childPointersList2 = new ArrayList<>(childInternalNode.getChildPointersList(degree));
                siblingLastChildPointer.setRight(childPointersList2.getFirst().getLeft());
                siblingLastChildPointer.setKey(currKey);
                siblingLastChildPointer.setLeft(siblingLastChildPointer.getRight());
                childPointersList2.add(siblingLastChildPointer);
                childInternalNode.setChildPointers(childPointersList2, degree, true);   // todo: probably no need to clean? it was short
            } else {
                Pointer first = childInternalNode.getChildrenList().getFirst();
                childInternalNode.addChildPointers(currKey, siblingLastChildPointer.getRight(), first, degree, true);
            }

        } else {
            LeafTreeNode siblingLeafNode = (LeafTreeNode) sibling;
            LeafTreeNode childLeafNode = (LeafTreeNode) child;

            List<LeafTreeNode.KeyValue> keyValueList = new ArrayList<>(siblingLeafNode.getKeyValueList(degree));
            LeafTreeNode.KeyValue keyValue = keyValueList.removeLast();
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
    private void borrowFromNext(InternalTreeNode parent, int idx, @Nullable BaseTreeNode optionalChild) throws ExecutionException, InterruptedException, IOException {
        BaseTreeNode child = optionalChild != null ? optionalChild : indexIOSession.read(parent.getChildrenList().get(idx));
        BaseTreeNode sibling = indexIOSession.read(parent.getChildrenList().get(idx + 1));

        if (!child.isLeaf()){
            InternalTreeNode siblingInternalNode = (InternalTreeNode) sibling;
            List<InternalTreeNode.ChildPointers> siblingPointersList = new ArrayList<>(siblingInternalNode.getChildPointersList(degree));
            InternalTreeNode.ChildPointers siblingFirstChildPointer = siblingPointersList.removeFirst();
            siblingInternalNode.setChildPointers(siblingPointersList, degree, true);

            long currKey = parent.getKeyList(degree).get(idx);
            parent.setKey(idx, siblingFirstChildPointer.getKey());

            InternalTreeNode childInternalNode = (InternalTreeNode) child;

            // we put removed sibling child at right side of newly added key
            // if child looks empty it definitely still has aa child pointer despite keys being empty
            // this happens due to merge calls on lower level or removal of the key in internal node
            if (!childInternalNode.getKeyList(degree).isEmpty()){
                ArrayList<InternalTreeNode.ChildPointers> childPointersList2 = new ArrayList<>(childInternalNode.getChildPointersList(degree));
                siblingFirstChildPointer.setRight(siblingFirstChildPointer.getLeft());
                siblingFirstChildPointer.setKey(currKey);
                siblingFirstChildPointer.setLeft(childPointersList2.getLast().getRight());
                childPointersList2.add(siblingFirstChildPointer);
                childInternalNode.setChildPointers(childPointersList2, degree, true);   // todo: probably no need to clean? it was short
            } else {
                Pointer first = childInternalNode.getChildrenList().getFirst();
                childInternalNode.addChildPointers(currKey, first, siblingFirstChildPointer.getLeft(), degree, true);
            }

        } else {
            LeafTreeNode siblingLeafNode = (LeafTreeNode) sibling;
            LeafTreeNode childLeafNode = (LeafTreeNode) child;

            List<LeafTreeNode.KeyValue> keyValueList = new ArrayList<>(siblingLeafNode.getKeyValueList(degree));
            LeafTreeNode.KeyValue keyValue = keyValueList.removeFirst();
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
    private void merge(InternalTreeNode parent, BaseTreeNode child, int idx) throws ExecutionException, InterruptedException, IOException {
        int siblingIndex = idx + 1;
        if (idx == parent.getChildrenList().size() - 1){
            siblingIndex = idx - 1;
        }
        BaseTreeNode sibling = indexIOSession.read(parent.getChildrenList().get(siblingIndex));
        BaseTreeNode toRemove = sibling;

        if (sibling.getKeyList(degree).size() > child.getKeyList(degree).size()){
            // Sibling has more keys, lets merge from child to sibling and remove child
            BaseTreeNode temp = child;
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
            InternalTreeNode childInternalTreeNode = (InternalTreeNode) child;
            List<Long> childKeyList = new ArrayList<>(childInternalTreeNode.getKeyList(degree));
            childKeyList.addAll(sibling.getKeyList(degree));
            childKeyList.sort(Long::compareTo);
            childInternalTreeNode.setKeys(childKeyList);

            ArrayList<Pointer> childPointers = new ArrayList<>(childInternalTreeNode.getChildrenList());
            // Prepend or append
            if (idx > siblingIndex){
                childPointers.addAll(0, ((InternalTreeNode) sibling).getChildrenList());
            } else {
                childPointers.addAll(((InternalTreeNode) sibling).getChildrenList());
            }
            childInternalTreeNode.setChildren(childPointers);

        } else {
            LeafTreeNode childLeafTreeNode = (LeafTreeNode) child;
            ArrayList<LeafTreeNode.KeyValue> keyValueList = new ArrayList<>(childLeafTreeNode.getKeyValueList(degree));
            keyValueList.addAll(((LeafTreeNode) sibling).getKeyValueList(degree));
            Collections.sort(keyValueList);
            ((LeafTreeNode) child).setKeyValues(keyValueList, degree);
        }

        int keyToRemoveIndex = siblingIndex == 0 ? siblingIndex : siblingIndex - 1;
        long parentKeyAtIndex = parent.getKeyList(degree).get(keyToRemoveIndex);

        parent.removeKey(keyToRemoveIndex, degree);
        parent.removeChild(siblingIndex, degree);

        /*
         *   We may have removed the only key remaining in the parent
         *   In such case, we mark parent as removed and move it's key to child
         *   If it was root, child would be new root
         */
        if (parent.getKeyList(degree).isEmpty()){
            if (!child.isLeaf()) {
                assert child instanceof InternalTreeNode;
                ((InternalTreeNode) child).addKey(parentKeyAtIndex, degree);
            }
            if (parent.isRoot()){
                child.setAsRoot();
                parent.unsetAsRoot();
            }
            indexIOSession.update(child);
            indexIOSession.remove(parent);
        }else{
            if (child.isLeaf()) {
                assert child instanceof LeafTreeNode;
                this.connectSiblings((LeafTreeNode) child);
            }
            indexIOSession.update(parent, child);
        }
        if (toRemove.isLeaf()) {
            assert toRemove instanceof LeafTreeNode;
            this.connectSiblings((LeafTreeNode) toRemove);
        }
        indexIOSession.remove(toRemove);
    }

    private void connectSiblings(LeafTreeNode toRemove) throws ExecutionException, InterruptedException, IOException {
        Optional<Pointer> optionalNextSiblingPointer = toRemove.getNextSiblingPointer(degree);
        Optional<Pointer> optionalPreviousSiblingPointer = toRemove.getPreviousSiblingPointer(degree);
        if (optionalNextSiblingPointer.isPresent()){
            LeafTreeNode nextNode = (LeafTreeNode) indexIOSession.read(optionalNextSiblingPointer.get());
            if (optionalPreviousSiblingPointer.isPresent()){
                nextNode.setPreviousSiblingPointer(optionalPreviousSiblingPointer.get(), degree);
            } else {
                nextNode.setPreviousSiblingPointer(Pointer.empty(), degree);
            }
            indexIOSession.update(nextNode);
        }

        if (optionalPreviousSiblingPointer.isPresent()){
            LeafTreeNode previousNode = (LeafTreeNode) indexIOSession.read(optionalPreviousSiblingPointer.get());
            if (optionalNextSiblingPointer.isPresent()){
                previousNode.setNextSiblingPointer(optionalNextSiblingPointer.get(), degree);
            } else {
                previousNode.setNextSiblingPointer(Pointer.empty(), degree);
            }
            indexIOSession.update(previousNode);
        }

    }


}
