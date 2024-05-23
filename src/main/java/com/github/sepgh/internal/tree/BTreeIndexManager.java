package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.tree.node.BaseTreeNode;
import com.github.sepgh.internal.tree.node.InternalTreeNode;
import com.github.sepgh.internal.tree.node.LeafTreeNode;
import com.google.common.hash.HashCode;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class BTreeIndexManager implements IndexManager {

    private final IndexStorageManager indexStorageManager;
    private final int degree;
    private final int minKeys;

    public BTreeIndexManager(int degree, IndexStorageManager indexStorageManager){
        this.degree = degree;
        this.indexStorageManager = indexStorageManager;
        this.minKeys = (degree - 1) / 2;
    }

    @Override
    public BaseTreeNode addIndex(int table, long identifier, Pointer pointer) throws ExecutionException, InterruptedException, IOException {

        BaseTreeNode root = getRoot(table);

        List<BaseTreeNode> path = new LinkedList<>();
        getPathToResponsibleNode(path, root, identifier, table);


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
                    TreeNodeIO.write(currentNode, indexStorageManager, table).get();
                    return currentNode;
                }

                /* Current node didn't have any space, so let's create a sibling and split */
                LeafTreeNode newSiblingLeafNode = new LeafTreeNode(indexStorageManager.getEmptyNode());
                List<LeafTreeNode.KeyValue> passingKeyValues = ((LeafTreeNode) currentNode).split(identifier, pointer, degree);
                newSiblingLeafNode.setKeyValues(passingKeyValues, degree);
                TreeNodeIO.write(newSiblingLeafNode, indexStorageManager, table).get(); // we want the node to have a pointer so that we can fix siblings
                /* Fix sibling pointers */
                fixSiblingPointers((LeafTreeNode) currentNode, newSiblingLeafNode, table);
                TreeNodeIO.write(newSiblingLeafNode, indexStorageManager, table).get();
                TreeNodeIO.write(currentNode, indexStorageManager, table).get();

                answer = currentNode.getKeyList(degree).contains(identifier) ? currentNode : newSiblingLeafNode;

                /* this leaf doesn't have a parent! create one and deal with it right here! */
                if (path.size() == 1) {

                    currentNode.unsetAsRoot();
                    InternalTreeNode newRoot = new InternalTreeNode(indexStorageManager.getEmptyNode());
                    newRoot.setAsRoot();
                    newRoot.addChildPointers(
                            passingKeyValues.get(0).key(),
                            currentNode.getPointer(),
                            newSiblingLeafNode.getPointer(),
                            degree,
                            false
                    );
                    TreeNodeIO.write(newRoot, indexStorageManager, table).get();
                    TreeNodeIO.write(currentNode, indexStorageManager, table).get();

                    return answer;
                }

                newChildForParent = newSiblingLeafNode;
                idForParentToStore = passingKeyValues.get(0).key();
            } else {

                /* current node is an internal node */

                InternalTreeNode currentInternalTreeNode = (InternalTreeNode) currentNode;
                if (currentInternalTreeNode.getKeyList(degree).size() < degree - 1) {
                    /* current internal node can store the key */
                    currentInternalTreeNode.addChildPointers(idForParentToStore, null, newChildForParent.getPointer(), degree, false);
                    TreeNodeIO.write(currentInternalTreeNode, indexStorageManager, table).get();
                    return answer;
                }


                /* current internal node cant store the key, split and ask parent */
                List<InternalTreeNode.ChildPointers> passingChildPointers = currentInternalTreeNode.addAndSplit(idForParentToStore, newChildForParent.getPointer(), degree);
                InternalTreeNode.ChildPointers firstPassingChildPointers = passingChildPointers.getFirst();
                idForParentToStore = firstPassingChildPointers.getKey();
                passingChildPointers.remove(0);

                InternalTreeNode newInternalSibling = new InternalTreeNode(indexStorageManager.getEmptyNode());
                newInternalSibling.setChildPointers(passingChildPointers, degree, true);
                TreeNodeIO.write(newInternalSibling, indexStorageManager, table).get();


                // Current node was root and needs a new parent
                if (currentInternalTreeNode.isRoot()){
                    currentInternalTreeNode.unsetAsRoot();
                    InternalTreeNode newRoot = new InternalTreeNode(indexStorageManager.getEmptyNode());
                    newRoot.setAsRoot();

                    newRoot.addChildPointers(
                            idForParentToStore,
                            currentNode.getPointer(),
                            newInternalSibling.getPointer(),
                            degree,
                            false
                    );
                    TreeNodeIO.write(newRoot, indexStorageManager, table).get();
                    TreeNodeIO.write(currentInternalTreeNode, indexStorageManager, table).get();
                    return answer;
                } else {
                    TreeNodeIO.write(currentInternalTreeNode, indexStorageManager, table).get();
                }

            }
        }

        throw new RuntimeException("Logic error: probably failed to store index?");
    }

    @Override
    public Optional<Pointer> getIndex(int table, long identifier) throws ExecutionException, InterruptedException {
        Optional<LeafTreeNode> optionalBaseTreeNode = this.getResponsibleNode(getRoot(table), identifier, table);
        if (optionalBaseTreeNode.isPresent()){
            for (LeafTreeNode.KeyValue entry : optionalBaseTreeNode.get().getKeyValueList(degree)) {
                if (entry.key() == identifier)
                    return Optional.of(entry.value());
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean removeIndex(int table, long identifier) throws ExecutionException, InterruptedException, IOException {
        BaseTreeNode root = getRoot(table);

        List<BaseTreeNode> path = new LinkedList<>();
        getPathToResponsibleNode(path, root, identifier, table);

        boolean result = false;

        for (int i = 0; i < path.size(); i++){
            BaseTreeNode currentNode = path.get(i);

            if (i == 0){   // Leaf
                LeafTreeNode leafNode = (LeafTreeNode) currentNode;
                result = leafNode.removeKeyValue(identifier, degree);
                TreeNodeIO.update(indexStorageManager, table, leafNode);

                if (result && !leafNode.isRoot() && leafNode.getKeyList(degree).size() < minKeys){   // Under filled
                    this.fillLeaf(table, leafNode, path, i, identifier);
                }
            } else {  // internal
                this.checkInternalNode(table, (InternalTreeNode) path.get(i), path, i, identifier);
            }
        }

        return result;
    }

    private void deleteInternalNode(int table, InternalTreeNode parent, InternalTreeNode node, int idx) throws ExecutionException, InterruptedException, IOException {
        List<Pointer> childrenList = node.getChildrenList();
        if (idx != 0){
            BaseTreeNode leftIDXChild = TreeNodeIO.read(indexStorageManager, table, childrenList.get(idx - 1));
            if (leftIDXChild.getKeyList(degree).size() >= minKeys){
                long pred = this.getPredecessor(table, node, idx);
                node.setKey(idx, pred);
                TreeNodeIO.update(indexStorageManager, table, node);
            }
        } else {
            BaseTreeNode rightIDXChild = TreeNodeIO.read(indexStorageManager, table, childrenList.get(idx + 1));
            if (rightIDXChild.getKeyList(degree).size() >= minKeys) {
                long succ = getSuccessor(table, node, idx);
                node.setKey(idx, succ);
                TreeNodeIO.update(indexStorageManager, table, node);
            } else {
                merge(table, parent, node, idx);
            }
        }

    }

    private long getPredecessor(int table, InternalTreeNode node, int idx) throws ExecutionException, InterruptedException {
        BaseTreeNode cur = TreeNodeIO.read(indexStorageManager, table, node.getChildrenList().get(idx));
        while (!cur.isLeaf()) {
            cur = TreeNodeIO.read(indexStorageManager, table, node.getChildrenList().get(cur.getKeyList(degree).size()));
        }
        return cur.getKeyList(degree).getLast();
    }

    private long getSuccessor(int table, InternalTreeNode node, int idx) throws ExecutionException, InterruptedException {
        BaseTreeNode cur = TreeNodeIO.read(indexStorageManager, table, node.getChildrenList().get(idx + 1));
        while (!cur.isLeaf()) {
            cur = TreeNodeIO.read(indexStorageManager, table, node.getChildrenList().get(0));
        }
        return cur.getKeyList(degree).getFirst();
    }

    private void checkInternalNode(int table, InternalTreeNode internalTreeNode, List<BaseTreeNode> path, int nodeIndex, long identifier) throws ExecutionException, InterruptedException, IOException {
        List<Long> keyList = internalTreeNode.getKeyList(degree);
        if (nodeIndex == path.size() - 1 && keyList.isEmpty())
            return;
        int indexOfKey = keyList.indexOf(identifier);
        if (indexOfKey != -1){
            this.deleteInternalNode(table, (InternalTreeNode) path.get(nodeIndex + 1), internalTreeNode, indexOfKey);
        }

        int nodeKeySize = internalTreeNode.getKeyList(degree).size();
        if (nodeKeySize < minKeys && !internalTreeNode.isRoot()){
            InternalTreeNode parent = (InternalTreeNode) path.get(nodeIndex + 1);
            this.fillInternal(table, internalTreeNode, parent, parent.getIndexOfChild(internalTreeNode.getPointer()));
        }
    }

    private void fillLeaf(int table, LeafTreeNode node, List<BaseTreeNode> path, int currentIndex, long identifier) throws ExecutionException, InterruptedException, IOException {
        InternalTreeNode parentNode = (InternalTreeNode) path.get(currentIndex + 1);

        int idx = parentNode.getIndexOfChild(node.getPointer());
        boolean borrowed = false;
        if (idx == 0){  // Leaf was at the beginning, check if we can borrow from right
            borrowed = tryBorrowRight(table, parentNode, idx, node);
        } else {
            // Leaf was not at the beginning, check if we can borrow from left first
            borrowed = tryBorrowLeft(table, parentNode, idx, node);

            if (!borrowed && idx < parentNode.getKeyList(degree).size() - 1){
                // we may still be able to borrow from right despite leaf not being at beginning, only if idx is not for last node
                borrowed = tryBorrowRight(table, parentNode, idx, node);
            }
        }

        if (!borrowed){
            merge(table, parentNode, node, idx);
        }

    }

    private boolean tryBorrowRight(int table, InternalTreeNode parentNode, int idx, BaseTreeNode child) throws ExecutionException, InterruptedException, IOException {
        BaseTreeNode sibling = TreeNodeIO.read(indexStorageManager, table, parentNode.getChildrenList().get(idx + 1));
        if (sibling.getKeyList(degree).size() > minKeys){
            this.borrowFromNext(table, parentNode, idx, child);
            return true;
        }
        return false;
    }

    private boolean tryBorrowLeft(int table, InternalTreeNode parentNode, int idx, BaseTreeNode child) throws ExecutionException, InterruptedException, IOException {
        BaseTreeNode sibling = TreeNodeIO.read(indexStorageManager, table, parentNode.getChildrenList().get(idx - 1));
        if (sibling.getKeyList(degree).size() > minKeys){
            this.borrowFromPrev(table, parentNode, idx, child);
            return true;
        }
        return false;
    }

    /**
     * Fills the child node at idx if it has less than minDegree keys.
     * @param node The parent node.
     * @param idx The index of the child node to fill.
     */
    private void fillInternal(int table, InternalTreeNode node, InternalTreeNode parent, int idx) throws ExecutionException, InterruptedException, IOException {
        boolean borrowed = false;
        if (idx == 0){  // Leaf was at the beginning, check if we can borrow from right
            borrowed = tryBorrowRight(table, parent, idx, node);
        } else {
            // Leaf was not at the beginning, check if we can borrow from left first
            borrowed = tryBorrowLeft(table, parent, idx, node);

            if (!borrowed && idx < parent.getKeyList(degree).size() - 1){
                // we may still be able to borrow from right despite leaf not being at beginning, only if idx is not for last node
                borrowed = tryBorrowRight(table, parent, idx, node);
            }
        }

        if (!borrowed){
            merge(table, parent, node, idx);
        }
    }

    /**
     * Borrows a key from the previous sibling and moves it to the child at idx.
     * @param node The parent node.
     * @param idx The index of the child node to borrow for.
     */
    private void borrowFromPrev(int table, InternalTreeNode node, int idx, @Nullable BaseTreeNode optionalChild) throws ExecutionException, InterruptedException, IOException {
        BaseTreeNode child = optionalChild != null ? optionalChild : TreeNodeIO.read(indexStorageManager, table, node.getChildrenList().get(idx));
        BaseTreeNode sibling = TreeNodeIO.read(indexStorageManager, table, node.getChildrenList().get(idx - 1));

        if (!child.isLeaf()){
            InternalTreeNode siblingInternalNode = (InternalTreeNode) sibling;
            List<InternalTreeNode.ChildPointers> childPointersList = new ArrayList<>(siblingInternalNode.getChildPointersList(degree));
            InternalTreeNode.ChildPointers lastChildPointer = childPointersList.removeLast();
            siblingInternalNode.setChildPointers(childPointersList, degree, true);

            long currKey = node.getKeyList(degree).get(idx - 1);
            node.setKey(idx - 1, lastChildPointer.getKey());

            InternalTreeNode childInternalNode = (InternalTreeNode) child;
            if (!childInternalNode.getKeyList(degree).isEmpty()){
                ArrayList<InternalTreeNode.ChildPointers> childPointersList2 = new ArrayList<>(childInternalNode.getChildPointersList(degree));
                lastChildPointer.setRight(childPointersList2.getFirst().getLeft());
                lastChildPointer.setKey(currKey);
                lastChildPointer.setLeft(lastChildPointer.getRight());
                childPointersList2.add(lastChildPointer);
                childInternalNode.setChildPointers(childPointersList2, degree, true);   // todo: probably no need to clean? it was short
            } else {
                Pointer first = childInternalNode.getChildrenList().getFirst();
                childInternalNode.addChildPointers(currKey, lastChildPointer.getRight(), first, degree, true);
            }

        } else {
            LeafTreeNode siblingLeafNode = (LeafTreeNode) sibling;
            LeafTreeNode childLeafNode = (LeafTreeNode) child;

            List<LeafTreeNode.KeyValue> keyValueList = new ArrayList<>(siblingLeafNode.getKeyValueList(degree));
            LeafTreeNode.KeyValue keyValue = keyValueList.removeLast();
            siblingLeafNode.setKeyValues(keyValueList, degree);


            node.setKey(idx - 1, keyValue.key());

            childLeafNode.addKeyValue(keyValue, degree);
        }
        TreeNodeIO.update(indexStorageManager, table, node, child, sibling);
    }

    private void borrowFromNext(int table, InternalTreeNode node, int idx, @Nullable BaseTreeNode optionalChild) throws ExecutionException, InterruptedException, IOException {
        BaseTreeNode child = optionalChild != null ? optionalChild : TreeNodeIO.read(indexStorageManager, table, node.getChildrenList().get(idx));
        BaseTreeNode sibling = TreeNodeIO.read(indexStorageManager, table, node.getChildrenList().get(idx + 1));
        if (!child.isLeaf()){
            InternalTreeNode siblingInternalNode = (InternalTreeNode) sibling;
            List<InternalTreeNode.ChildPointers> siblingPointersList = new ArrayList<>(siblingInternalNode.getChildPointersList(degree));
            InternalTreeNode.ChildPointers firstChildPointer = siblingPointersList.removeFirst();
            siblingInternalNode.setChildPointers(siblingPointersList, degree, true);

            long currKey = node.getKeyList(degree).get(idx);
            node.setKey(idx, firstChildPointer.getKey());

            InternalTreeNode childInternalNode = (InternalTreeNode) child;
            if (!childInternalNode.getKeyList(degree).isEmpty()){
                ArrayList<InternalTreeNode.ChildPointers> childPointersList2 = new ArrayList<>(childInternalNode.getChildPointersList(degree));
                firstChildPointer.setRight(firstChildPointer.getLeft());
                firstChildPointer.setKey(currKey);
                firstChildPointer.setLeft(childPointersList2.getLast().getRight());
                childPointersList2.add(firstChildPointer);
                childInternalNode.setChildPointers(childPointersList2, degree, true);   // todo: probably no need to clean? it was short
            } else {
                Pointer first = childInternalNode.getChildrenList().getFirst();
                childInternalNode.addChildPointers(currKey, first, firstChildPointer.getLeft(), degree, true);
            }

        } else {
            LeafTreeNode siblingLeafNode = (LeafTreeNode) sibling;
            LeafTreeNode childLeafNode = (LeafTreeNode) child;

            List<LeafTreeNode.KeyValue> keyValueList = new ArrayList<>(siblingLeafNode.getKeyValueList(degree));
            LeafTreeNode.KeyValue keyValue = keyValueList.removeFirst();
            siblingLeafNode.setKeyValues(keyValueList, degree);
            node.setKey(idx, keyValueList.getFirst().key());
            childLeafNode.addKeyValue(keyValue, degree);

        }

        TreeNodeIO.update(indexStorageManager, table, node, child, sibling);
    }

    /**
     * Merges the child node at idx with its next sibling.
     * @param parent The parent parent.
     * @param idx The index of the child parent to merge.
     */
    private void merge(int table, InternalTreeNode parent, BaseTreeNode child, int idx) throws ExecutionException, InterruptedException, IOException {
        int siblingIndex = idx + 1;
        if (idx == parent.getChildrenList().size() - 1){
            siblingIndex = idx - 1;
        }
        BaseTreeNode sibling = TreeNodeIO.read(indexStorageManager, table, parent.getChildrenList().get(siblingIndex));
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


        if (!child.isLeaf()){
            InternalTreeNode childInternalTreeNode = (InternalTreeNode) child;
            List<Long> childKeyList = new ArrayList<>(childInternalTreeNode.getKeyList(degree));
            childKeyList.addAll(sibling.getKeyList(degree));
            childKeyList.sort(Long::compareTo);
            childInternalTreeNode.setKeys(childKeyList);

            ArrayList<Pointer> childPointers = new ArrayList<>(childInternalTreeNode.getChildrenList());
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
        }
        int keyToRemoveIndex = siblingIndex == 0 ? siblingIndex : siblingIndex - 1;
        long parentKeyAtIndex = parent.getKeyList(degree).get(keyToRemoveIndex);

        parent.removeKey(keyToRemoveIndex, degree);
        parent.removeChild(siblingIndex, degree);
        if (parent.getKeyList(degree).isEmpty()){
            if (!child.isLeaf())
                ((InternalTreeNode) child).addKey(parentKeyAtIndex, degree);
            if (parent.isRoot()){
                child.setAsRoot();
                parent.unsetAsRoot();
            }
            TreeNodeIO.update(indexStorageManager, table, child);
            TreeNodeIO.remove(indexStorageManager, table, parent);
        }else{
            TreeNodeIO.update(indexStorageManager, table, parent, child);
        }
        TreeNodeIO.remove(indexStorageManager, table, toRemove);

    }


    private BaseTreeNode getRoot(int table) throws ExecutionException, InterruptedException {
        Optional<IndexStorageManager.NodeData> optionalNodeData = indexStorageManager.getRoot(table).get();
        if (optionalNodeData.isPresent()){
            return TreeNodeIO.read(indexStorageManager, table, optionalNodeData.get().pointer());
        }

        byte[] emptyNode = indexStorageManager.getEmptyNode();
        LeafTreeNode leafTreeNode = (LeafTreeNode) BaseTreeNode.fromBytes(emptyNode, BaseTreeNode.Type.LEAF);
        leafTreeNode.setAsRoot();

        IndexStorageManager.NodeData nodeData = indexStorageManager.fillRoot(table, leafTreeNode.getData()).get();
        leafTreeNode.setPointer(nodeData.pointer());
        return leafTreeNode;
    }

    private void fixSiblingPointers(LeafTreeNode currentNode, LeafTreeNode newLeafTreeNode, int table) throws ExecutionException, InterruptedException, IOException {
        Optional<Pointer> currentNodeNextSiblingPointer = currentNode.getNextSiblingPointer(degree);
        currentNode.setNextSiblingPointer(newLeafTreeNode.getPointer(), degree);
        newLeafTreeNode.setPreviousSiblingPointer(currentNode.getPointer(), degree);
        if (currentNodeNextSiblingPointer.isPresent()){
            newLeafTreeNode.setNextSiblingPointer(currentNodeNextSiblingPointer.get(), degree);

            LeafTreeNode currentNextSibling = (LeafTreeNode) TreeNodeIO.read(indexStorageManager, table, currentNodeNextSiblingPointer.get());
            currentNextSibling.setPreviousSiblingPointer(newLeafTreeNode.getPointer(), degree);
            TreeNodeIO.write(currentNextSibling, indexStorageManager, table).get();
        }
    }

    private Optional<LeafTreeNode> getResponsibleNode(BaseTreeNode node, long identifier, int table) throws ExecutionException, InterruptedException {
        if (node.isLeaf()){
            return Optional.of((LeafTreeNode) node);
        }
        List<InternalTreeNode.ChildPointers> childPointersList = ((InternalTreeNode) node).getChildPointersList(degree);
        for (int i = 0; i < childPointersList.size(); i++){
            InternalTreeNode.ChildPointers childPointers = childPointersList.get(i);
            if (childPointers.getKey() > identifier && childPointers.getLeft() != null){
                return getResponsibleNode(
                        TreeNodeIO.read(indexStorageManager, table, childPointers.getLeft()),
                        identifier,
                        table
                );
            }
            if (i == childPointersList.size() - 1 && childPointers.getRight() != null){
                return getResponsibleNode(
                        TreeNodeIO.read(indexStorageManager, table, childPointers.getRight()),
                        identifier,
                        table
                );
            }
        }
        return Optional.empty();
    }

    private void getPathToResponsibleNode(List<BaseTreeNode> path, BaseTreeNode node, long identifier, int table) throws ExecutionException, InterruptedException {
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
                        path,
                        TreeNodeIO.read(indexStorageManager, table, childPointers.getLeft()),
                        identifier,
                        table
                );
                return;
            }
            if (i == childPointersList.size() - 1 && childPointers.getRight() != null){
                path.addFirst(node);
                getPathToResponsibleNode(
                        path,
                        TreeNodeIO.read(indexStorageManager, table, childPointers.getRight()),
                        identifier,
                        table
                );
                return;
            }
        }
    }


}
