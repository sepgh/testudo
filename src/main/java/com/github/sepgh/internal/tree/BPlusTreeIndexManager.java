package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.tree.node.BaseTreeNode;
import com.github.sepgh.internal.tree.node.InternalTreeNode;
import com.github.sepgh.internal.tree.node.LeafTreeNode;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class BPlusTreeIndexManager implements IndexManager {

    private final IndexStorageManager indexStorageManager;
    private final int degree;
    private final int minKeys;

    public BPlusTreeIndexManager(int degree, IndexStorageManager indexStorageManager){
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
                            passingKeyValues.getFirst().key(),
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
                idForParentToStore = passingKeyValues.getFirst().key();
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
                passingChildPointers.removeFirst();

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
        LeafTreeNode baseTreeNode = this.getResponsibleNode(getRoot(table), identifier, table);
        for (LeafTreeNode.KeyValue entry : baseTreeNode.getKeyValueList(degree)) {
            if (entry.key() == identifier)
                return Optional.of(entry.value());
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
                    InternalTreeNode parentNode = (InternalTreeNode) path.get(i + 1);
                    this.fillNode(table, leafNode, parentNode, parentNode.getIndexOfChild(currentNode.getPointer()));
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
            cur = TreeNodeIO.read(indexStorageManager, table, node.getChildrenList().getFirst());
        }
        return cur.getKeyList(degree).getFirst();
    }

    private void checkInternalNode(int table, InternalTreeNode internalTreeNode, List<BaseTreeNode> path, int nodeIndex, long identifier) throws ExecutionException, InterruptedException, IOException {
        List<Long> keyList = internalTreeNode.getKeyList(degree);
        if (nodeIndex == path.size() - 1 && keyList.isEmpty())
            return;
        int indexOfKey = keyList.indexOf(identifier);
        if (indexOfKey != -1){
            if (internalTreeNode.isRoot()){
                this.fillRootAtIndex(table, internalTreeNode, indexOfKey, identifier);
            } else {
                this.deleteInternalNode(table, (InternalTreeNode) path.get(nodeIndex + 1), internalTreeNode, indexOfKey);
            }
        }

        int nodeKeySize = internalTreeNode.getKeyList(degree).size();
        if (nodeKeySize < minKeys && !internalTreeNode.isRoot()){
            InternalTreeNode parent = (InternalTreeNode) path.get(nodeIndex + 1);
            this.fillNode(table, internalTreeNode, parent, parent.getIndexOfChild(internalTreeNode.getPointer()));
        }
    }

    private void fillRootAtIndex(int table, InternalTreeNode internalTreeNode, int indexOfKey, long identifier) throws ExecutionException, InterruptedException, IOException {
        LeafTreeNode leafTreeNode = getResponsibleNode(
                TreeNodeIO.read(indexStorageManager, table, internalTreeNode.getChildAtIndex(indexOfKey + 1)),
                identifier,
                table
        );

        internalTreeNode.setKey(indexOfKey, leafTreeNode.getKeyList(degree).getLast());
        TreeNodeIO.update(indexStorageManager, table, internalTreeNode);
    }

    private void fillNode(int table, BaseTreeNode currentNode, InternalTreeNode parentNode, int idx) throws IOException, ExecutionException, InterruptedException {
        boolean borrowed;
        if (idx == 0){  // Leaf was at the beginning, check if we can borrow from right
            borrowed = tryBorrowRight(table, parentNode, idx, currentNode);
        } else {
            // Leaf was not at the beginning, check if we can borrow from left first
            borrowed = tryBorrowLeft(table, parentNode, idx, currentNode);

            if (!borrowed && idx < parentNode.getKeyList(degree).size() - 1){
                // we may still be able to borrow from right despite leaf not being at beginning, only if idx is not for last node
                borrowed = tryBorrowRight(table, parentNode, idx, currentNode);
            }
        }

        // We could not borrow from neither sides. Merge nodes
        if (!borrowed){
            merge(table, parentNode, currentNode, idx);
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
     * Borrow a node from sibling at left (previous)
     * We remove sibling last child pointer and pass it to child (current node)
     * If child (and sibling) is an internal node, then we be careful that the child may already "LOOK EMPTY" in terms of keys,
     *      but still include child pointers, so the position of sibling first child entrance to child's children is important
     *
     * @param table table id
     * @param parent node
     * @param idx index of child in parent
     * @param optionalChild nullable child node, if not provided it will be calculated based on idx
     */
    private void borrowFromPrev(int table, InternalTreeNode parent, int idx, @Nullable BaseTreeNode optionalChild) throws ExecutionException, InterruptedException, IOException {
        BaseTreeNode child = optionalChild != null ? optionalChild : TreeNodeIO.read(indexStorageManager, table, parent.getChildrenList().get(idx));
        BaseTreeNode sibling = TreeNodeIO.read(indexStorageManager, table, parent.getChildrenList().get(idx - 1));

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
        TreeNodeIO.update(indexStorageManager, table, parent, child, sibling);
    }

    /**
     * Borrow a node from sibling at right (next)
     * We remove sibling first child pointer and pass it to child (current node)
     * If child (and sibling) is an internal node, then we be careful that the child may already "LOOK EMPTY" in terms of keys,
     *      but still include child pointers, so the position of sibling first child entrance to child's children is important
     *
     * @param table table id
     * @param parent node
     * @param idx index of child in parent
     * @param optionalChild nullable child node, if not provided it will be calculated based on idx
     */
    private void borrowFromNext(int table, InternalTreeNode parent, int idx, @Nullable BaseTreeNode optionalChild) throws ExecutionException, InterruptedException, IOException {
        BaseTreeNode child = optionalChild != null ? optionalChild : TreeNodeIO.read(indexStorageManager, table, parent.getChildrenList().get(idx));
        BaseTreeNode sibling = TreeNodeIO.read(indexStorageManager, table, parent.getChildrenList().get(idx + 1));
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

        TreeNodeIO.update(indexStorageManager, table, parent, child, sibling);
    }

    /**
     * Merges the child node at idx with its next sibling.
     * Next sibling may be left or right depending on which one  is available (right has priority)
     * If sibling has more keys than current node we merge current to sibling (not sibling to current)
     * @param parent The current node parent.
     * @param child The current node. We will merge into child (we may switch it with sibling first)
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

    private LeafTreeNode getResponsibleNode(BaseTreeNode node, long identifier, int table) throws ExecutionException, InterruptedException {
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

        if (flag){
            return getResponsibleNode(
                    TreeNodeIO.read(indexStorageManager, table, childrenList.get(i)),
                    identifier,
                    table
            );
        } else {
            return getResponsibleNode(
                    TreeNodeIO.read(indexStorageManager, table, childrenList.getLast()),
                    identifier,
                    table
            );
        }

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
