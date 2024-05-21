package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.tree.node.BaseTreeNode;
import com.github.sepgh.internal.tree.node.InternalTreeNode;
import com.github.sepgh.internal.tree.node.LeafTreeNode;

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
    public boolean removeIndex(int table, long identifier) throws ExecutionException, InterruptedException {
        BaseTreeNode root = getRoot(table);
        return this.deleteRecursive(table, root, identifier);
    }

    private boolean deleteRecursive(int table, BaseTreeNode node, long key) throws ExecutionException, InterruptedException {
        int idx = node.findKey(degree, key);
        boolean result;

        if (idx < node.getKeyList(degree).size() && node.getKeyList(degree).get(idx) == key){
            if (node.isLeaf()){
                ((LeafTreeNode) node).removeKeyValue(idx);
                TreeNodeIO.update(indexStorageManager, table, node);
                result = true;
            } else {
                this.deleteInternalNode(table, (InternalTreeNode) node, key, idx);
                result = true;
            }
        } else {
            if (node.isLeaf()){
                return false;
            }

            boolean flag = (idx == node.getKeyList(degree).size());

            InternalTreeNode internalTreeNode = (InternalTreeNode) node;

            BaseTreeNode leftChildAtIDX = TreeNodeIO.read(indexStorageManager, table, internalTreeNode.getChildrenList(degree).get(idx));
            if (leftChildAtIDX.getKeyList(degree).size() < minKeys){
                this.fill(table, (InternalTreeNode) node, idx);
            }

            BaseTreeNode nextChild;
            if (flag && idx > node.getKeyList(degree).size()){
                nextChild = TreeNodeIO.read(indexStorageManager, table, internalTreeNode.getChildrenList(degree).get(idx - 1));
            } else {
                nextChild = TreeNodeIO.read(indexStorageManager, table, internalTreeNode.getChildrenList(degree).get(idx));
            }
            result = deleteRecursive(table, nextChild, key);
        }
        return result;
    }

    private void deleteInternalNode(int table, InternalTreeNode node, long key, int idx) throws ExecutionException, InterruptedException {
        List<Pointer> childPointersList = node.getChildrenList(degree);
        BaseTreeNode leftIDXChild = TreeNodeIO.read(indexStorageManager, degree, childPointersList.get(idx));
        if (leftIDXChild.getKeyList(degree).size() >= minKeys){
            long pred = this.getPredecessor(table, node, idx);
            node.setKey(idx, pred);
            deleteRecursive(table, leftIDXChild, pred);
        } else {
            BaseTreeNode leftNextIDXChild = TreeNodeIO.read(indexStorageManager, degree, childPointersList.get(idx + 1));
            if (leftNextIDXChild.getKeyList(degree).size() >= minKeys) {
                long succ = getSuccessor(table, node, idx);
                node.setKey(idx, succ);
                deleteRecursive(table, leftNextIDXChild, succ);
            } else {
                merge(table, node, idx);
                deleteRecursive(table, leftIDXChild, key);
            }
        }

    }

    private long getPredecessor(int table, InternalTreeNode node, int idx) throws ExecutionException, InterruptedException {
        BaseTreeNode cur = TreeNodeIO.read(indexStorageManager, table, node.getChildPointersList(degree).get(idx).getLeft());
        while (!cur.isLeaf()) {
            cur = TreeNodeIO.read(indexStorageManager, table, node.getChildPointersList(degree).get(cur.getKeyList(degree).size() - 1).getLeft());
        }
        return cur.getKeyList(degree).get(cur.getKeyList(degree).size() - 1);
    }

    private long getSuccessor(int table, InternalTreeNode node, int idx) throws ExecutionException, InterruptedException {
        BaseTreeNode cur = TreeNodeIO.read(indexStorageManager, table, node.getChildrenList(degree).get(idx + 1));
        while (!cur.isLeaf()) {
            cur = TreeNodeIO.read(indexStorageManager, table, node.getChildrenList(degree).get(0));
        }
        return cur.getKeyList(degree).get(0);
    }


    /**
     * Fills the child node at idx if it has less than minDegree keys.
     * @param node The parent node.
     * @param idx The index of the child node to fill.
     */
    private void fill(int table, InternalTreeNode node, int idx) throws ExecutionException, InterruptedException {
        InternalTreeNode treeNode = (InternalTreeNode) TreeNodeIO.read(indexStorageManager, table, node.getChildrenList(degree).get(idx - 1));
        if (idx != 0 && treeNode.getKeyList(degree).size() >= minKeys) {
            this.borrowFromPrev(table, node, idx);
        } else {
            InternalTreeNode treeNode1 = (InternalTreeNode) TreeNodeIO.read(indexStorageManager, table, node.getChildrenList(degree).get(idx + 1));
            if (idx != treeNode.getKeyList(degree).size() && treeNode1.getKeyList(degree).size() >= minKeys) {
                this.borrowFromNext(table, node, idx);
            } else {
                if (idx != node.getKeyList(degree).size()) {
                    merge(table, node, idx);
                } else {
                    merge(table, node, idx - 1);
                }
            }
        }
    }

    /**
     * Borrows a key from the previous sibling and moves it to the child at idx.
     * @param node The parent node.
     * @param idx The index of the child node to borrow for.
     */
    private void borrowFromPrev(int table, InternalTreeNode node, int idx) throws ExecutionException, InterruptedException {
        BaseTreeNode child = TreeNodeIO.read(indexStorageManager, table, node.getChildrenList(degree).get(idx));
        BaseTreeNode sibling = TreeNodeIO.read(indexStorageManager, table, node.getChildrenList(degree).get(idx - 1));

        if (!child.isLeaf()){
            InternalTreeNode siblingInternalNode = (InternalTreeNode) sibling;
            List<InternalTreeNode.ChildPointers> childPointersList = new ArrayList<>(siblingInternalNode.getChildPointersList(degree));
            InternalTreeNode.ChildPointers lastChildPointer = childPointersList.removeLast();
            siblingInternalNode.setChildPointers(childPointersList, degree, true);

            long currKey = node.getKeyList(degree).get(idx);
            node.setKey(idx, lastChildPointer.getKey());

            InternalTreeNode childInternalNode = (InternalTreeNode) child;
            ArrayList<InternalTreeNode.ChildPointers> childPointersList2 = new ArrayList<>(childInternalNode.getChildPointersList(degree));
            lastChildPointer.setRight(childPointersList2.getFirst().getLeft());
            lastChildPointer.setKey(currKey);
            lastChildPointer.setLeft(lastChildPointer.getRight());
            childPointersList2.add(lastChildPointer);
            childInternalNode.setChildPointers(childPointersList2, degree, false);   // todo: probably no need to clean? it was short

            // Todo: save them
        } else {
            LeafTreeNode siblingLeafNode = (LeafTreeNode) sibling;
            LeafTreeNode childLeafNode = (LeafTreeNode) sibling;

            List<LeafTreeNode.KeyValue> keyValueList = new ArrayList<>(siblingLeafNode.getKeyValueList(degree));
            LeafTreeNode.KeyValue keyValue = keyValueList.removeLast();
            siblingLeafNode.setKeyValues(keyValueList, degree);

            long currKey = node.getKeyList(degree).get(idx);
            node.setKey(idx, keyValue.key());

            childLeafNode.addKeyValue(currKey, keyValue.value(), degree);

            // Todo: save them
        }
    }

    private void borrowFromNext(int table, InternalTreeNode node, int idx) throws ExecutionException, InterruptedException {
        BaseTreeNode child = TreeNodeIO.read(indexStorageManager, table, node.getChildrenList(degree).get(idx));
        BaseTreeNode sibling = TreeNodeIO.read(indexStorageManager, table, node.getChildrenList(degree).get(idx + 1));

        if (!child.isLeaf()){
            InternalTreeNode siblingInternalNode = (InternalTreeNode) sibling;
            List<InternalTreeNode.ChildPointers> childPointersList = new ArrayList<>(siblingInternalNode.getChildPointersList(degree));
            InternalTreeNode.ChildPointers firstChildPointer = childPointersList.removeFirst();
            siblingInternalNode.setChildPointers(childPointersList, degree, true);

            long currKey = node.getKeyList(degree).get(idx);
            node.setKey(idx, firstChildPointer.getKey());

            InternalTreeNode childInternalNode = (InternalTreeNode) child;
            ArrayList<InternalTreeNode.ChildPointers> childPointersList2 = new ArrayList<>(childInternalNode.getChildPointersList(degree));
            firstChildPointer.setRight(firstChildPointer.getLeft());
            firstChildPointer.setKey(currKey);
            firstChildPointer.setLeft(childPointersList2.getLast().getRight());
            childPointersList2.add(firstChildPointer);
            childInternalNode.setChildPointers(childPointersList2, degree, false);   // todo: probably no need to clean? it was short

        } else {
            LeafTreeNode siblingLeafNode = (LeafTreeNode) sibling;
            LeafTreeNode childLeafNode = (LeafTreeNode) sibling;

            List<LeafTreeNode.KeyValue> keyValueList = new ArrayList<>(siblingLeafNode.getKeyValueList(degree));
            LeafTreeNode.KeyValue keyValue = keyValueList.removeFirst();
            siblingLeafNode.setKeyValues(keyValueList, degree);

            long currKey = node.getKeyList(degree).get(idx);
            node.setKey(idx, keyValue.key());

            childLeafNode.addKeyValue(currKey, keyValue.value(), degree);

        }

        TreeNodeIO.update(indexStorageManager, table, node, sibling, child);
    }

    /**
     * Merges the child node at idx with its next sibling.
     * @param node The parent node.
     * @param idx The index of the child node to merge.
     */
    private void merge(int table, InternalTreeNode node, int idx) throws ExecutionException, InterruptedException {
        BaseTreeNode child = TreeNodeIO.read(indexStorageManager, table, node.getChildrenList(degree).get(idx));
        BaseTreeNode sibling = TreeNodeIO.read(indexStorageManager, table, node.getChildrenList(degree).get(idx + 1));

        if (!child.isLeaf()){
            InternalTreeNode childInternalTreeNode = (InternalTreeNode) child;
            List<Long> childKeyList = new ArrayList<>(childInternalTreeNode.getKeyList(degree));
            childKeyList.addAll(sibling.getKeyList(degree));
            childKeyList.sort(Long::compareTo);
            childInternalTreeNode.setKeys(childKeyList);

            ArrayList<Pointer> childPointers = new ArrayList<>(childInternalTreeNode.getChildrenList(degree));
            childPointers.addAll(((InternalTreeNode) sibling).getChildrenList(degree));
            childInternalTreeNode.setChildren(childPointers);

            node.removeKey(idx);
            node.removeChild(idx + 1);

        } else {
            LeafTreeNode childLeafTreeNode = (LeafTreeNode) child;
            ArrayList<LeafTreeNode.KeyValue> keyValueList = new ArrayList<>(childLeafTreeNode.getKeyValueList(degree));
            keyValueList.addAll(((LeafTreeNode) sibling).getKeyValueList(degree));
            Collections.sort(keyValueList);

            node.removeKey(idx);
            node.removeChild(idx + 1);

        }

        TreeNodeIO.update(indexStorageManager, table, node, child);
        TreeNodeIO.remove(indexStorageManager, table, node);

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
