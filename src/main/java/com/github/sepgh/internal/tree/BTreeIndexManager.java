package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.tree.node.BaseTreeNode;
import com.github.sepgh.internal.tree.node.InternalTreeNode;
import com.github.sepgh.internal.tree.node.LeafTreeNode;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class BTreeIndexManager implements IndexManager {

    private final IndexStorageManager indexStorageManager;
    private final int degree;

    public BTreeIndexManager(int degree, IndexStorageManager indexStorageManager){
        this.degree = degree;
        this.indexStorageManager = indexStorageManager;
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
                    newRoot.addKeyPointers(
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
                    currentInternalTreeNode.addKeyPointers(idForParentToStore, null, newChildForParent.getPointer(), degree, false);
                    TreeNodeIO.write(currentInternalTreeNode, indexStorageManager, table).get();
                    return answer;
                }


                /* current internal node cant store the key, split and ask parent */
                List<InternalTreeNode.KeyPointers> passingKeyPointers = currentInternalTreeNode.addAndSplit(idForParentToStore, newChildForParent.getPointer(), degree);
                InternalTreeNode.KeyPointers firstPassingKeyPointers = passingKeyPointers.getFirst();
                idForParentToStore = firstPassingKeyPointers.getKey();
                passingKeyPointers.remove(0);

                InternalTreeNode newInternalSibling = new InternalTreeNode(indexStorageManager.getEmptyNode());
                newInternalSibling.setKeyPointers(passingKeyPointers, degree, true);
                TreeNodeIO.write(newInternalSibling, indexStorageManager, table).get();


                // Current node was root and needs a new parent
                if (currentInternalTreeNode.isRoot()){
                    currentInternalTreeNode.unsetAsRoot();
                    InternalTreeNode newRoot = new InternalTreeNode(indexStorageManager.getEmptyNode());
                    newRoot.setAsRoot();

                    newRoot.addKeyPointers(
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
        return false;
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
        List<InternalTreeNode.KeyPointers> keyPointersList = ((InternalTreeNode) node).getKeyPointersList(degree);
        for (int i = 0; i < keyPointersList.size(); i++){
            InternalTreeNode.KeyPointers keyPointers = keyPointersList.get(i);
            if (keyPointers.getKey() > identifier && keyPointers.getLeft() != null){
                return getResponsibleNode(
                        TreeNodeIO.read(indexStorageManager, table, keyPointers.getLeft()),
                        identifier,
                        table
                );
            }
            if (i == keyPointersList.size() - 1 && keyPointers.getRight() != null){
                return getResponsibleNode(
                        TreeNodeIO.read(indexStorageManager, table, keyPointers.getRight()),
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

        List<InternalTreeNode.KeyPointers> keyPointersList = ((InternalTreeNode) node).getKeyPointersList(degree);
        for (int i = 0; i < keyPointersList.size(); i++){
            InternalTreeNode.KeyPointers keyPointers = keyPointersList.get(i);
            if (keyPointers.getKey() > identifier && keyPointers.getLeft() != null){
                path.addFirst(node);
                getPathToResponsibleNode(
                        path,
                        TreeNodeIO.read(indexStorageManager, table, keyPointers.getLeft()),
                        identifier,
                        table
                );
                return;
            }
            if (i == keyPointersList.size() - 1 && keyPointers.getRight() != null){
                path.addFirst(node);
                getPathToResponsibleNode(
                        path,
                        TreeNodeIO.read(indexStorageManager, table, keyPointers.getRight()),
                        identifier,
                        table
                );
                return;
            }
        }
    }


    private record ChildSplitResults(long idForParent, List<Long> keys, List<Pointer> children){

        @Override
        public String toString() {
            return "ChildSplitResults{" +
                    "idForParent=" + idForParent +
                    ", keys=" + keys +
                    ", children=" + children +
                    '}';
        }
    }

}
