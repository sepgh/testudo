package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.tree.node.BaseTreeNode;
import com.github.sepgh.internal.tree.node.InternalTreeNode;
import com.github.sepgh.internal.tree.node.LeafTreeNode;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class BTreeIndexManager implements IndexManager {

    private final IndexStorageManager indexStorageManager;
    private final int order;

    public BTreeIndexManager(int order, IndexStorageManager indexStorageManager){
        this.order = order;
        this.indexStorageManager = indexStorageManager;
    }

    private BaseTreeNode getRoot(int table) throws ExecutionException, InterruptedException, IOException {
        CompletableFuture<Optional<IndexStorageManager.NodeData>> completableFuture = indexStorageManager.getRoot(table);
        Optional<IndexStorageManager.NodeData> optionalNodeData = completableFuture.get();
        if (optionalNodeData.isPresent()){
            BaseTreeNode root = BaseTreeNode.fromBytes(optionalNodeData.get().bytes());
            root.setNodePointer(optionalNodeData.get().pointer());
            return root;
        }

        byte[] emptyNode = indexStorageManager.getEmptyNode();
        LeafTreeNode leafTreeNode = (LeafTreeNode) BaseTreeNode.fromBytes(emptyNode, BaseTreeNode.NodeType.LEAF);
        leafTreeNode.setAsRoot();

        IndexStorageManager.NodeData nodeData = indexStorageManager.fillRoot(table, leafTreeNode.getData()).get();
        leafTreeNode.setNodePointer(nodeData.pointer());
        return leafTreeNode;
    }

    @Override
    public BaseTreeNode addIndex(int table, long identifier, Pointer pointer) throws ExecutionException, InterruptedException, IOException {

        BaseTreeNode root = getRoot(table);

        List<BaseTreeNode> path = new LinkedList<>();
        getPathToResponsibleNode(table, path, root, identifier);


        /* variables to fill and use in while */
        long idForParentToStore = identifier;
        BaseTreeNode newChildForParent = null;
        BaseTreeNode answer = null;

        for (int i = 0; i < path.size(); i++){
            BaseTreeNode currentNode = path.get(i);

            if (i == 0){
                /* current node is a leaf which should handle storing the data */

                /* If current node has space, store and exit */
                if (currentNode.keyList().size() < order){
                    ((LeafTreeNode) currentNode).addKeyValue(identifier, pointer);
                    indexStorageManager.updateNode(table, currentNode.toBytes(), currentNode.getNodePointer()).get();
                    return currentNode;
                }

                /* Current node didn't have any space, so let's create a sibling and split */
                byte[] emptyNode = indexStorageManager.getEmptyNode();
                LeafTreeNode newLeafTreeNode = (LeafTreeNode) BaseTreeNode.fromBytes(emptyNode, BaseTreeNode.NodeType.LEAF);

                List<Map.Entry<Long, Pointer>> movingKeyValues = this.splitKeyValues((LeafTreeNode) currentNode, identifier, pointer);
                for (int z = 0; z < movingKeyValues.size(); z++){
                    Map.Entry<Long, Pointer> entry = movingKeyValues.get(z);
                    newLeafTreeNode.setKeyValue(z, entry.getKey(), entry.getValue());
                }


                // Todo: fixing siblings could potentially be faster. We could write new node, and "onComplete" update current node
                IndexStorageManager.NodeData newLeafNodeData = indexStorageManager.writeNewNode(table, newLeafTreeNode.toBytes()).get();
                newLeafTreeNode.setNodePointer(newLeafNodeData.pointer());
                fixSiblingPointers(table, (LeafTreeNode) currentNode, newLeafTreeNode);
                indexStorageManager.updateNode(table, currentNode.getData(), currentNode.getNodePointer()).get();
                indexStorageManager.updateNode(table, newLeafTreeNode.getData(), newLeafTreeNode.getNodePointer()).get();

                /* this leaf doesn't have a parent! create one and deal with it right here! */
                if (path.size() == 1) {
                    InternalTreeNode parentInternalTreeNode = (InternalTreeNode) BaseTreeNode.fromBytes(
                            indexStorageManager.getEmptyNode(),
                            BaseTreeNode.NodeType.INTERNAL
                    );
                    currentNode.unsetAsRoot();
                    parentInternalTreeNode.setAsRoot();
                    parentInternalTreeNode.addKey(movingKeyValues.get(0).getKey());
                    parentInternalTreeNode.setChildAtIndex(0, currentNode.getNodePointer());
                    parentInternalTreeNode.setChildAtIndex(1, newLeafTreeNode.getNodePointer());
                    indexStorageManager.writeNewNode(table, parentInternalTreeNode.toBytes(), true).get();
                    indexStorageManager.updateNode(table, currentNode.getData(), currentNode.getNodePointer());
                    return newLeafTreeNode;
                }

                answer = newLeafTreeNode;
                newChildForParent = newLeafTreeNode;
                idForParentToStore = movingKeyValues.get(0).getKey();
            } else {
                /* current node is an internal node */
                InternalTreeNode currentInternalTreeNode = (InternalTreeNode) currentNode;
                List<Long> keys = currentInternalTreeNode.keyList();

                if (keys.size() < order){
                    /* current internal node can store the key */
                    int indexOfNewKey = currentInternalTreeNode.addKey(idForParentToStore);
                    Long firstKeyInChild = newChildForParent.keys().next();
                    if (firstKeyInChild < idForParentToStore){
                        currentInternalTreeNode.setChildAtIndex(indexOfNewKey, newChildForParent.getNodePointer());
                    } else {
                        currentInternalTreeNode.setChildAtIndex(indexOfNewKey + 1, newChildForParent.getNodePointer());
                    }
                    indexStorageManager.updateNode(table, currentNode.getData(), currentNode.getNodePointer()).get();
                    return answer;
                } else {
                    /* current internal node cant store the key */
                    ChildSplitResults childSplitResults = this.splitChildren(
                            (InternalTreeNode) currentNode,
                            idForParentToStore,
                            newChildForParent.getNodePointer()
                    );

                    InternalTreeNode newSiblingInternalNode = (InternalTreeNode) BaseTreeNode.fromBytes(
                            indexStorageManager.getEmptyNode(),
                            BaseTreeNode.NodeType.INTERNAL
                    );


                    List<Pointer> newNodeChildren = childSplitResults.children();
                    for (int z = 0; z < newNodeChildren.size(); z++) {
                        newSiblingInternalNode.setChildAtIndex(z, newNodeChildren.get(z));
                    }
                    List<Long> newNodeKeys = childSplitResults.keys();
                    for (int z = 0; z < newNodeKeys.size(); z++) {
                        newSiblingInternalNode.setKeyAtIndex(z, newNodeKeys.get(z));
                    }

                    indexStorageManager.updateNode(table, currentNode.toBytes(), currentNode.getNodePointer()).get();
                    IndexStorageManager.NodeData nodeData = indexStorageManager.writeNewNode(table, newSiblingInternalNode.toBytes()).get();
                    newSiblingInternalNode.setNodePointer(nodeData.pointer());

                    newChildForParent = newSiblingInternalNode;
                    idForParentToStore = childSplitResults.idForParent();


                    // Current node was root and needs a new parent
                    if (i == path.size() - 1){
                        InternalTreeNode newParentInternalNode = (InternalTreeNode) BaseTreeNode.fromBytes(
                                indexStorageManager.getEmptyNode(),
                                BaseTreeNode.NodeType.INTERNAL
                        );
                        newParentInternalNode.setAsRoot();
                        currentNode.unsetAsRoot();

                        newParentInternalNode.addKey(idForParentToStore);
                        newParentInternalNode.setChildAtIndex(0, currentNode.getNodePointer());
                        newParentInternalNode.setChildAtIndex(1, newSiblingInternalNode.getNodePointer());

                        indexStorageManager.updateNode(table, currentNode.getData(), currentNode.getNodePointer()).get();
                        indexStorageManager.writeNewNode(table, newParentInternalNode.toBytes(), true).get();

                        return answer;
                    }

                }

            }
        }


        throw new RuntimeException("Logic error: probably failed to store index?");
    }

    private ChildSplitResults splitChildren(InternalTreeNode node, long identifier, Pointer pointer) {

        int mid = order / 2;

        List<Long> nodeKeyList = node.keyList();
        List<Long> allKeys = new ArrayList<>(nodeKeyList);
        allKeys.add(identifier);
        allKeys.sort(Long::compareTo);


        int indexOfNewKey = allKeys.indexOf(identifier);

        List<Pointer> nodeChildrenList = node.childrenList();
        List<Pointer> allChildren = new ArrayList<>(nodeChildrenList);


        if (identifier < allKeys.get(indexOfNewKey))
            allChildren.add(indexOfNewKey, pointer);
        else
            allChildren.add(indexOfNewKey + 1, pointer);


        List<Long> keysToKeep = allKeys.subList(0, mid + 1);
        List<Long> keysToPass = allKeys.subList(mid + 1, allKeys.size());

        List<Pointer> childrenToKeep = allChildren.subList(0, mid + 2);
        List<Pointer> childrenToPass = allChildren.subList(mid + 2, allChildren.size());


        for (int i = 0; i < nodeKeyList.size(); i++){
            if (i < keysToKeep.size()){
                node.setKeyAtIndex(i, keysToKeep.get(i));
            } else {
                node.removeKeyAtIndex(i);
            }
        }

        for (int i = 0; i < nodeChildrenList.size(); i++){
            if (i < childrenToKeep.size()){
                node.setChildAtIndex(i, childrenToKeep.get(i));
            } else {
                node.removeChildAtIndex(i);
            }
        }

        return new ChildSplitResults(keysToPass.get(0), keysToPass.subList(1, keysToPass.size()), childrenToPass);
    }

    /**
     * Splits node key values and returns a list of key values which should be moved.
     * The new list is sorted and includes new id and pointer
     */
    private List<Map.Entry<Long, Pointer>> splitKeyValues(LeafTreeNode node, long identifier, Pointer pointer) {

        int mid = order / 2;

        List<Map.Entry<Long, Pointer>> keyValues = new ArrayList<>(node.keyValueList());
        keyValues.add(new AbstractMap.SimpleEntry<>(identifier, pointer));
        keyValues.sort(Comparator.comparingLong(Map.Entry::getKey));

        List<Map.Entry<Long, Pointer>> keep = keyValues.subList(0, mid + 1);
        List<Map.Entry<Long, Pointer>> pass = keyValues.subList(mid + 1, keyValues.size());

        for (int i = mid; i < order; i++){
            node.removeKeyValueAtIndex(i);
        }
        for (int i = 0; i < keep.size(); i++){
            Map.Entry<Long, Pointer> entry = keep.get(i);
            node.setKeyValue(i, entry.getKey(), entry.getValue());
        }

        return pass;
    }

    @Override
    public Optional<Pointer> getIndex(int table, long identifier) throws IOException, ExecutionException, InterruptedException {
        Optional<LeafTreeNode> optionalBaseTreeNode = this.getResponsibleNode(table, getRoot(table), identifier);
        if (optionalBaseTreeNode.isPresent()){
            for (Map.Entry<Long, Pointer> entry : optionalBaseTreeNode.get().keyValueList()) {
                if (entry.getKey() == identifier)
                    return Optional.of(entry.getValue());
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean removeIndex(int table, long identifier) {
        return false;
    }

    private void fixSiblingPointers(int table, LeafTreeNode currentNode, LeafTreeNode newLeafTreeNode) throws ExecutionException, InterruptedException {
        Optional<Pointer> optionalCurrentNext = currentNode.getNext();
        currentNode.setNext(newLeafTreeNode.getNodePointer());
        newLeafTreeNode.setPrevious(currentNode.getNodePointer());
        if (optionalCurrentNext.isPresent()){
            newLeafTreeNode.setNext(optionalCurrentNext.get());
            IndexStorageManager.NodeData nodeData = indexStorageManager.readNode(table, optionalCurrentNext.get()).get();
            BaseTreeNode baseTreeNode = BaseTreeNode.fromBytes(nodeData.bytes());
            ((LeafTreeNode) baseTreeNode).setPrevious(newLeafTreeNode.getNodePointer());
            indexStorageManager.updateNode(table, baseTreeNode.getData(), nodeData.pointer()).get();
        }
    }

    private Optional<LeafTreeNode> getResponsibleNode(int table, BaseTreeNode node, long identifier) throws ExecutionException, InterruptedException {
        if (node.isLeaf()){
            return Optional.of((LeafTreeNode) node);
        }

        InternalTreeNode internalTreeNode = (InternalTreeNode) node;
        Iterator<Long> keys = internalTreeNode.keys();
        int i = 0;
        while(keys.hasNext()){
            long key = keys.next();
            Optional<Pointer> pointer;
            if (i == 0 && identifier < key){
                pointer = internalTreeNode.getChildAtIndex(0);
            }else if (identifier >= key) {
                pointer = internalTreeNode.getChildAtIndex(i + 1);
            } else {
                pointer = Optional.empty();
            }

            if (pointer.isPresent()){
                IndexStorageManager.NodeData nodeData = indexStorageManager.readNode(table, pointer.get()).get();
                BaseTreeNode baseTreeNode = BaseTreeNode.fromBytes(nodeData.bytes());
                baseTreeNode.setNodePointer(nodeData.pointer());
                return getResponsibleNode(table, baseTreeNode, identifier);
            }

            i++;
        }
        return Optional.empty();
    }

    private void getPathToResponsibleNode(int table, List<BaseTreeNode> path, BaseTreeNode node, long identifier) throws ExecutionException, InterruptedException {
        path.addFirst(node);

        if (node.isLeaf()){
            return;
        }

        InternalTreeNode internalTreeNode = (InternalTreeNode) node;
        List<Long> keyList = internalTreeNode.keyList();

        int childIndex = keyList.size();
        for (long l : keyList.reversed()) {
            if (l < identifier)
                break;
            childIndex--;
        }
        Pointer pointer = internalTreeNode.getChildAtIndex(childIndex).get();
        IndexStorageManager.NodeData nodeData = indexStorageManager.readNode(table, pointer).get();
        BaseTreeNode baseTreeNode = BaseTreeNode.fromBytes(nodeData.bytes());
        baseTreeNode.setNodePointer(nodeData.pointer());
        getPathToResponsibleNode(table, path, baseTreeNode, identifier);
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
