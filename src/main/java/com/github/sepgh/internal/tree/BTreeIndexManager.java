package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.exception.ChunkIsFullException;
import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;
import com.github.sepgh.internal.tree.exception.IllegalNodeAccess;
import com.github.sepgh.internal.tree.node.BaseTreeNode;
import com.github.sepgh.internal.tree.node.InternalTreeNode;
import com.github.sepgh.internal.tree.node.LeafTreeNode;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class BTreeIndexManager implements IndexManager {

    private final IndexStorageManager indexStorageManager;
    private final HeaderManager headerManager;
    private final int table;
    private final int order;
    private BaseTreeNode root;

    public BTreeIndexManager(int table, int order, HeaderManager headerManager, IndexStorageManager indexStorageManager){
        this.table = table;
        this.order = order;
        this.indexStorageManager = indexStorageManager;
        this.headerManager = headerManager;
        initialize();
    }

    @SneakyThrows
    private void initialize(){
        Optional<Pointer> optionalRootPointer = this.indexStorageManager.getRoot(table);
        if (optionalRootPointer.isEmpty())
            return;

        Pointer pointer = optionalRootPointer.get();
        Future<byte[]> node = indexStorageManager.readNode(table, pointer);
        byte[] bytes = node.get();
        this.root = BaseTreeNode.fromBytes(bytes);
        this.root.setNodePointer(pointer);
    }

    // Todo: the current way we maintain root is not good. If data is manipulated and location of root is changed (allocation happened for previous table)
    //       then we no longer have updated root
    // Todo: as a result, whenever allocation happens we should update possible roots that are messed up in the header
    //       also, we should always read root position fresh from header

    @Override
    public CompletableFuture<BaseTreeNode> addIndex(long identifier, Pointer pointer) throws ExecutionException, InterruptedException, IllegalNodeAccess {

        if (this.root == null){
            return this.generateRoot(identifier, pointer);
        }

        CompletableFuture<BaseTreeNode> output = new CompletableFuture<>();

        List<BaseTreeNode> path = new LinkedList<>();
        getPathToResponsibleNode(path, root, identifier);


        /* variables to fill and use in while */
        long idForParentToStore = identifier;
        BaseTreeNode newChildForParent = null;


        for (int i = 0; i < path.size(); i++){
            BaseTreeNode currentNode = path.getLast();

            if (i == 0){
                /* current node is a leaf which should handle storing the data */

                /* If current node has space, store and exit */
                if (currentNode.keyList().size() < order){
                    ((LeafTreeNode) currentNode).addKeyValue(identifier, pointer);
                    indexStorageManager.writeNode(table, currentNode.toBytes(), currentNode.getNodePointer()).whenComplete((integer, throwable) -> {
                        if (throwable != null) {
                            output.completeExceptionally(throwable);
                        }
                        output.complete(currentNode);
                    });

                    return output;
                }

                /* Current node didn't have any space, so let's create a sibling and split */
                IndexStorageManager.AllocationResult allocationResult = this.allocateNodeAtAnyExistingChunk(table).get();
                BaseTreeNode newTreeNode = BaseTreeNode.fromBytes(
                        indexStorageManager.readNode(table, allocationResult.position(), allocationResult.chunk()).get()
                );
                newTreeNode.setType(BaseTreeNode.NodeType.LEAF);
                LeafTreeNode newLeafTreeNode = (LeafTreeNode) newTreeNode;

                List<Map.Entry<Long, Pointer>> movingKeyValues = this.splitKeyValues((LeafTreeNode) currentNode, identifier, pointer);
                for (int z = 0; z < movingKeyValues.size(); z++){
                    Map.Entry<Long, Pointer> entry = movingKeyValues.get(z);
                    newLeafTreeNode.setKeyValue(z, entry.getKey(), entry.getValue());
                }


                fixSiblingPointers((LeafTreeNode) currentNode, newLeafTreeNode);

                /* this leaf doesn't have a parent! create one and deal with it right here! */
                if (path.size() == 1) {
                    allocationResult = this.allocateNodeAtAnyExistingChunk(table).get();
                    BaseTreeNode newParent = BaseTreeNode.fromBytes(
                            indexStorageManager.readNode(table, allocationResult.position(), allocationResult.chunk()).get()
                    );
                    currentNode.unsetAsRoot();
                    newParent.setAsRoot();
                    InternalTreeNode parentInternalTreeNode = (InternalTreeNode) newParent;
                    parentInternalTreeNode.addKey(movingKeyValues.get(0).getKey());
                    parentInternalTreeNode.setChildAtIndex(0, currentNode.getNodePointer());
                    parentInternalTreeNode.setChildAtIndex(1, newLeafTreeNode.getNodePointer());
                    indexStorageManager.writeNode(table, parentInternalTreeNode.toBytes(), newTreeNode.getNodePointer()).whenComplete((integer, throwable) -> {
                        if (throwable != null){
                            output.completeExceptionally(throwable);
                        }
                    });
                }

                indexStorageManager.writeNode(table, currentNode.toBytes(), currentNode.getNodePointer()).whenComplete((integer, throwable) -> {
                    if (throwable != null){
                        output.completeExceptionally(throwable);
                    }
                    indexStorageManager.writeNode(table, newLeafTreeNode.toBytes(), currentNode.getNodePointer()).whenComplete((integer1, throwable1) -> {
                        if (throwable1 != null){
                            output.completeExceptionally(throwable);
                        }
                    });
                });

                // We are done already
                if (path.size() == 1)
                    return output;

                newChildForParent = newLeafTreeNode;
                idForParentToStore = movingKeyValues.get(0).getKey();
            } else {

                /* current node is an internal node */
                InternalTreeNode currentInternalTreeNode = (InternalTreeNode) currentNode;
                List<Long> keys = currentInternalTreeNode.keyList();

                if (keys.size() < order){
                    /* current internal node can store the key */
                    int indexOfNewKey = currentInternalTreeNode.addKey(idForParentToStore);

                    if (indexOfNewKey == 0){
                        currentInternalTreeNode.setChildAtIndex(0, newChildForParent.getNodePointer());
                    } else {
                        currentInternalTreeNode.setChildAtIndex(indexOfNewKey + 1, newChildForParent.getNodePointer());
                    }
                    indexStorageManager.writeNode(table, currentNode.toBytes(), currentNode.getNodePointer()).whenComplete((integer, throwable) -> {
                        if (throwable != null){
                            output.completeExceptionally(throwable);
                        }
                        output.complete(path.getFirst());
                    });
                } else {
                    /* current internal node cant store the key */
                    List<Map.Entry<Long, Pointer>> splitChildren = this.splitChildren(
                            (InternalTreeNode) currentNode,
                            idForParentToStore,
                            newChildForParent.getNodePointer()
                    );
                    IndexStorageManager.AllocationResult allocationResult = this.allocateNodeAtAnyExistingChunk(table).get();
                    InternalTreeNode newSiblingInternalNode = (InternalTreeNode) BaseTreeNode.fromAllocationResult(allocationResult, BaseTreeNode.NodeType.INTERNAL);

                    idForParentToStore = splitChildren.get(0).getKey();
                    List<Map.Entry<Long, Pointer>> forSibling = splitChildren.subList(1, splitChildren.size() - 1);

                    for (Map.Entry<Long, Pointer> longPointerEntry : forSibling) {
                        int i1 = newSiblingInternalNode.addKey(longPointerEntry.getKey());
                        if (i1 == 0){
                            newSiblingInternalNode.setChildAtIndex(0, longPointerEntry.getValue());
                        } else {
                            newSiblingInternalNode.setChildAtIndex(i1 + 1, longPointerEntry.getValue());
                        }
                    }

                    indexStorageManager.writeNode(table, currentNode.toBytes(), currentNode.getNodePointer()).whenComplete((integer, throwable) -> {
                        if (throwable != null){
                            output.completeExceptionally(throwable);
                        }
                        indexStorageManager.writeNode(table, newSiblingInternalNode.toBytes(), currentNode.getNodePointer()).whenComplete((integer1, throwable1) -> {
                            if (throwable1 != null){
                                output.completeExceptionally(throwable);
                            }
                        });
                    });

                    // Current node was root and needs a new parent
                    if (i == path.size() - 1){
                        allocationResult = this.allocateNodeAtAnyExistingChunk(table).get();
                        InternalTreeNode newParentInternalNode = (InternalTreeNode) BaseTreeNode.fromAllocationResult(allocationResult, BaseTreeNode.NodeType.INTERNAL);
                        newParentInternalNode.setAsRoot();
                        currentNode.unsetAsRoot();

                        newParentInternalNode.addKey(idForParentToStore);
                        newParentInternalNode.setChildAtIndex(0, currentNode.getNodePointer());
                        newParentInternalNode.setChildAtIndex(1, newSiblingInternalNode.getNodePointer());

                        indexStorageManager.writeNode(table, currentNode.toBytes(), currentNode.getNodePointer()).whenComplete((integer, throwable) -> {
                            if (throwable != null){
                                output.completeExceptionally(throwable);
                            }
                            indexStorageManager.writeNode(table, newParentInternalNode.toBytes(), currentNode.getNodePointer()).whenComplete((integer1, throwable1) -> {
                                if (throwable1 != null){
                                    output.completeExceptionally(throwable);
                                }
                            });
                        });
                        return output;
                    } else {
                        // Tell parent what new node to store
                        newChildForParent = currentNode;
                    }

                }

            }
        }


        return output;
    }

    private List<Map.Entry<Long, Pointer>> splitChildren(InternalTreeNode node, long idForParentToStore, Pointer nodePointer) {
        List<Map.Entry<Long, Pointer>> entries = new LinkedList<>();

        int mid = (int) Math.round((double) order / 2);
        List<Pointer> childrenList = node.childrenList().reversed();
        List<Long> keyList = node.keyList().reversed();

        for (int i = 0; i < mid; i++){
            int childIndex = childrenList.size() - i - 1;

            Pointer child = childrenList.get(childIndex);
            long key = keyList.get(childIndex - 1);
            entries.add(new AbstractMap.SimpleEntry<>(key, child));

            node.removeChildAtIndex(childIndex);
            node.removeKeyAtIndex(childIndex - 1);
        }

        entries.add(new AbstractMap.SimpleEntry<>(idForParentToStore, nodePointer));
        entries.sort(Comparator.comparingLong(Map.Entry::getKey));

        return entries;
    }

    /**
     * Splits node key values and returns a list of key values which should be moved.
     * The new list is sorted and includes new id and pointer
     */
    private List<Map.Entry<Long, Pointer>> splitKeyValues(LeafTreeNode node, long identifier, Pointer pointer) {
        List<Map.Entry<Long, Pointer>> entries = new LinkedList<>();

        int mid = (int) Math.round((double) order / 2);

        List<Map.Entry<Long, Pointer>> keyValues = node.keyValueList();

        for (int i = 0; i < mid; i++){
            int index = keyValues.size() - i - 1;

            Map.Entry<Long, Pointer> keyValueEntry = keyValues.get(index);
            entries.add(keyValueEntry);
            node.removeKeyValueAtIndex(index);
        }

        entries.add(new AbstractMap.SimpleEntry<>(identifier, pointer));
        entries.sort(Comparator.comparingLong(Map.Entry::getKey));
        return entries;
    }

    @Override
    public CompletableFuture<Optional<BaseTreeNode>> getIndex(long identifier) {
        return null;
    }

    @Override
    public CompletableFuture<Void> removeIndex(long identifier) {
        return null;
    }

    private void fixSiblingPointers(LeafTreeNode currentNode, LeafTreeNode newLeafTreeNode) {
        currentNode.setNext(newLeafTreeNode.getNodePointer());
        newLeafTreeNode.setPrevious(currentNode.getNodePointer());
    }

    private void getPathToResponsibleNode(List<BaseTreeNode> path, BaseTreeNode node, long identifier) throws ExecutionException, InterruptedException {
        path.addFirst(node);

        if (node.isLeaf()){
            return;
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
                byte[] bytes = indexStorageManager.readNode(table, pointer.get()).get();
                BaseTreeNode baseTreeNode = BaseTreeNode.fromBytes(bytes);
                baseTreeNode.setNodePointer(pointer.get());
                getPathToResponsibleNode(path, baseTreeNode, identifier);
            }

            i++;
        }

    }

    private CompletableFuture<IndexStorageManager.AllocationResult> allocateNodeAtAnyExistingChunk(int table) {

        List<Header.IndexChunk> chunks = this.headerManager.getHeader().getTableOfId(table).get().getChunks();
        CompletableFuture<IndexStorageManager.AllocationResult> output = new CompletableFuture<>();

        for (int i = 0; i < chunks.size(); i++){
            CompletableFuture<IndexStorageManager.AllocationResult> allocationFuture = null;

            try {
                allocationFuture = this.indexStorageManager.allocateForNewNode(this.table, chunks.get(i).getChunk());
            } catch (IOException e) {
                output.completeExceptionally(e);
                return output;
            } catch (ChunkIsFullException e) {
                continue;
            }

            allocationFuture.whenComplete((allocationResult, throwable) -> {
                if (throwable != null){
                    output.completeExceptionally(throwable);
                    return;
                }

                output.complete(allocationResult);

            });
            return output;
        }

        // Todo: make new chunk and try again?
        return null;
    }

    private CompletableFuture<BaseTreeNode> generateRoot(long identifier, Pointer pointer) {
        CompletableFuture<BaseTreeNode> answer = new CompletableFuture<>();

        this.allocateNodeAtAnyExistingChunk(table).whenComplete((allocationResult, throwable) -> {
            if (throwable != null){
                answer.completeExceptionally(throwable);
            }

            LeafTreeNode treeNode = new LeafTreeNode(new byte[allocationResult.size()]);
            treeNode.setType(BaseTreeNode.NodeType.LEAF);
            treeNode.setAsRoot();
            try {
                treeNode.setKeyValue(0, identifier, pointer);
            } catch (IllegalNodeAccess e) {
                answer.completeExceptionally(e);
            }

            indexStorageManager.writeNode(table, treeNode.toBytes(), allocationResult.position(), allocationResult.chunk()).whenComplete((integer, throwable1) -> {
                if (throwable1 != null){
                    answer.completeExceptionally(throwable1);
                    return;
                }
                headerManager.getHeader().getTableOfId(this.table).get().setRoot(
                        Header.IndexChunk.builder()
                                .offset(pointer.position())
                                .chunk(pointer.chunk())
                                .build()
                );
                root = treeNode;  // Todo: check above long todo about this not being the best way to do about root
                answer.complete(treeNode);
            });
        });

        return answer;
    }
}
