package com.github.sepgh.testudo.index.tree;

import com.github.sepgh.testudo.ds.KVSize;
import com.github.sepgh.testudo.ds.KeyValue;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.IndexMissingException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.AbstractUniqueTreeIndexManager;
import com.github.sepgh.testudo.index.UniqueQueryableIndex;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.InternalTreeNode;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.testudo.operation.query.Operation;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.session.ImmediateCommitIndexIOSession;
import com.github.sepgh.testudo.storage.index.session.IndexIOSession;
import com.github.sepgh.testudo.storage.index.session.IndexIOSessionFactory;
import com.github.sepgh.testudo.utils.IteratorUtils;
import com.github.sepgh.testudo.utils.LockableIterator;
import com.google.common.base.Preconditions;
import lombok.SneakyThrows;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class BPlusTreeUniqueTreeIndexManager<K extends Comparable<K>, V> extends AbstractUniqueTreeIndexManager<K, V> implements UniqueQueryableIndex<K, V> {
    private final IndexStorageManager indexStorageManager;
    private final IndexIOSessionFactory indexIOSessionFactory;
    private final int degree;
    protected final IndexBinaryObjectFactory<K> keyIndexBinaryObjectFactory;
    private final IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory;
    private final NodeFactory<K> nodeFactory;
    protected final KVSize kvSize;
    public static final int PURGE_ITERATION_MULTIPLIER = 2;  // Todo: the `2` here is just an example. Make it configurable?

    public BPlusTreeUniqueTreeIndexManager(int index, int degree, IndexStorageManager indexStorageManager, IndexIOSessionFactory indexIOSessionFactory, IndexBinaryObjectFactory<K> keyIndexBinaryObjectFactory, IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory, NodeFactory<K> nodeFactory) {
        super(index);
        this.degree = degree;
        this.indexStorageManager = indexStorageManager;
        this.indexIOSessionFactory = indexIOSessionFactory;
        this.keyIndexBinaryObjectFactory = keyIndexBinaryObjectFactory;
        this.valueIndexBinaryObjectFactory = valueIndexBinaryObjectFactory;
        this.nodeFactory = nodeFactory;
        this.kvSize = new KVSize(
                keyIndexBinaryObjectFactory.size(),
                valueIndexBinaryObjectFactory.size()
        );
    }

    public BPlusTreeUniqueTreeIndexManager(int index, int degree, IndexStorageManager indexStorageManager, IndexBinaryObjectFactory<K> keyIndexBinaryObjectFactory, IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory, NodeFactory<K> nodeFactory){
        this(index, degree, indexStorageManager, ImmediateCommitIndexIOSession.Factory.getInstance(), keyIndexBinaryObjectFactory, valueIndexBinaryObjectFactory, nodeFactory);
    }

    public BPlusTreeUniqueTreeIndexManager(int index, int degree, IndexStorageManager indexStorageManager, IndexIOSessionFactory indexIOSessionFactory, IndexBinaryObjectFactory<K> keyIndexBinaryObjectFactory, IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory){
        this(index, degree, indexStorageManager, indexIOSessionFactory, keyIndexBinaryObjectFactory, valueIndexBinaryObjectFactory, new NodeFactory.DefaultNodeFactory<>(keyIndexBinaryObjectFactory, valueIndexBinaryObjectFactory));

    }

    public BPlusTreeUniqueTreeIndexManager(int index, int degree, IndexStorageManager indexStorageManager, IndexBinaryObjectFactory<K> keyIndexBinaryObjectFactory, IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory){
        this(index, degree, indexStorageManager, ImmediateCommitIndexIOSession.Factory.getInstance(), keyIndexBinaryObjectFactory, valueIndexBinaryObjectFactory);
    }

    @Override
    public AbstractTreeNode<K> addIndex(K identifier, V value) throws InternalOperationException, IndexExistsException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, indexId, nodeFactory, kvSize);
        AbstractTreeNode<K> root = getRoot(indexIOSession);
        return new BPlusTreeIndexCreateOperation<>(degree, indexIOSession, keyIndexBinaryObjectFactory, valueIndexBinaryObjectFactory, this.kvSize).addIndex(root, identifier, value);
    }

    @Override
    public AbstractTreeNode<K> updateIndex(K identifier, V value) throws InternalOperationException, IndexMissingException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, indexId, nodeFactory, kvSize);

        AbstractLeafTreeNode<K, V> node = BPlusTreeUtils.getResponsibleNode(indexStorageManager, getRoot(indexIOSession), identifier, indexId, degree, nodeFactory);
        List<K> keyList = node.getKeyList(degree);
        if (!keyList.contains(identifier)) {
            throw new IndexMissingException();
        }

        node.setKeyValue(keyList.indexOf(identifier), new KeyValue<>(identifier, value));
        indexIOSession.update(node);
        indexIOSession.commit();
        return node;
    }

    @Override  // Todo: use BinarySearch
    public Optional<V> getIndex(K identifier) throws InternalOperationException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, indexId, nodeFactory, kvSize);
        AbstractLeafTreeNode<K, V> baseTreeNode = BPlusTreeUtils.getResponsibleNode(indexStorageManager, getRoot(indexIOSession), identifier, indexId, degree, nodeFactory);
        for (KeyValue<K, V> entry : baseTreeNode.getKeyValueList(degree)) {
            if (entry.key().equals(identifier)){
                return Optional.of(entry.value());
            }
        }

        return Optional.empty();
    }

    @Override
    public boolean removeIndex(K identifier) throws InternalOperationException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, indexId, nodeFactory, kvSize);
        AbstractTreeNode<K> root = getRoot(indexIOSession);
        return new BPlusTreeIndexDeleteOperation<>(degree, indexId, indexIOSession, valueIndexBinaryObjectFactory, nodeFactory).removeIndex(root, identifier);
    }

    @Override
    public int size() throws InternalOperationException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, indexId, nodeFactory, kvSize);

        Optional<IndexStorageManager.NodeData> optionalNodeData = null;
        try {
            optionalNodeData = this.indexStorageManager.getRoot(indexId, kvSize).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new InternalOperationException(e);
        }

        if (optionalNodeData.isEmpty())
            return 0;

        AbstractTreeNode<K> root = nodeFactory.fromNodeData(optionalNodeData.get());
        if (root.isLeaf()){
            return root.getKeyList(degree, valueIndexBinaryObjectFactory.size()).size();
        }

        AbstractTreeNode<K> curr = root;
        while (!curr.isLeaf()) {
            curr = indexIOSession.read(((InternalTreeNode<K>) curr).getChildrenList().getFirst());
        }

        int size = curr.getKeyList(degree, valueIndexBinaryObjectFactory.size()).size();
        Optional<Pointer> optionalNext = ((LeafClusterTreeNode<K>) curr).getNextSiblingPointer(degree);
        while (optionalNext.isPresent()){
            AbstractTreeNode<K> nextNode = indexIOSession.read(optionalNext.get());
            size += nextNode.getKeyList(degree, valueIndexBinaryObjectFactory.size()).size();
            optionalNext = ((LeafClusterTreeNode<K>) nextNode).getNextSiblingPointer(degree);
        }

        return size;
    }

    @Override
    public LockableIterator<KeyValue<K, V>> getSortedIterator(Order order) throws InternalOperationException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, indexId, nodeFactory, kvSize);

        Iterator<KeyValue<K,V>> iterator = switch (order) {
            case DESC -> BPlusTreeUtils.getDescendingIterator(indexIOSession, getRoot(indexIOSession), degree);
            case ASC -> BPlusTreeUtils.getAscendingIterator(indexIOSession, getRoot(indexIOSession), degree);
        };

        return new LockableIterator<>() {
            @Override
            public void lock() {

            }

            @Override
            public void unlock() {

            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public KeyValue<K, V> next() {
                return iterator.next();
            }
        };
    }

    @Override
    public synchronized void purgeIndex() {
        if (this.indexStorageManager.supportsPurge()) {
            this.indexStorageManager.purgeIndex(indexId);
        }

        boolean removed;
        int maxPerIteration = this.degree * PURGE_ITERATION_MULTIPLIER;

        do {
            try {
                LockableIterator<KeyValue<K, V>> sortedIterator = getSortedIterator(Order.DEFAULT);
                List<K> toRemove = new ArrayList<>();
                int i = 0;

                try {
                    sortedIterator.lock();
                    while (sortedIterator.hasNext() && i < maxPerIteration){
                        KeyValue<K, V> next = sortedIterator.next();
                        toRemove.add(next.key());
                        i++;
                    }
                } finally {
                    sortedIterator.unlock();
                }

                removed = !toRemove.isEmpty();

                for (K k : toRemove) {
                    this.removeIndex(k);
                }

            } catch (InternalOperationException e) {
                throw new RuntimeException(e);  // Todo
            }
        } while (removed);

    }

    @Override
    public boolean supportIncrement() {
        return Number.class.isAssignableFrom(keyIndexBinaryObjectFactory.getType());
    }

    // Todo: this could be done better:
    // - What if we reach max?
    // - IndexBinaryObjects can provide "T first()" and "T next(T current)" methods
    @Override
    public K nextKey() throws InternalOperationException {
        if (supportIncrement()) {
            Iterator<K> sortedKeyIterator = this.getSortedKeyIterator(Order.DESC);
            if (sortedKeyIterator.hasNext()) {
                return keyIndexBinaryObjectFactory.create(sortedKeyIterator.next()).getNext();
            } else {
                return keyIndexBinaryObjectFactory.createEmpty().asObject();
            }
        }
        throw new UnsupportedOperationException("nextKey not supported");
    }

    public AbstractTreeNode<K> getRoot(IndexIOSession<K> indexIOSession) throws InternalOperationException {
        Optional<AbstractTreeNode<K>> optionalRoot = indexIOSession.getRoot();
        if (optionalRoot.isPresent()){
            return optionalRoot.get();
        }

        byte[] emptyNode = indexStorageManager.getEmptyNode(kvSize);
        AbstractLeafTreeNode<K, ?> leafTreeNode = (AbstractLeafTreeNode<K, ?>) nodeFactory.fromBytes(emptyNode, AbstractTreeNode.Type.LEAF);
        leafTreeNode.setAsRoot();

        IndexStorageManager.NodeData nodeData = indexIOSession.write(leafTreeNode);
        leafTreeNode.setPointer(nodeData.pointer());
        return leafTreeNode;
    }

    @Override
    public Iterator<KeyValue<K, V>> getSortedKeyValueIterator(Order order) throws InternalOperationException {
        return getSortedIterator(order);
    }

    @Override
    public Iterator<V> getGreaterThan(K identifier, Order order) throws InternalOperationException {
        return new QueryIterator(order, Operation.GT, identifier);
    }

    @Override
    public Iterator<V> getGreaterThanEqual(K identifier, Order order) throws InternalOperationException {
        return new QueryIterator(order, Operation.GTE, identifier);
    }

    @Override
    public Iterator<V> getLessThan(K identifier, Order order) throws InternalOperationException {
        return new QueryIterator(order, Operation.LT, identifier);
    }

    @Override
    public Iterator<V> getLessThanEqual(K identifier, Order order) throws InternalOperationException {
        return new QueryIterator(order, Operation.LTE, identifier);
    }

    @Override
    public Iterator<V> getEqual(K k, Order order) throws InternalOperationException {
        Optional<V> index = getIndex(k);
        if (index.isPresent()){
            return new Iterator<>() {
                boolean hasNext = true;

                @Override
                public boolean hasNext() {
                    return hasNext;
                }

                @Override
                public V next() {
                    hasNext = false;
                    return index.get();
                }
            };
        }
        return IteratorUtils.getCleanIterator();
    }

    @Override
    public Iterator<V> getNotEqual(K k, Order order) throws InternalOperationException {
        return IteratorUtils.getNotEqualIterator(
                getSortedKeyValueIterator(order),
                k
        );
    }

    @Override
    public Iterator<V> getNulls(Order order) {
        return this.getNullIndexes(order);
    }


    private class QueryIterator implements Iterator<V> {
        private final Order order;
        private final IndexIOSession<K> indexIOSession;
        private final Operation operation;
        private final K identifier;
        private final Function<K, Boolean> ltf;
        private final Function<K, Boolean> ltef;
        private final Function<K, Boolean> gtf;
        private final Function<K, Boolean> gtef;
        private final Map<Operation, Function<K, Boolean>> operationFunctionMap = new HashMap<>();

        private int index = -1;
        private AbstractLeafTreeNode<K, V> leafTreeNode;
        private List<KeyValue<K, V>> keyValueList;

        private QueryIterator(Order order, Operation operation, K identifier) throws InternalOperationException {
            this.order = order;
            Preconditions.checkArgument(
                    operation == Operation.LT || operation == Operation.LTE
                    || operation == Operation.GT || operation == Operation.GTE,
                    "Only number based operations are supported"
            );
            this.operation = operation;
            this.identifier = identifier;
            this.indexIOSession = indexIOSessionFactory.create(indexStorageManager, indexId, nodeFactory, kvSize);

            this.ltf = (k) -> k.compareTo(identifier) < 0;
            this.ltef = (k) -> k.compareTo(identifier) <= 0;
            this.gtf = (k) -> k.compareTo(identifier) > 0;
            this.gtef = (k) -> k.compareTo(identifier) >= 0;

            operationFunctionMap.put(Operation.LT, ltf);
            operationFunctionMap.put(Operation.LTE, ltef);
            operationFunctionMap.put(Operation.GT, gtf);
            operationFunctionMap.put(Operation.GTE, gtef);

            init();
        }

        private void init() throws InternalOperationException {

            if (order == Order.DESC) {

                // DESC - LT || LTE
                if (operation == Operation.LT || operation == Operation.LTE) {

                    leafTreeNode = BPlusTreeUtils.getResponsibleNode(indexStorageManager, getRoot(indexIOSession), identifier, indexId, degree, nodeFactory);
                    keyValueList = leafTreeNode.getKeyValueList(degree);

                    if (keyValueList.isEmpty()){
                        index = -1;
                        return;
                    }

                    if (operation == Operation.LT && keyValueList.getFirst().key().compareTo(identifier) >= 0 && leafTreeNode.getPreviousSiblingPointer(degree).isPresent()){
                        leafTreeNode = (AbstractLeafTreeNode<K, V>) indexIOSession.read(leafTreeNode.getPreviousSiblingPointer(degree).get());
                        keyValueList = leafTreeNode.getKeyValueList(degree);
                    }

                    // Todo: binary search?
                    for (int i = keyValueList.size() - 1; i >= 0; i--) {
                        if (
                            operationFunctionMap.get(operation).apply(keyValueList.get(i).key())
                        ) {
                            index = i;
                            break;
                        }
                    }
                } else {

                    leafTreeNode = BPlusTreeUtils.getFarRightLeaf(indexIOSession, getRoot(indexIOSession));
                    keyValueList = leafTreeNode.getKeyValueList(degree);

                    // DESC - GT || GTE
                    // Todo: binary search?
                    for (int i = keyValueList.size() - 1; i >= 0; i--) {
                        if (
                            operationFunctionMap.get(operation).apply(keyValueList.get(i).key())
                        ) {
                            index = i;
                            break;
                        }
                    }
                }
            } else {

                //  ASC - GT || GTE
                if (operation == Operation.GTE || operation == Operation.GT) {

                    leafTreeNode = BPlusTreeUtils.getResponsibleNode(indexStorageManager, getRoot(indexIOSession), identifier, indexId, degree, nodeFactory);
                    keyValueList = leafTreeNode.getKeyValueList(degree);

                    if (keyValueList.isEmpty()){
                        index = -1;
                        return;
                    }

                    if (operation == Operation.GT && keyValueList.getLast().key().compareTo(identifier) <= 0 && leafTreeNode.getNextSiblingPointer(degree).isPresent()){
                        leafTreeNode = (AbstractLeafTreeNode<K, V>) indexIOSession.read(leafTreeNode.getNextSiblingPointer(degree).get());
                        keyValueList = leafTreeNode.getKeyValueList(degree);
                    }

                    // Todo: binary search?
                    for (int i = 0; i < keyValueList.size(); i++) {
                        if (
                            operationFunctionMap.get(operation).apply(keyValueList.get(i).key())
                        ) {
                            index = i;
                            break;
                        }
                    }
                } else {

                    leafTreeNode = BPlusTreeUtils.getFarLeftLeaf(indexIOSession, getRoot(indexIOSession));
                    keyValueList = leafTreeNode.getKeyValueList(degree);

                    for (int i = 0; i < keyValueList.size(); i++) {
                        if (
                            operationFunctionMap.get(operation).apply(keyValueList.get(i).key())
                        ) {
                            index = i;
                            break;
                        }
                    }
                }
            }
        }

        @Override
        @SneakyThrows
        public boolean hasNext() {
            if (order == Order.DESC) {
                if (index == -1) {
                    Optional<Pointer> siblingPointer = leafTreeNode.getPreviousSiblingPointer(degree);

                    if (siblingPointer.isEmpty())
                        return false;

                    leafTreeNode = (AbstractLeafTreeNode<K, V>) indexIOSession.read(siblingPointer.get());
                    keyValueList = leafTreeNode.getKeyValueList(degree);
                    index = keyValueList.size() - 1;
                }
            } else {
                if (index == -1)
                    return false;

                if (index == keyValueList.size()) {
                    Optional<Pointer> nextSiblingPointerOptional = leafTreeNode.getNextSiblingPointer(degree);
                    if (nextSiblingPointerOptional.isEmpty())
                        return false;
                    leafTreeNode = (AbstractLeafTreeNode<K, V>) indexIOSession.read(nextSiblingPointerOptional.get());
                    keyValueList = leafTreeNode.getKeyValueList(degree);
                    index = 0;
                }
            }
            return operationFunctionMap.get(operation).apply(keyValueList.get(index).key());
        }

        @Override
        public V next() {
            V result = keyValueList.get(index).value();
            if (order == Order.DESC) {
                index--;
            } else {
                index++;
            }
            return result;
        }
    }

}
