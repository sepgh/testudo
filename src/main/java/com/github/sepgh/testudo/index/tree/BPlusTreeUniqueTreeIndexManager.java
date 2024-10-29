package com.github.sepgh.testudo.index.tree;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.IndexMissingException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.*;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.InternalTreeNode;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.session.ImmediateCommitIndexIOSession;
import com.github.sepgh.testudo.storage.index.session.IndexIOSession;
import com.github.sepgh.testudo.storage.index.session.IndexIOSessionFactory;
import com.github.sepgh.testudo.utils.KVSize;
import com.github.sepgh.testudo.utils.LockableIterator;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class BPlusTreeUniqueTreeIndexManager<K extends Comparable<K>, V> extends AbstractUniqueTreeIndexManager<K, V> implements UniqueQueryableIndex<K, V> {
    private final IndexStorageManager indexStorageManager;
    private final IndexIOSessionFactory indexIOSessionFactory;
    private final int degree;
    private final IndexBinaryObjectFactory<K> keyIndexBinaryObjectFactory;
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
    public LockableIterator<KeyValue<K, V>> getSortedIterator() throws InternalOperationException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, indexId, nodeFactory, kvSize);

        return new LockableIterator<>() {
            @Override
            public void lock() {
            }

            @Override
            public void unlock() {
            }

            private int keyIndex = 0;
            AbstractLeafTreeNode<K, V> currentLeaf = getFarLeftLeaf();

            @Override
            public boolean hasNext() {
                int size = currentLeaf.getKeyList(degree).size();
                if (keyIndex == size)
                    return currentLeaf.getNextSiblingPointer(degree).isPresent();
                return true;
            }

            @SneakyThrows
            @Override
            public KeyValue<K, V> next() {
                List<KeyValue<K, V>> keyValueList = currentLeaf.getKeyValueList(degree);

                if (keyIndex == keyValueList.size()){
                    currentLeaf = (AbstractLeafTreeNode<K, V>) indexIOSession.read(currentLeaf.getNextSiblingPointer(degree).get());
                    keyIndex = 0;
                    keyValueList = currentLeaf.getKeyValueList(degree);
                }

                KeyValue<K, V> output = keyValueList.get(keyIndex);
                keyIndex += 1;
                return output;
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
                LockableIterator<KeyValue<K, V>> sortedIterator = getSortedIterator();
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

    protected AbstractLeafTreeNode<K, V> getFarLeftLeaf() throws InternalOperationException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, indexId, nodeFactory, kvSize);
        AbstractTreeNode<K> root = getRoot(indexIOSession);
        if (root.isLeaf())
            return (AbstractLeafTreeNode<K, V>) root;

        AbstractTreeNode<K> farLeftChild = root;

        while (!farLeftChild.isLeaf()){
            farLeftChild = indexIOSession.read(((InternalTreeNode<K>) farLeftChild).getChildAtIndex(0));
        }

        return (AbstractLeafTreeNode<K, V>) farLeftChild;
    }

    private AbstractTreeNode<K> getRoot(IndexIOSession<K> indexIOSession) throws InternalOperationException {
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

    private Iterator<V> getGreaterThanIterator(K identifier, boolean supportEQ) throws InternalOperationException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, indexId, nodeFactory, kvSize);

        AbstractLeafTreeNode<K, V> leafTreeNode = BPlusTreeUtils.getResponsibleNode(indexStorageManager, getRoot(indexIOSession), identifier, indexId, degree, nodeFactory);
        List<KeyValue<K, V>> keyValueList = leafTreeNode.getKeyValueList(degree);

        if (keyValueList.getLast().key().compareTo(identifier) <= 0 && !supportEQ && leafTreeNode.getNextSiblingPointer(degree).isPresent()){
            leafTreeNode = (AbstractLeafTreeNode<K, V>) indexIOSession.read(leafTreeNode.getNextSiblingPointer(degree).get());
            keyValueList = leafTreeNode.getKeyValueList(degree);
        }

        final AtomicReference<AbstractLeafTreeNode<K, V>> leafReference = new AtomicReference<>(leafTreeNode);
        final AtomicReference<List<KeyValue<K, V>>> keyValueListReference = new AtomicReference<>(keyValueList);

        AtomicInteger index = new AtomicInteger(-1);

        // Todo: binary search?
        for (int i = 0; i < keyValueList.size(); i++) {
            if (
                    supportEQ && keyValueList.get(i).key().compareTo(identifier) >= 0
                    ||
                    !supportEQ && keyValueList.get(i).key().compareTo(identifier) > 0
            ) {
                index.set(i);
                break;
            }
        }

        return new Iterator<>() {
            @SneakyThrows
            @Override
            public boolean hasNext() {
                if (index.get() == -1)
                    return false;

                if (index.get() == keyValueListReference.get().size()) {
                    Optional<Pointer> nextSiblingPointerOptional = leafReference.get().getNextSiblingPointer(degree);
                    if (nextSiblingPointerOptional.isEmpty())
                        return false;
                    leafReference.set((AbstractLeafTreeNode<K, V>) indexIOSession.read(nextSiblingPointerOptional.get()));
                    keyValueListReference.set(leafReference.get().getKeyValueList(degree));
                    index.set(0);
                }

                return true;
            }

            @Override
            public V next() {
                int i = index.getAndIncrement();
                return keyValueListReference.get().get(i).value();
            }
        };
    }

    private Iterator<V> getLessThanIterator(K identifier, boolean supportEQ) throws InternalOperationException {
        IndexIOSession<K> indexIOSession = this.indexIOSessionFactory.create(indexStorageManager, indexId, nodeFactory, kvSize);

        AbstractLeafTreeNode<K, V> leafTreeNode = BPlusTreeUtils.getResponsibleNode(indexStorageManager, getRoot(indexIOSession), identifier, indexId, degree, nodeFactory);
        List<KeyValue<K, V>> keyValueList = leafTreeNode.getKeyValueList(degree);

        if (keyValueList.getFirst().key().compareTo(identifier) >= 0 && !supportEQ && leafTreeNode.getPreviousSiblingPointer(degree).isPresent()){
            leafTreeNode = (AbstractLeafTreeNode<K, V>) indexIOSession.read(leafTreeNode.getPreviousSiblingPointer(degree).get());
            keyValueList = leafTreeNode.getKeyValueList(degree);
        }

        final AtomicReference<AbstractLeafTreeNode<K, V>> leafReference = new AtomicReference<>(leafTreeNode);
        final AtomicReference<List<KeyValue<K, V>>> keyValueListReference = new AtomicReference<>(keyValueList);

        AtomicInteger index = new AtomicInteger(-1);

        // Todo: binary search?
        for (int i = keyValueList.size() - 1; i >= 0; i--) {
            if (
                    supportEQ && keyValueList.get(i).key().compareTo(identifier) <= 0
                    ||
                    !supportEQ && keyValueList.get(i).key().compareTo(identifier) < 0
            ) {
                index.set(i);
                break;
            }
        }

        return new Iterator<>() {
            @SneakyThrows
            @Override
            public boolean hasNext() {
                if (index.get() == -1) {
                    Optional<Pointer> siblingPointer = leafReference.get().getPreviousSiblingPointer(degree);
                    if (siblingPointer.isEmpty())
                        return false;
                    leafReference.set((AbstractLeafTreeNode<K, V>) indexIOSession.read(siblingPointer.get()));
                    List<KeyValue<K, V>> keyValueList1 = leafReference.get().getKeyValueList(degree);
                    keyValueListReference.set(keyValueList1);
                    index.set(keyValueList1.size() - 1);
                }
                return true;
            }

            @Override
            public V next() {
                int i = index.getAndDecrement();
                return keyValueListReference.get().get(i).value();
            }
        };
    }

    @Override
    public Iterator<V> getGreaterThan(K identifier) throws InternalOperationException {
        return getGreaterThanIterator(identifier, false);
    }

    @Override
    public Iterator<V> getGreaterThanEqual(K identifier) throws InternalOperationException {
        return getGreaterThanIterator(identifier, true);
    }

    @Override
    public Iterator<V> getLessThan(K k) throws InternalOperationException {
        return getLessThanIterator(k, false);
    }

    @Override
    public Iterator<V> getLessThanEqual(K k) throws InternalOperationException {
        return getLessThanIterator(k, true);
    }

    @Override
    public Optional<Iterator<V>> getEqual(K k) throws InternalOperationException {
        Optional<V> index = getIndex(k);
        return index.map(v -> new Iterator<>() {
            boolean hasNext = true;

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public V next() {
                hasNext = false;
                return v;
            }
        });
    }
}
