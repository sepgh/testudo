package com.github.sepgh.internal.index.tree.node.cluster;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.TreeNodeUtils;
import com.github.sepgh.internal.index.tree.node.data.PointerInnerObject;
import com.github.sepgh.internal.utils.CollectionUtils;
import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;

import java.util.*;

public class LeafClusterTreeNode<K extends Comparable<K>> extends BaseClusterTreeNode<K> {
    public LeafClusterTreeNode(byte[] data, ClusterIdentifier.Strategy<K> strategy) {
        super(data, strategy);
        setType(Type.LEAF);
    }

    public void setNextSiblingPointer(Pointer pointer, int degree){
        modified();
        TreeNodeUtils.setNextPointer(this, degree, pointer, clusterIdentifierStrategy.size(), PointerInnerObject.BYTES);
    }

    public void setPreviousSiblingPointer(Pointer pointer, int degree){
        modified();
        TreeNodeUtils.setPreviousPointer(this, degree, pointer, clusterIdentifierStrategy.size(), PointerInnerObject.BYTES);
    }

    public Optional<Pointer> getPreviousSiblingPointer(int degree){
        return TreeNodeUtils.getPreviousPointer(this, degree, clusterIdentifierStrategy.size(), PointerInnerObject.BYTES);
    }

    public Optional<Pointer> getNextSiblingPointer(int degree){
        return TreeNodeUtils.getNextPointer(this, degree, clusterIdentifierStrategy.size(), PointerInnerObject.BYTES);
    }

    public Iterator<KeyValue<K>> getKeyValues(int degree){
        return new KeyValueIterator(this, degree);
    }

    public List<KeyValue<K>> getKeyValueList(int degree) {
        return ImmutableList.copyOf(getKeyValues(degree));
    }

    public void setKeyValues(List<KeyValue<K>> keyValueList, int degree){
        modified();
        for (int i = 0; i < keyValueList.size(); i++){
            KeyValue<K> keyValue = keyValueList.get(i);
            this.setKeyValue(i, keyValue);
        }
        for (int i = keyValueList.size(); i < (degree - 1); i++){
            TreeNodeUtils.removeKeyValueAtIndex(this, i, clusterIdentifierStrategy.size(), PointerInnerObject.BYTES);
        }
    }

    public void setKeyValue(int index, KeyValue<K> keyValue){
        TreeNodeUtils.setKeyValueAtIndex(this, index, clusterIdentifierStrategy.fromObject(keyValue.key()), new PointerInnerObject(keyValue.value()));
    }

    public List<KeyValue<K>> split(K identifier, Pointer pointer, int degree){
        int mid = (degree - 1) / 2;

        List<KeyValue<K>> allKeyValues = new ArrayList<>(getKeyValueList(degree));
        KeyValue<K> keyValue = new KeyValue<>(identifier, pointer);
        int i = CollectionUtils.indexToInsert(allKeyValues, keyValue);
        allKeyValues.add(i, keyValue);

        List<KeyValue<K>> toKeep = allKeyValues.subList(0, mid + 1);
        this.setKeyValues(toKeep, degree);
        return allKeyValues.subList(mid + 1, allKeyValues.size());
    }

    @SneakyThrows
    public int addKeyValue(K identifier, Pointer pointer, int degree) {
        return TreeNodeUtils.addKeyValueAndGetIndex(this, degree, clusterIdentifierStrategy.getNodeInnerObjClass(), identifier, clusterIdentifierStrategy.size(), PointerInnerObject.class, pointer, PointerInnerObject.BYTES);
    }

    public int addKeyValue(KeyValue<K> keyValue, int degree) {
        return this.addKeyValue(keyValue.key, keyValue.value, degree);
    }

    public void removeKeyValue(int index) {
        TreeNodeUtils.removeKeyValueAtIndex(this, index, clusterIdentifierStrategy.size(), PointerInnerObject.BYTES);
    }

    public boolean removeKeyValue(K key, int degree) {
        List<KeyValue<K>> keyValueList = new ArrayList<KeyValue<K>>(this.getKeyValueList(degree));
        boolean removed = keyValueList.removeIf(keyValue -> keyValue.key.compareTo(key) == 0);
        setKeyValues(keyValueList, degree);
        return removed;
    }


    public static class KeyValue<K extends Comparable<K>> implements Comparable<KeyValue<K>> {
        private final K key;
        private final Pointer value;

        public KeyValue(K key, Pointer value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int compareTo(KeyValue<K> o) {
            return this.key.compareTo(o.key);
        }

        public K key(){
            return key;
        }

        public Pointer value(){
            return value;
        }
    }

    private class KeyValueIterator implements Iterator<KeyValue<K>>{
        private int cursor = 0;

        private final LeafClusterTreeNode<K> node;
        private final int degree;

        public KeyValueIterator(LeafClusterTreeNode<K> node, int degree) {
            this.node = node;
            this.degree = degree;
        }

        @SneakyThrows
        @Override
        public boolean hasNext() {
            return TreeNodeUtils.hasKeyAtIndex(node, cursor, degree, clusterIdentifierStrategy.getNodeInnerObjClass(), clusterIdentifierStrategy.size(), PointerInnerObject.BYTES);
        }

        @SneakyThrows
        @Override
        public KeyValue<K> next() {
            Map.Entry<K, Pointer> keyValuePointerAtIndex = TreeNodeUtils.getKeyValuePointerAtIndex(
                    node,
                    cursor,
                    clusterIdentifierStrategy.getNodeInnerObjClass(),
                    clusterIdentifierStrategy.size(),
                    PointerInnerObject.class,
                    PointerInnerObject.BYTES
            );
            cursor++;
            return new KeyValue<>(keyValuePointerAtIndex.getKey(), keyValuePointerAtIndex.getValue());
        }
    }

}
