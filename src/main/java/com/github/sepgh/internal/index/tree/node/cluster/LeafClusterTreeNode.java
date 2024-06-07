package com.github.sepgh.internal.index.tree.node.cluster;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.TreeNodeUtils;
import com.github.sepgh.internal.index.tree.node.data.Identifier;
import com.github.sepgh.internal.index.tree.node.data.PointerInnerObject;
import com.github.sepgh.internal.utils.CollectionUtils;
import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;

import java.util.*;

public class LeafClusterTreeNode extends BaseClusterTreeNode {
    public LeafClusterTreeNode(byte[] data) {
        super(data);
        setType(Type.LEAF);
    }

    public void setNextSiblingPointer(Pointer pointer, int degree){
        modified();
        TreeNodeUtils.setNextPointer(this, degree, pointer, Identifier.BYTES, PointerInnerObject.BYTES);
    }

    public void setPreviousSiblingPointer(Pointer pointer, int degree){
        modified();
        TreeNodeUtils.setPreviousPointer(this, degree, pointer, Identifier.BYTES, PointerInnerObject.BYTES);
    }

    public Optional<Pointer> getPreviousSiblingPointer(int degree){
        return TreeNodeUtils.getPreviousPointer(this, degree, Identifier.BYTES, PointerInnerObject.BYTES);
    }

    public Optional<Pointer> getNextSiblingPointer(int degree){
        return TreeNodeUtils.getNextPointer(this, degree, Identifier.BYTES, PointerInnerObject.BYTES);
    }

    public Iterator<KeyValue> getKeyValues(int degree){
        return new KeyValueIterator(this, degree);
    }

    public List<KeyValue> getKeyValueList(int degree) {
        return ImmutableList.copyOf(getKeyValues(degree));
    }

    public void setKeyValues(List<KeyValue> keyValueList, int degree){
        modified();
        for (int i = 0; i < keyValueList.size(); i++){
            KeyValue keyValue = keyValueList.get(i);
            this.setKeyValue(i, keyValue);
        }
        for (int i = keyValueList.size(); i < (degree - 1); i++){
            TreeNodeUtils.removeKeyValueAtIndex(this, i, Identifier.BYTES, PointerInnerObject.BYTES);
        }
    }

    public void setKeyValue(int index, KeyValue keyValue){
        TreeNodeUtils.setKeyValueAtIndex(this, index, new Identifier(keyValue.key()), new PointerInnerObject(keyValue.value()));
    }

    public List<KeyValue> split(long identifier, Pointer pointer, int degree){
        int mid = (degree - 1) / 2;

        List<KeyValue> allKeyValues = new ArrayList<>(getKeyValueList(degree));
        KeyValue keyValue = new KeyValue(identifier, pointer);
        int i = CollectionUtils.indexToInsert(allKeyValues, keyValue);
        allKeyValues.add(i, keyValue);

        List<KeyValue> toKeep = allKeyValues.subList(0, mid + 1);
        this.setKeyValues(toKeep, degree);
        return allKeyValues.subList(mid + 1, allKeyValues.size());
    }

    @SneakyThrows
    public int addKeyValue(long identifier, Pointer pointer, int degree) {
        return TreeNodeUtils.addKeyValueAndGetIndex(this, degree, Identifier.class, identifier, Identifier.BYTES, PointerInnerObject.class, pointer, PointerInnerObject.BYTES);
    }

    public int addKeyValue(KeyValue keyValue, int degree) {
        return this.addKeyValue(keyValue.key, keyValue.value, degree);
    }

    public void removeKeyValue(int index) {
        TreeNodeUtils.removeKeyValueAtIndex(this, index, Identifier.BYTES, PointerInnerObject.BYTES);
    }

    public boolean removeKeyValue(long key, int degree) {
        List<KeyValue> keyValueList = new ArrayList<>(this.getKeyValueList(degree));
        boolean removed = keyValueList.removeIf(keyValue -> keyValue.key == key);
        setKeyValues(keyValueList, degree);
        return removed;
    }


    public record KeyValue(long key, Pointer value) implements Comparable<KeyValue> {

        @Override
        public int compareTo(KeyValue o) {
            return Long.compare(this.key, o.key);
        }
    }

    public class KeyValueIterator implements Iterator<KeyValue>{
        private int cursor = 0;

        private final LeafClusterTreeNode node;
        private final int degree;

        public KeyValueIterator(LeafClusterTreeNode node, int degree) {
            this.node = node;
            this.degree = degree;
        }

        @SneakyThrows
        @Override
        public boolean hasNext() {
            return TreeNodeUtils.hasKeyAtIndex(node, cursor, degree, Identifier.class, Identifier.BYTES, PointerInnerObject.BYTES);
        }

        @SneakyThrows
        @Override
        public KeyValue next() {
            Map.Entry<Long, Pointer> keyValuePointerAtIndex = TreeNodeUtils.getKeyValuePointerAtIndex(
                    node,
                    cursor,
                    Identifier.class,
                    Identifier.BYTES,
                    PointerInnerObject.class,
                    PointerInnerObject.BYTES
            );
            cursor++;
            return new KeyValue(keyValuePointerAtIndex.getKey(), keyValuePointerAtIndex.getValue());
        }
    }

}
