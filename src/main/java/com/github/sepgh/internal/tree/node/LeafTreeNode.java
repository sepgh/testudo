package com.github.sepgh.internal.tree.node;

import com.github.sepgh.internal.tree.Pointer;
import com.github.sepgh.internal.tree.TreeNodeUtils;
import com.google.common.collect.ImmutableList;

import java.util.*;

public class LeafTreeNode extends BaseTreeNode {
    public LeafTreeNode(byte[] data) {
        super(data);
        setType(Type.LEAF);
    }

    public void setNextSiblingPointer(Pointer pointer, int degree){
        modified();
        TreeNodeUtils.setNextPointer(this, degree, pointer);
    }

    public void setPreviousSiblingPointer(Pointer pointer, int degree){
        modified();
        TreeNodeUtils.setPreviousPointer(this, degree, pointer);
    }

    public Optional<Pointer> getPreviousSiblingPointer(int degree){
        return TreeNodeUtils.getPreviousPointer(this, degree);
    }

    public Optional<Pointer> getNextSiblingPointer(int degree){
        return TreeNodeUtils.getNextPointer(this, degree);
    }

    public Iterator<KeyValue> getKeyValues(){
        return new KeyValueIterator(this);
    }

    public List<KeyValue> getKeyValueList() {
        return ImmutableList.copyOf(getKeyValues());
    }

    public void setKeyValues(List<KeyValue> keyValueList, int degree){
        modified();
        for (int i = 0; i < keyValueList.size(); i++){
            KeyValue keyValue = keyValueList.get(i);
            TreeNodeUtils.setKeyValueAtIndex(this, i, keyValue.key(), keyValue.value());
        }
        for (int i = keyValueList.size(); i < (degree - 1); i++){
            TreeNodeUtils.removeKeyValueAtIndex(this, i);
        }
    }

    public void setKeyValue(int index, KeyValue keyValue){
        TreeNodeUtils.setKeyValueAtIndex(this, index, keyValue.key(), keyValue.value());
    }

    public List<KeyValue> split(long identifier, Pointer pointer, int degree){
        int mid = (degree - 1) / 2;

        List<KeyValue> allKeyValues = new ArrayList<>(getKeyValueList());
        allKeyValues.add(new KeyValue(identifier, pointer));
        Collections.sort(allKeyValues);

        List<KeyValue> toKeep = allKeyValues.subList(0, mid + 1);
        this.setKeyValues(toKeep, degree);
        return allKeyValues.subList(mid + 1, allKeyValues.size());
    }

    public int addKeyValue(long identifier, Pointer pointer) {
        return TreeNodeUtils.addKeyValueAndGetIndex(this, identifier, pointer);
    }

    public int addKeyValue(KeyValue keyValue) {
        return TreeNodeUtils.addKeyValueAndGetIndex(this, keyValue.key, keyValue.value);
    }

    private static class TreeNodeKeyValueIterator implements Iterator<Map.Entry<Long, Pointer>> {
        private final BaseTreeNode node;
        private int cursor; // Cursor points to current key index, not current byte
        private boolean hasNext = true;

        private TreeNodeKeyValueIterator(BaseTreeNode node) {
            this.node = node;
        }

        @Override
        public boolean hasNext() {
            if (!hasNext)
                return false;

            hasNext = TreeNodeUtils.hasKeyValuePointerAtIndex(this.node, cursor);
            return hasNext;
        }

        @Override
        public Map.Entry<Long, Pointer> next() {
            Map.Entry<Long, Pointer> value = TreeNodeUtils.getKeyValuePointerAtIndex(this.node, cursor);
            cursor++;
            return value;
        }
    }


    public record KeyValue(long key, Pointer value) implements Comparable<KeyValue> {

        @Override
        public int compareTo(KeyValue o) {
            return Long.compare(this.key, o.key);
        }
    }

    public class KeyValueIterator implements Iterator<KeyValue>{
        private int cursor = 0;

        private final LeafTreeNode node;

        public KeyValueIterator(LeafTreeNode node) {
            this.node = node;
        }

        @Override
        public boolean hasNext() {
            return TreeNodeUtils.hasKeyAtIndex(node, cursor);
        }

        @Override
        public KeyValue next() {
            Map.Entry<Long, Pointer> keyValuePointerAtIndex = TreeNodeUtils.getKeyValuePointerAtIndex(node, cursor);
            cursor++;
            return new KeyValue(keyValuePointerAtIndex.getKey(), keyValuePointerAtIndex.getValue());
        }
    }

}
