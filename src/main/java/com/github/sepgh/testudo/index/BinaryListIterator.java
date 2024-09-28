package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.index.data.IndexBinaryObject;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.utils.BinaryUtils;
import com.google.common.primitives.Ints;
import lombok.Getter;

import java.util.ListIterator;
import java.util.NoSuchElementException;


public class BinaryListIterator<V extends Comparable<V>> implements ListIterator<V> {

    private final EngineConfig engineConfig;
    private final IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory;
    private volatile int cursor = -1;
    private boolean lastCallWasNext = false;
    @Getter
    private byte[] data;
    public static final int META_SIZE_END_CURSOR = Integer.BYTES;
    public static final int META_INDEX_END_CURSOR = 0;
    public static final int META_INDEX_END_CURSOR_DEFAULT = -1;
    public static final int META_SIZE = META_SIZE_END_CURSOR;

    public BinaryListIterator(EngineConfig engineConfig, IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory, byte[] data) {
        this.engineConfig = engineConfig;
        this.valueIndexBinaryObjectFactory = valueIndexBinaryObjectFactory;
        this.data = data;
    }

    public void initialize() {
        System.arraycopy(
                Ints.toByteArray(META_INDEX_END_CURSOR_DEFAULT),
                0,
                data,
                0,
                META_SIZE_END_CURSOR
        );
    }

    private int getNumberOfElements() {
        return this.data.length - META_SIZE / valueIndexBinaryObjectFactory.size();
    }

    public int getLastItemIndex(){
        return BinaryUtils.bytesToInteger(data, META_INDEX_END_CURSOR);
    }

    public void setLastItemIndex(int index) {
        byte[] byteArray = Ints.toByteArray(index);
        System.arraycopy(byteArray, 0, data, META_INDEX_END_CURSOR, byteArray.length);
    }

    @Override
    public boolean hasNext() {
        return cursor + 1 <= getLastItemIndex();
    }

    @Override
    public synchronized V next() {
        if (cursor + 1 <= getLastItemIndex()) {
            V next = getObjectAt(cursor + 1);
            cursor++;
            lastCallWasNext = true;
            return next;
        }

        throw new NoSuchElementException();
    }

    @Override
    public boolean hasPrevious() {
        return cursor >= 0;
    }

    @Override
    public synchronized V previous() {
        if (cursor < 0) {
            throw new NoSuchElementException();
        }

        V previous = getObjectAt(cursor);
        cursor--;
        return previous;
    }

    @Override
    public int nextIndex() {
        return cursor + 1;
    }

    @Override
    public int previousIndex() {
        return cursor - 1;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public boolean remove(V value) {
        int i = binarySearchMatching(value);

        if (i == -1)
            return false;

        clearObjectAt(i);

        if (i < this.getLastItemIndex()){
            for (int j = i; j < getLastItemIndex() - 1; j++) {
                byte[] bytesAtJPlusOne = getObjectBytes(j + 1);
                setObjectBytes(j, bytesAtJPlusOne);
            }
        }

        this.setLastItemIndex(this.getLastItemIndex() - 1);

        return true;
    }

    @Override
    public void set(V v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(V v) {
        throw new UnsupportedOperationException("Use addNew(V) instead to handle exceptions");
    }

    private int getDbObjectSize() {
        return this.data.length + DBObject.META_BYTES;
    }
    
    public boolean addNew(V v) throws IndexBinaryObject.InvalidIndexBinaryObject {
        // GROW BYTE[] IF POSSIBLE
        int lastItemIndex = getLastItemIndex();
        if (lastItemIndex == this.getNumberOfElements() - 1) {
            if ((engineConfig.getDbPageSize()) > getDbObjectSize() + (5 * valueIndexBinaryObjectFactory.size())){
                byte[] newData = new byte[getDbObjectSize() + (5 * valueIndexBinaryObjectFactory.size())];
                System.arraycopy(
                        this.data,
                        0,
                        newData,
                        0,
                        this.data.length
                );
                this.data = newData;
            } else {
                // Todo: throw better exception
                throw new RuntimeException("No space left to add item. This will exceed the limit of DB Page size.");
            }
        }

        int i = binarySearchPosition(v);

        if (i == -1)
            return false;

        if (i <= this.getLastItemIndex()){
            for (int j = this.getLastItemIndex(); j > i; j--) {
                byte[] bytesAtJ = getObjectBytes(j);
                setObjectBytes(j+1, bytesAtJ);
            }
        }

        setObjectAt(i, v);
        setLastItemIndex(lastItemIndex + 1);
        return true;
    }

    private V getObjectAt(int index) {
        int offset = META_SIZE + index * valueIndexBinaryObjectFactory.size();
        IndexBinaryObject<V> vIndexBinaryObject = valueIndexBinaryObjectFactory.create(this.data, offset);
        return vIndexBinaryObject.asObject();
    }

    private byte[] getObjectBytes(int index) {
        int offset = META_SIZE + index * valueIndexBinaryObjectFactory.size();
        IndexBinaryObject<V> vIndexBinaryObject = valueIndexBinaryObjectFactory.create(this.data, offset);
        return vIndexBinaryObject.getBytes();
    }

    private void clearObjectAt(int index) {
        int offset = META_SIZE + index * valueIndexBinaryObjectFactory.size();
        System.arraycopy(
                new byte[valueIndexBinaryObjectFactory.size()],
                0,
                this.data,
                offset,
                valueIndexBinaryObjectFactory.size()
        );
    }

    private void setObjectBytes(int index, byte[] bytes) {
        int offset = META_SIZE + index * valueIndexBinaryObjectFactory.size();
        System.arraycopy(bytes, 0, this.data, offset, bytes.length);
    }

    private void setObjectAt(int index, V v) throws IndexBinaryObject.InvalidIndexBinaryObject {
        int offset = META_SIZE + index * valueIndexBinaryObjectFactory.size();
        IndexBinaryObject<V> vIndexBinaryObject = valueIndexBinaryObjectFactory.create(v);
        byte[] bytes = vIndexBinaryObject.getBytes();
        System.arraycopy(bytes, 0, this.data, offset, bytes.length);
    }

    private int binarySearchPosition(V v){
        int low = 0;
        int high = getLastItemIndex();
        int mid = 0;

        if (v.compareTo(getObjectAt(high)) > 0)
            return high + 1;

        while (low <= high) {
            mid = low + (high - low) / 2;

            V objectAtMid = getObjectAt(mid);

            if (objectAtMid.compareTo(v) == 0) {
                return -1;
            } else if (objectAtMid.compareTo(v) < 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        return -(low + 1);
    }

    private int binarySearchMatching(V v) {
        int low = 0;
        int high = getLastItemIndex();

        while (low <= high) {
            int mid = low + (high - low) / 2;

            V objectAtMid = getObjectAt(mid);

            if (objectAtMid.compareTo(v) == 0) {
                return mid;
            } else if (objectAtMid.compareTo(v) < 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        return -1;
    }

    public void resetCursor() {
        cursor = -1;
    }
}
