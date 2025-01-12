package com.github.sepgh.testudo.ds;

import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.IndexBinaryObjectCreationException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.AscendingBinaryListIterator;
import com.github.sepgh.testudo.index.DescendingBinaryListIterator;
import com.github.sepgh.testudo.index.data.IndexBinaryObject;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.utils.BinaryUtils;
import com.google.common.primitives.Ints;
import lombok.Getter;

import java.util.ListIterator;

public class BinaryList<V extends Comparable<V>> {
    private final EngineConfig engineConfig;
    protected final IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory;
    @Getter
    protected byte[] data;
    public static final int META_SIZE_END_CURSOR = Integer.BYTES;
    public static final int META_INDEX_END_CURSOR = 0;
    public static final int META_INDEX_END_CURSOR_DEFAULT = -1;
    public static final int META_SIZE = META_SIZE_END_CURSOR;

    public BinaryList(EngineConfig engineConfig, IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory, byte[] data) {
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

    public int getNumberOfElements() {
        return (this.data.length - META_SIZE) / valueIndexBinaryObjectFactory.size();
    }

    public int getLastItemIndex(){
        return BinaryUtils.bytesToInteger(data, META_INDEX_END_CURSOR);
    }

    public void setLastItemIndex(int index) {
        byte[] byteArray = Ints.toByteArray(index);
        System.arraycopy(byteArray, 0, data, META_INDEX_END_CURSOR, byteArray.length);
    }

    private int getDbObjectSize() {
        return this.data.length + DBObject.META_BYTES;
    }


    public boolean remove(V value) throws DeserializationException {
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


    public boolean addNew(V v) throws InternalOperationException, DeserializationException {
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
                throw new InternalOperationException("No space left to add item. This will exceed the limit of DB Page size.");
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

    public V getObjectAt(int index) throws DeserializationException {
        int offset = META_SIZE + index * valueIndexBinaryObjectFactory.size();
        IndexBinaryObject<V> vIndexBinaryObject = valueIndexBinaryObjectFactory.create(this.data, offset);
        return vIndexBinaryObject.asObject();
    }

    public byte[] getObjectBytes(int index) {
        int offset = META_SIZE + index * valueIndexBinaryObjectFactory.size();
        IndexBinaryObject<V> vIndexBinaryObject = valueIndexBinaryObjectFactory.create(this.data, offset);
        return vIndexBinaryObject.getBytes();
    }

    public void clearObjectAt(int index) {
        int offset = META_SIZE + index * valueIndexBinaryObjectFactory.size();
        System.arraycopy(
                new byte[valueIndexBinaryObjectFactory.size()],
                0,
                this.data,
                offset,
                valueIndexBinaryObjectFactory.size()
        );
    }

    public void setObjectBytes(int index, byte[] bytes) {
        int offset = META_SIZE + index * valueIndexBinaryObjectFactory.size();
        System.arraycopy(bytes, 0, this.data, offset, bytes.length);
    }

    public void setObjectAt(int index, V v) throws IndexBinaryObjectCreationException {
        int offset = META_SIZE + index * valueIndexBinaryObjectFactory.size();
        IndexBinaryObject<V> vIndexBinaryObject = valueIndexBinaryObjectFactory.create(v);
        byte[] bytes = vIndexBinaryObject.getBytes();
        System.arraycopy(bytes, 0, this.data, offset, bytes.length);
    }

    public int binarySearchPosition(V v) throws DeserializationException {
        int low = 0;
        int high = getLastItemIndex();
        int mid;

        if (high == -1)
            return 0;

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

    public int binarySearchMatching(V v) throws DeserializationException {
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

    public ListIterator<V> getIterator(Order order) {
        return switch (order) {
            case DESC -> new DescendingBinaryListIterator<>(this);
            case ASC -> new AscendingBinaryListIterator<>(this);
        };
    }
}
