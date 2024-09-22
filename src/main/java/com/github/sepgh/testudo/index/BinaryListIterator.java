package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.index.data.IndexBinaryObject;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.storage.db.DBObject;
import lombok.Getter;

import java.util.ListIterator;
import java.util.NoSuchElementException;


// Todo: has next and has previous functions cant check with null (no longer Optional), so go back to previous implementation where currentValue property was not used
public class BinaryListIterator<V extends Comparable<V>> implements ListIterator<V> {

    private final EngineConfig engineConfig;
    private final IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory;
    private int numberOfElements = 0;
    private volatile int cursor = 0;
    @Getter
    private byte[] data;
    private volatile V currentValue;

    public BinaryListIterator(EngineConfig engineConfig, IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory, byte[] data) {
        this.engineConfig = engineConfig;
        this.valueIndexBinaryObjectFactory = valueIndexBinaryObjectFactory;
        this.data = data;
        this.numberOfElements = this.data.length / valueIndexBinaryObjectFactory.size();
    }

    private void fixCursorBound(){
        if (cursor < 0)
            cursor = 0;
        if (cursor >= numberOfElements)
            cursor = numberOfElements - 1;
    }

    public void resetCursor(){
        cursor = 0;
        currentValue = null;
    }

    @Override
    public boolean hasNext() {
        if (cursor < numberOfElements){
            fixCursorBound();
            currentValue = getObjectAt(cursor);
            return true;
        }
        currentValue = null;
        return false;
    }

    @Override
    public V next() {
        if (cursor >= numberOfElements)
            throw new NoSuchElementException();

        synchronized (this){
            try {
                if (currentValue != null){
                    cursor++;
                    return currentValue;
                }
            } finally {
                currentValue = null;
            }
            cursor++;
            return getObjectAt(cursor);
        }
    }

    @Override
    public boolean hasPrevious() {
        if (cursor >= 0){
            fixCursorBound();
            currentValue = getObjectAt(cursor);
            return true;
        }
        currentValue = null;
        return false;
    }

    @Override
    public V previous() {
        if (cursor < 0)
            throw new NoSuchElementException();

        synchronized (this){
            try {
                if (currentValue != null){
                    cursor--;
                    return currentValue;
                }
            } finally {
                currentValue = null;
            }

            cursor--;
            return getObjectAt(cursor);
        }
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

        // Shift back the bytes after the element
        System.arraycopy(
                this.data,
                (i + 1) * this.valueIndexBinaryObjectFactory.size(),
                this.data,
                i * this.valueIndexBinaryObjectFactory.size(),
                this.data.length - (i + 1) * this.valueIndexBinaryObjectFactory.size()
        );

        // Remove the end
        System.arraycopy(
                new byte[this.valueIndexBinaryObjectFactory.size()],
                0,
                this.data,
                this.data.length - this.valueIndexBinaryObjectFactory.size(),
                this.valueIndexBinaryObjectFactory.size()
        );

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

        if (getObjectAt(this.numberOfElements - 1) != null){  // Todo: we wont have null object. track last element
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

        if (i != 0){
            System.arraycopy(
                    this.data,
                    i * this.valueIndexBinaryObjectFactory.size(),
                    this.data,
                    (i + 1) * this.valueIndexBinaryObjectFactory.size(),
                    this.data.length - (i + 1) * this.valueIndexBinaryObjectFactory.size()
            );
        }

        System.arraycopy(
                this.valueIndexBinaryObjectFactory.create(v).getBytes(),
                0,
                this.data,
                i * this.valueIndexBinaryObjectFactory.size(),
                this.valueIndexBinaryObjectFactory.size()
        );

        this.numberOfElements = this.data.length / valueIndexBinaryObjectFactory.size();
        return true;
    }

    private V getObjectAt(int index) {
        int offset = index * valueIndexBinaryObjectFactory.size();
        IndexBinaryObject<V> vIndexBinaryObject = valueIndexBinaryObjectFactory.create(this.data, offset);
        return vIndexBinaryObject.asObject();
    }

    private int binarySearchPosition(V v){
        int low = 0;
        int high = numberOfElements - 1;
        int mid = 0;

        while (low <= high) {
            mid = low + (high - low) / 2;

            V objectAtMid = getObjectAt(mid);

            // Todo: This is done linearly :/
            //       Alternative solution: keep track of last element index (store it in the data)
            //       If last element is smaller than the new value, just add the new value to last+1
            //       Otherwise, run the binary search with low=0 and high=last so we wont face problems in middle
            // START SECTION OF COMMENT ^
            if (objectAtMid == null){   // Todo: we actually wont have a null object
                for (int i = low; i < mid; i++){
                    if (getObjectAt(i) != null) {
                        return i;
                    }
                }
                return mid;
            }
            // END SECTION OF COMMENT

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
        int high = numberOfElements - 1;

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
}
