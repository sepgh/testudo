package com.github.sepgh.testudo.storage.db;


import com.github.sepgh.testudo.exception.VerificationException;
import com.github.sepgh.testudo.utils.BinaryUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;

import java.util.*;


/*
*   Meta:  cursorPosition (int)
*/

@AllArgsConstructor
@Data
public class Page {
    public static int META_BYTES = Integer.BYTES;

    private final int pageNumber;
    private final int pageSize;
    private final int chunk;
    private int cursorPosition;
    private final byte[] data;
    private final Map<Integer, DBObject> wrapperPool;

    public Page(int pageNumber, int pageSize, int chunk, byte[] data) {
        this.pageNumber = pageNumber;
        this.pageSize = pageSize;
        this.chunk = chunk;
        this.cursorPosition = BinaryUtils.bytesToInteger(data, 0) + META_BYTES;
        this.data = data;
        this.wrapperPool = new HashMap<>();
    }

    public Page(int pageNumber, int pageSize, int chunk) {
        this(pageNumber, pageSize, chunk, new byte[0]);
    }

    public synchronized Optional<DBObject> getDBObjectWrapper(int offset) throws VerificationException.InvalidDBObjectWrapper {
        if (wrapperPool.containsKey(offset)) {
            return Optional.of(wrapperPool.get(offset));
        }

        int dataSize = DBObject.getDataSize(getData(), offset);
        if (dataSize == 0) {
            return Optional.empty();
        }

        int wrappedSize = DBObject.getWrappedSize(dataSize);
        DBObject dbObject = new DBObject(this, offset, offset + wrappedSize);

        wrapperPool.put(offset, dbObject);
        return Optional.of(dbObject);
    }

    public Iterator<DBObject> getIterator() {
        return new PageDBObjectsIterator(this);
    }

    public List<DBObject> getObjectList() {
        return ImmutableList.copyOf(getIterator());
    }

    private void setCursorPosition(int cursorPosition){
        this.cursorPosition = cursorPosition;
        System.arraycopy(Ints.toByteArray(cursorPosition), 0, this.data, 0, Integer.BYTES);
    }

    public synchronized Optional<DBObject> getEmptyDBObjectWrapper(int length) throws VerificationException.InvalidDBObjectWrapper {
        if (getData().length - cursorPosition > length + DBObject.META_BYTES){
            DBObject dbObject = new DBObject(this, cursorPosition, cursorPosition + length + DBObject.META_BYTES);
            Optional<DBObject> output = Optional.of(dbObject);
            this.wrapperPool.putIfAbsent(cursorPosition, dbObject);
            this.setCursorPosition(this.cursorPosition + length + DBObject.META_BYTES);
            return output;
        }
        return Optional.empty();
    }

    private static class PageDBObjectsIterator implements Iterator<DBObject> {
        private int cursor = 0;
        private final Page page;

        private PageDBObjectsIterator(Page page) {
            this.page = page;
        }

        @Override
        public boolean hasNext() {
            return DBObject.getDataSize(this.page.getData(), this.cursor) != 0;
        }

        @SneakyThrows
        @Override
        public DBObject next() {
            int dataSize = DBObject.getDataSize(this.page.getData(), this.cursor);
            int wrappedSize = DBObject.getWrappedSize(dataSize);

            DBObject dbObject = new DBObject(this.page, cursor, cursor + wrappedSize);

            cursor += wrappedSize;
            return dbObject;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Page page = (Page) o;
        return getPageNumber() == page.getPageNumber() && getPageSize() == page.getPageSize() && getChunk() == page.getChunk();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPageNumber(), getPageSize(), getChunk());
    }

    @Override
    public String toString() {
        return "Page{" +
                "pageNumber=" + pageNumber +
                ", pageSize=" + pageSize +
                ", chunk=" + chunk +
                ", cursorPosition=" + cursorPosition +
                '}';
    }
}
