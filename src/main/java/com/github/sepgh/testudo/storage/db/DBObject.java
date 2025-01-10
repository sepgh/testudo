package com.github.sepgh.testudo.storage.db;

import com.github.sepgh.testudo.exception.InvalidDBObjectWrapper;
import com.github.sepgh.testudo.utils.BinaryUtils;
import com.google.common.primitives.Ints;
import lombok.Getter;


/*
 * - META
 *      - Flags (1 byte):
 *          - 0x01  alive
 *          - 0x00  dead
 *      - SchemeId (int, 4 bytes)
 *      - CollectionId (int, 4 bytes)
 *      - Version (int, 4 bytes)
 *      - Size  (int, 4 bytes)
 * - Data  (byte[])
 */
public class DBObject {
    public static byte ALIVE_OBJ = 0x01;
    public static int FLAG_BYTES = 1;
    public static int META_BYTES = FLAG_BYTES + (4 * Integer.BYTES);
    public static int META_SCHEME_ID_OFFSET = FLAG_BYTES;
    public static int META_COLLECTION_ID_OFFSET = FLAG_BYTES + Integer.BYTES;
    public static int META_VERSION_OFFSET = FLAG_BYTES + (2 * Integer.BYTES);
    public static int META_SIZE_OFFSET = FLAG_BYTES + (3 * Integer.BYTES);
    private final byte[] wrappedData;
    @Getter
    private final int begin;
    @Getter
    private final int end;
    @Getter
    private final int length;
    @Getter
    private final Page page;
    @Getter
    private boolean modified;

    public DBObject(Page page, int begin, int end) throws InvalidDBObjectWrapper {
        this.wrappedData = page.getData();
        this.begin = begin;
        this.end = end;
        length = end - begin;
        this.page = page;
        this.verify();
    }

    private void verify() throws InvalidDBObjectWrapper {
        if (end > this.wrappedData.length - 1)
            throw new InvalidDBObjectWrapper("The passed value for end (%d) is larger than the data byte array length (%d).".formatted(end, this.wrappedData.length));

        int min = META_BYTES + 1;
        if (this.length < min) {
            throw new InvalidDBObjectWrapper("Minimum size of byte array should be " + min + ". Passed: " + this.length);
        }
    }

    public int getSchemeId() {
        return DBObject.getSchemeId(this.wrappedData, this.begin);
    }

    public int getCollectionId() {
        return DBObject.getCollectionId(this.wrappedData, this.begin);
    }

    public boolean isAlive() {
        byte[] b = new byte[this.length];
        System.arraycopy(wrappedData, this.begin, b, 0, this.length);
        return DBObject.isAlive(this.wrappedData, this.begin);
    }

    public int getObjectSize() {
        return this.getDataSize() + DBObject.META_BYTES;
    }

    public int getDataSize() {
        return DBObject.getDataSize(this.wrappedData, this.begin);
    }

    public byte[] readData(int offset, int size) {
        byte[] output = new byte[size];

        System.arraycopy(
                this.wrappedData,
                begin + META_BYTES + offset,
                output,
                0,
                size
        );
        return output;
    }

    public void modifyData(byte[] value) throws InvalidDBObjectWrapper {
        if (value.length > DBObject.getWrappedSize(value.length)) {
            throw new InvalidDBObjectWrapper("Can't extend DBObject size. Create a new one."); // Todo
        }

        this.setSize(value.length);

        this.modified = true;

        System.arraycopy(
                value,
                0,
                this.wrappedData,
                begin + META_BYTES,
                value.length
        );
    }

    public void modifyData(int offset, byte[] value){
        this.modified = true;

        System.arraycopy(
                value,
                0,
                this.wrappedData,
                begin + META_BYTES + offset,
                value.length
        );
    }

    public int getVersion() {
        return DBObject.getVersion(this.wrappedData, this.begin);
    }

    public void setVersion(int version) {
        this.modified = true;

        System.arraycopy(
                Ints.toByteArray(version),
                0,
                this.wrappedData,
                begin + META_VERSION_OFFSET,
                Integer.BYTES
        );
    }

    public void setSchemeId(int schemeId) {
        this.modified = true;

        System.arraycopy(
                Ints.toByteArray(schemeId),
                0,
                this.wrappedData,
                begin + META_SCHEME_ID_OFFSET,
                Integer.BYTES
        );
    }

    public void setCollectionId(int collectionId) {
        this.modified = true;

        System.arraycopy(
                Ints.toByteArray(collectionId),
                0,
                this.wrappedData,
                begin + META_COLLECTION_ID_OFFSET,
                Integer.BYTES
        );
    }

    private void setSize(int size) {
        this.modified = true;

        System.arraycopy(
                Ints.toByteArray(size),
                0,
                this.wrappedData,
                begin + META_SIZE_OFFSET,
                Integer.BYTES
        );
    }

    public void deactivate() {
        this.modified = true;
        this.wrappedData[begin] = (byte) (wrappedData[begin] & ~ALIVE_OBJ);
    }

    public void activate() {
        this.modified = true;
        this.wrappedData[begin] = (byte) (wrappedData[begin] | ALIVE_OBJ);
    }

    public byte[] getData() {
        byte[] result = new byte[getDataSize()];
        System.arraycopy(
                this.wrappedData,
                begin + META_BYTES,
                result,
                0,
                result.length
        );
        return result;
    }

    public static int getSchemeId(byte[] wrappedData, int begin) {
        return BinaryUtils.bytesToInteger(wrappedData, begin + META_SCHEME_ID_OFFSET);
    }

    public static int getCollectionId(byte[] wrappedData, int begin) {
        return BinaryUtils.bytesToInteger(wrappedData, begin + META_COLLECTION_ID_OFFSET);
    }

    public static int getVersion(byte[] wrappedData, int begin) {
        return BinaryUtils.bytesToInteger(wrappedData, begin + META_VERSION_OFFSET);
    }

    public static boolean isAlive(byte[] wrappedData, int begin){
        return (wrappedData[begin] & ALIVE_OBJ) == ALIVE_OBJ;
    }

    public static int getDataSize(byte[] wrappedData, int begin){
        return BinaryUtils.bytesToInteger(wrappedData, begin + META_SIZE_OFFSET);
    }

    public static int getWrappedSize(int length) {
        return META_BYTES + length;
    }
}
