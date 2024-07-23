package com.github.sepgh.testudo.storage.db;

import com.github.sepgh.testudo.exception.VerificationException;
import com.github.sepgh.testudo.utils.BinaryUtils;
import com.google.common.primitives.Ints;
import lombok.Getter;


/*
 * - Flags (1 byte):
 *     - 0x01  alive
 *     - 0x00  dead
 * - CollectionId (int, 4 bytes)
 * - Size  (int, 4 bytes)
 * - Data  (byte[])
 */
public class DBObjectWrapper {
    public static byte ALIVE_OBJ = 0x01;
    public static int FLAG_BYTES = 1;
    public static int META_BYTES = FLAG_BYTES + (2 * Integer.BYTES);
    private final byte[] wrappedData;
    @Getter
    private final int begin;
    @Getter
    private final int end;
    @Getter
    private final int length;
    @Getter
    private final Page page;

    public DBObjectWrapper(Page page, int begin, int end) throws VerificationException.InvalidDBObjectWrapper {
        this.wrappedData = page.getData();
        this.begin = begin;
        this.end = end;
        length = end - begin;
        this.page = page;
        this.setSize(length);
        this.verify();
    }

    private void verify() throws VerificationException.InvalidDBObjectWrapper {
        if (end > this.wrappedData.length - 1)
            throw new VerificationException.InvalidDBObjectWrapper("The passed value for end (%d) is larger than the data byte array length (%d).".formatted(end, this.wrappedData.length));

        int min = META_BYTES + 1;
        if (this.length < min) {
            throw new VerificationException.InvalidDBObjectWrapper("Minimum size of byte array should be " + min + ". Passed: " + this.length);
        }
    }

    public int getCollectionId() {
        return DBObjectWrapper.getCollectionId(this.wrappedData, this.begin);
    }

    public boolean isAlive() {

        byte[] b = new byte[this.length];
        System.arraycopy(wrappedData, this.begin, b, 0, this.length);
        return DBObjectWrapper.isAlive(this.wrappedData, this.begin);
    }

    public int getDataSize() {
        return DBObjectWrapper.getDataSize(this.wrappedData, this.begin);
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

    public void modifyData(byte[] value) throws VerificationException.InvalidDBObjectWrapper {
        if (value.length > this.length - META_BYTES) {
            throw new VerificationException.InvalidDBObjectWrapper("Todo"); // Todo
        }

        System.arraycopy(
                value,
                0,
                this.wrappedData,
                begin + META_BYTES,
                value.length
        );
    }

    public void modifyData(int offset, byte[] value){
        System.arraycopy(
                value,
                0,
                this.wrappedData,
                begin + META_BYTES + offset,
                value.length
        );
    }

    public void setCollectionId(int collectionId) {
        System.arraycopy(
                Ints.toByteArray(collectionId),
                0,
                this.wrappedData,
                begin + FLAG_BYTES,
                Integer.BYTES
        );
    }

    public void setSize(int size) {
        System.arraycopy(
                Ints.toByteArray(size),
                0,
                this.wrappedData,
                begin + FLAG_BYTES + Integer.BYTES,
                Integer.BYTES
        );
    }

    public void deactivate() {
        this.wrappedData[begin] = (byte) (wrappedData[begin] & ~ALIVE_OBJ);
    }

    public void activate() {
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

    public static int getCollectionId(byte[] wrappedData, int begin) {
        return BinaryUtils.bytesToInteger(wrappedData, begin + FLAG_BYTES);
    }

    public static boolean isAlive(byte[] wrappedData, int begin){
        return (wrappedData[begin] & ALIVE_OBJ) == ALIVE_OBJ;
    }

    public static int getDataSize(byte[] wrappedData, int begin){
        return BinaryUtils.bytesToInteger(wrappedData, begin + FLAG_BYTES + Integer.BYTES);
    }

    public static int getWrappedSize(int length) {
        return FLAG_BYTES + (2 * Integer.BYTES) + length;
    }
}
