package com.github.sepgh.internal.storage;

import com.github.sepgh.internal.utils.BinaryUtils;
import com.google.common.primitives.Ints;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexHeader {
    public static final char HEADER_ENDING_CHAR = '\n';
    private List<TableMetaData> tableMetaDataList;

    public IndexHeader(byte[] data) {
        this.tableMetaDataList = new ArrayList<>(data.length / (2 * Integer.BYTES));
        initialize(data);
    }

    protected void initialize(byte[] data){
        for (int i = 0; i < (data.length / (2 * Integer.BYTES)); i++){
            int keyPosition = i * Integer.BYTES;
            int offsetPosition = keyPosition + Integer.BYTES;
            tableMetaDataList.add(
                    new TableMetaData(
                            BinaryUtils.bytesToInteger(data, keyPosition),
                            BinaryUtils.bytesToInteger(data, offsetPosition)
                    )
            );
        }
    }

    public TableMetaData getTableMetaDataAtIndex(int index){
        return this.tableMetaDataList.get(index);
    }

    public Optional<TableMetaData> getTableMetaData(int table){
        return tableMetaDataList.stream().filter(tableMetaData -> {
            return tableMetaData.table == table;
        }).findAny();
    }

    public int size(){
        return this.tableMetaDataList.size();
    }

    public int indexOf(TableMetaData tableMetaData){
        return this.tableMetaDataList.indexOf(tableMetaData);
    }

    public byte[] asByteArray(){
        byte[] bytes = new byte[tableMetaDataList.size() * 2 * Integer.BYTES];
        int i = 0;
        for (TableMetaData tableMetaData: tableMetaDataList){
            System.arraycopy(Ints.toByteArray(tableMetaData.getOffset()), 0, bytes, i * Integer.BYTES, Integer.BYTES);
            System.arraycopy(Ints.toByteArray(tableMetaData.getOffset()), 0, bytes, (i * Integer.BYTES) + 2, Integer.BYTES);
            i++;
        }
        return bytes;
    }

    public void addTableMetaData(TableMetaData tableMetaData){
        this.tableMetaDataList.add(tableMetaData);
    }


    @Getter
    public class TableMetaData {
        private final int table;
        private final int offset;

        private AtomicInteger nodeCount;

        public TableMetaData(int table, int offset) {
            this.table = table;
            this.offset = offset;
        }

        public int increaseNodeCount(){
            return this.nodeCount.incrementAndGet();
        }

        public int decreaseNodeCount(){
            return this.nodeCount.decrementAndGet();
        }

        public void setNodeCount(int count){
            this.nodeCount.set(count);
        }

        public int getNodeCount(){
            return this.nodeCount.get();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableMetaData that = (TableMetaData) o;
            return getTable() == that.getTable();
        }

        @Override
        public int hashCode() {
            return Objects.hash(getTable());
        }
    }

}
