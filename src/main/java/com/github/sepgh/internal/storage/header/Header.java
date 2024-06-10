package com.github.sepgh.internal.storage.header;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Optional;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Header {
    private String database;
    private List<Table> tables;
    private boolean initialized;

    public Optional<Table> getTableOfId(int id){
        return getTables().stream().filter(table -> table.id == id).findFirst();
    }

    public int tablesCount() {
        return this.tables.size();
    }

    public int indexOfTable(int tableId){
        for (int i = 0; i < tables.size(); i++){
            if (tableId == tables.get(i).id){
                return i;
            }
        }
        return -1;
    }

    public Optional<Table> getTableOfIndex(int index){
        return index < tables.size() ? Optional.of(tables.get(index)) : Optional.empty();
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Table {
        private String name;
        private int id;
        private List<IndexChunk> chunks;
        private volatile IndexChunk root;
        private boolean initialized;

        public Optional<IndexChunk> getIndexChunk(int id){
            return getChunks().stream().filter(indexChunk -> indexChunk.chunk == id).findFirst();
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class IndexChunk {
        private int chunk;
        private long offset;
    }
}
