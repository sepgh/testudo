package com.github.sepgh.internal.storage.header;


import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Optional;

@Data
@Builder
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

    public Optional<Table> getTableOfIndex(int tableId){
        for (int i = 0; i < tables.size(); i++){
            Table table = tables.get(i);
            if (tableId == table.id){
                return Optional.of(table);
            }
        }
        return Optional.empty();
    }

    @Data
    @Builder
    public static class Table {
        private String name;
        private int id;
        private List<IndexChunk> chunks;
        private IndexChunk root;

        public Optional<IndexChunk> getIndexChunk(int id){
            return getChunks().stream().filter(indexChunk -> indexChunk.chunk == id).findFirst();
        }
    }

    @Data
    @Builder
    public static class IndexChunk {
        private int chunk;
        private long offset;
    }
}
