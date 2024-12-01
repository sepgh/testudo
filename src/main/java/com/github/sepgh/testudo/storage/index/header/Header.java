package com.github.sepgh.testudo.storage.index.header;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Header {
    Map<Integer, IndexHeaderManager.Location> roots = new HashMap<>();
    Map<Integer, List<Header.IndexOffset>> chunkIndexOffset = new HashMap<>();

    @Data
    @AllArgsConstructor
    public static class IndexOffset {
        private int indexId;
        private long offset;
    }

    public List<Header.IndexOffset> getIndexOffsets(int chunk) {
        List<Header.IndexOffset> indexOffsets = this.chunkIndexOffset.computeIfAbsent(chunk, k -> new ArrayList<>());
        indexOffsets.sort(Comparator.comparingLong(o -> o.offset));
        return indexOffsets;
    }

    public Optional<Header.IndexOffset> getNextIndexOffset(int chunk, int indexId) {
        List<Header.IndexOffset> indexOffsets = this.getIndexOffsets(chunk);

        for (int i = 0; i < indexOffsets.size(); i++) {
            if (indexOffsets.get(i).getIndexId() == indexId && i != indexOffsets.size() - 1) {
                return Optional.of(indexOffsets.get(i + 1));
            }
        }

        return Optional.empty();
    }
}
