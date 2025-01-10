package com.github.sepgh.testudo.storage.index.header;

import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class InMemoryIndexHeaderManager implements IndexHeaderManager {
    private final Header header = new Header();

    @Override
    public synchronized Optional<Location> getRootOfIndex(int indexId) {
        Location location = header.getRoots().get(indexId);
        return Optional.ofNullable(location);
    }

    @Override
    public synchronized void setRootOfIndex(int indexId, Location location) throws IOException {
        header.getRoots().put(indexId, location);
    }

    @Override
    public synchronized void setIndexBeginningInChunk(int indexId, Location location) throws IOException {
        List<Header.IndexOffset> indexOffsets = this.header.getChunkIndexOffset().computeIfAbsent(
                location.getChunk(),
                integer -> new ArrayList<>()
        );

        Optional<Header.IndexOffset> optionalIndexOffset = indexOffsets.stream().filter(indexOffset -> indexOffset.getIndexId() == indexId).findFirst();
        if (optionalIndexOffset.isPresent()) {
            optionalIndexOffset.get().setOffset(location.getOffset());
        } else {
            indexOffsets.add(new Header.IndexOffset(indexId, location.getOffset()));
        }

    }

    @Override
    public synchronized Optional<Location> getIndexBeginningInChunk(int indexId, int chunk) {
        List<Header.IndexOffset> indexOffsets = this.header.getChunkIndexOffset().computeIfAbsent(
                chunk,
                integer -> new ArrayList<>()
        );
        Optional<Header.IndexOffset> optionalIndexOffset = indexOffsets.stream().filter(indexOffset -> indexOffset.getIndexId() == indexId).findFirst();
        return optionalIndexOffset.map(indexOffset -> new Location(chunk, indexOffset.getOffset()));
    }

    @Override
    public synchronized Optional<Location> getNextIndexBeginningInChunk(int indexId, int chunk) {
        Optional<Header.IndexOffset> nextIndexOffset = this.header.getNextIndexOffset(chunk, indexId);
        return nextIndexOffset.map(indexOffset -> new Location(chunk, indexOffset.getOffset()));
    }

    @Override
    public synchronized List<Integer> getIndexesInChunk(int chunk) {
        List<Header.IndexOffset> indexOffsets = this.header.getIndexOffsets(chunk);
        List<Integer> indexes = new ArrayList<>(indexOffsets.size());
        for (Header.IndexOffset indexOffset : indexOffsets) {
            indexes.add(indexOffset.getIndexId());
        }
        indexes.sort(Comparator.naturalOrder());
        return indexes;
    }

    @Override
    public synchronized Optional<Integer> getNextIndexIdInChunk(int indexId, int chunk) {
        List<Header.IndexOffset> indexOffsets = this.header.getIndexOffsets(chunk);
        int currIndex = -1;
        for (int i = 0; i < indexOffsets.size(); i++) {
            if (indexId == indexOffsets.get(i).getIndexId()) {
                currIndex = i;
                break;
            }
        }

        if (currIndex == -1 || currIndex == indexOffsets.size() - 1) {
            return Optional.empty();
        }

        return Optional.of(indexOffsets.get(currIndex + 1).getIndexId());
    }

    @Override
    public synchronized List<Integer> getChunksOfIndex(int indexId) {
        List<Integer> chunks = new ArrayList<>();
        Map<Integer, List<Header.IndexOffset>> chunkIndexOffset = this.header.getChunkIndexOffset();
        chunkIndexOffset.forEach((chunk, indexOffsets) -> {
            indexOffsets.stream().filter(indexOffset -> indexOffset.getIndexId() == indexId).findAny().ifPresent(indexOffset -> chunks.add(chunk));
        });
        return chunks;
    }

    @Override
    public synchronized Optional<Location> getNullBitmapLocation(int indexId) {
        Map<Integer, Location> nullBitmaps = this.header.getNullBitmaps();
        return Optional.ofNullable(nullBitmaps.get(indexId));
    }

    @Override
    public synchronized void setNullBitmapLocation(int indexId, Location location) throws IOException {
        this.header.getNullBitmaps().put(indexId, location);
    }

    public static class SingletonFactory extends IndexHeaderManagerSingletonFactory {

        @Override
        @SneakyThrows
        public IndexHeaderManager create(Path path) {
            return new InMemoryIndexHeaderManager();
        }
    }
}
