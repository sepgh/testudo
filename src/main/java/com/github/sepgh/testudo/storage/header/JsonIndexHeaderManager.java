package com.github.sepgh.testudo.storage.header;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class JsonIndexHeaderManager implements IndexHeaderManager {

    private final Path path;
    private final JHeader jHeader;
    private final Gson gson = new GsonBuilder().serializeNulls().create();

    public JsonIndexHeaderManager(Path path) throws IOException {
        this.path = path;
        File f = this.path.toFile();
        if(!f.exists()){
            f.createNewFile();
        }
        JsonReader jsonReader = new JsonReader(new FileReader(f));
        JHeader readObject = gson.fromJson(jsonReader, JHeader.class);
        if (readObject == null) {
            this.jHeader = new JHeader();
            gson.toJson(jHeader, new FileWriter(this.path.toFile()));
        } else {
            this.jHeader = readObject;
        }
    }

    @Override
    public Optional<Location> getRootOfIndex(int indexId) {
        Location location = jHeader.getRoots().get(indexId);
        return Optional.ofNullable(location);
    }

    @Override
    public synchronized void setRootOfIndex(int indexId, Location location) throws IOException {
        jHeader.getRoots().put(indexId, location);
        try (FileWriter writer = new FileWriter(this.path.toFile())) {
            gson.toJson(jHeader, writer);
        }
    }

    @Override
    public void setIndexBeginningInChunk(int indexId, Location location) throws IOException {
        List<JHeader.IndexOffset> indexOffsets = this.jHeader.getChunkIndexOffset().computeIfAbsent(
                location.getChunk(),
                integer -> new ArrayList<>()
        );
        synchronized (this){
            Optional<JHeader.IndexOffset> optionalIndexOffset = indexOffsets.stream().filter(indexOffset -> indexOffset.indexId == indexId).findFirst();
            if (optionalIndexOffset.isPresent()) {
                optionalIndexOffset.get().offset = location.getOffset();
            } else {
                indexOffsets.add(new JHeader.IndexOffset(indexId, location.getOffset()));
            }

            try (FileWriter writer = new FileWriter(this.path.toFile())) {
                gson.toJson(jHeader, writer);
            }
        }
    }

    @Override
    public Optional<Location> getIndexBeginningInChunk(int indexId, int chunk) {
        List<JHeader.IndexOffset> indexOffsets = this.jHeader.getChunkIndexOffset().computeIfAbsent(
                chunk,
                integer -> new ArrayList<>()
        );
        Optional<JHeader.IndexOffset> optionalIndexOffset = indexOffsets.stream().filter(indexOffset -> indexOffset.indexId == indexId).findFirst();
        return optionalIndexOffset.map(indexOffset -> new Location(chunk, indexOffset.offset));
    }

    @Override
    public Optional<Location> getNextIndexBeginningInChunk(int indexId, int chunk) {
        Optional<JHeader.IndexOffset> nextIndexOffset = this.jHeader.getNextIndexOffset(chunk, indexId);
        return nextIndexOffset.map(indexOffset -> new Location(chunk, indexOffset.offset));
    }

    @Override
    public List<Integer> getIndexesInChunk(int chunk) {
        List<JHeader.IndexOffset> indexOffsets = this.jHeader.getIndexOffsets(chunk);
        List<Integer> indexes = new ArrayList<>(indexOffsets.size());
        for (JHeader.IndexOffset indexOffset : indexOffsets) {
            indexes.add(indexOffset.indexId);
        }
        indexes.sort(Comparator.naturalOrder());
        return indexes;
    }

    @Override
    public Optional<Integer> getNextIndexIdInChunk(int indexId, int chunk) {
        List<JHeader.IndexOffset> indexOffsets = this.jHeader.getIndexOffsets(chunk);
        int currIndex = -1;
        for (int i = 0; i < indexOffsets.size(); i++) {
            if (indexId == indexOffsets.get(i).indexId) {
                currIndex = i;
                break;
            }
        }

        if (currIndex == -1 || currIndex == indexOffsets.size() - 1) {
            return Optional.empty();
        }

        return Optional.of(indexOffsets.get(currIndex + 1).getIndexId());
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class JHeader {
        Map<Integer, Location> roots = new ConcurrentHashMap<>();
        Map<Integer, List<IndexOffset>> chunkIndexOffset = new ConcurrentHashMap<>();

        @Data
        @AllArgsConstructor
        private static class IndexOffset {
            private int indexId;
            private long offset;
        }

        public List<IndexOffset> getIndexOffsets(int chunk) {
            List<IndexOffset> indexOffsets = this.chunkIndexOffset.computeIfAbsent(chunk, k -> new ArrayList<>());
            indexOffsets.sort(Comparator.comparingLong(o -> o.offset));
            return indexOffsets;
        }

        public Optional<IndexOffset> getNextIndexOffset(int chunk, int indexId) {
            List<IndexOffset> indexOffsets = this.getIndexOffsets(chunk);

            for (int i = 0; i < indexOffsets.size(); i++) {
                if (indexOffsets.get(i).getIndexId() == indexId && i != indexOffsets.size() - 1) {
                    return Optional.of(indexOffsets.get(i + 1));
                }
            }

            return Optional.empty();
        }
    }


    public static class Factory implements IndexHeaderManagerFactory {

        @Override
        @SneakyThrows
        public IndexHeaderManager getInstance(Path path) {
            return new JsonIndexHeaderManager(path);
        }
    }

}
