package com.github.sepgh.testudo.storage.index.header;

import com.github.sepgh.testudo.index.Pointer;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public interface IndexHeaderManager {
    Optional<Location> getRootOfIndex(int indexId);
    void setRootOfIndex(int indexId, Location location) throws IOException;

    default void setIndexBeginningInChunk(int indexId, Location location) throws IOException {}
    default Optional<Location> getIndexBeginningInChunk(int indexId, int chunk){return Optional.of(new Location(0, 0));}

    default Optional<Location> getNextIndexBeginningInChunk(int indexId, int chunk){
        return Optional.of(new Location(0, 0));
    }

    List<Integer> getIndexesInChunk(int chunk);

    Optional<Integer> getNextIndexIdInChunk(int indexId, int chunk);

    List<Integer> getChunksOfIndex(int indexId);

    @Data
    @AllArgsConstructor
    class Location {
        private int chunk;
        private long offset;

        public static Location fromPointer(Pointer pointer) {
            return new Location(pointer.getChunk(), pointer.getPosition());
        }

        public Pointer toPointer(byte type) {
            return new Pointer(type, offset, chunk);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Location location = (Location) o;
            return getChunk() == location.getChunk() && getOffset() == location.getOffset();
        }

        @Override
        public int hashCode() {
            return Objects.hash(getChunk(), getOffset());
        }

        @Override
        public String toString() {
            return "Location{" +
                    "chunk=" + chunk +
                    ", fileOffset=" + offset +
                    '}';
        }
    }
}
