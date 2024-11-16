package com.github.sepgh.testudo.storage.db;

import com.github.sepgh.testudo.index.Pointer;
import lombok.Getter;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

public interface RemovedObjectsTracer {
    void add(RemovedObjectLocation removedObjectLocation);
    Optional<RemovedObjectLocation> getRemovedObjectLocation(int length);
    List<RemovedObjectLocation> getRemovedObjectLocations();

    record RemovedObjectLocation(Pointer pointer, int length){}

    class InMemoryRemovedObjectsTracer implements RemovedObjectsTracer {
        @Getter
        public List<RemovedObjectLocation> removedObjectLocations = new CopyOnWriteArrayList<>();
        private final int minLengthToSplit;

        public InMemoryRemovedObjectsTracer(int minLengthToSplit) {
            this.minLengthToSplit = minLengthToSplit;
        }

        @Override
        public synchronized void add(RemovedObjectLocation removedObjectLocation) {
            int i = Collections.binarySearch(this.removedObjectLocations, removedObjectLocation, Comparator.comparingInt(RemovedObjectLocation::length));
            if (i > 0){
                removedObjectLocations.add(i, removedObjectLocation);
            } else {
                i = (i * -1) - 1;
                removedObjectLocations.add(i, removedObjectLocation);
            }
        }

        @Override
        public synchronized Optional<RemovedObjectLocation> getRemovedObjectLocation(int length) {
            if (this.removedObjectLocations.isEmpty()){
                return Optional.empty();
            }

            int i = Collections.binarySearch(this.removedObjectLocations, new RemovedObjectLocation(null, length), Comparator.comparingInt(RemovedObjectLocation::length));
            if (i < 0) {
                i = (i * -1) - 1;
            }


            if (i <= removedObjectLocations.size() - 1 && length <= removedObjectLocations.get(i).length) {
                RemovedObjectLocation removedObjectLocation = removedObjectLocations.remove(i);

                if (removedObjectLocation.length - length > this.minLengthToSplit) {
                    this.add(
                            new RemovedObjectLocation(
                                    new Pointer(
                                            removedObjectLocation.pointer().getType(),
                                            removedObjectLocation.pointer().getPosition() + length,
                                            removedObjectLocation.pointer().getChunk()
                                    ),
                                    removedObjectLocation.length - length
                            )
                    );
                }
                return Optional.of(removedObjectLocation);
            }

            return Optional.empty();
        }
    }
}
