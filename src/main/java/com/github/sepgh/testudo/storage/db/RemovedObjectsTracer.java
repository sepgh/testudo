package com.github.sepgh.testudo.storage.db;

import com.github.sepgh.testudo.index.Pointer;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

public interface RemovedObjectsTracer {
    void add(RemovedObjectLocation removedObjectLocation);
    Optional<RemovedObjectLocation> getRemovedObjectLocation(int length);

    record RemovedObjectLocation(Pointer pointer, int length){}

    class InMemoryRemovedObjectsTracer implements RemovedObjectsTracer {
        public List<RemovedObjectLocation> removedObjectLocationList = new CopyOnWriteArrayList<>();

        @Override
        public void add(RemovedObjectLocation removedObjectLocation) {
            int i = Collections.binarySearch(this.removedObjectLocationList, removedObjectLocation, Comparator.comparingInt(RemovedObjectLocation::length));
            if (i > 0){
                removedObjectLocationList.add(i, removedObjectLocation);
            } else {
                i = (i * -1) - 1;
                removedObjectLocationList.add(i, removedObjectLocation);
            }
        }

        @Override
        public Optional<RemovedObjectLocation> getRemovedObjectLocation(int length) {
            if (this.removedObjectLocationList.isEmpty())
                return Optional.empty();
            int i = Collections.binarySearch(this.removedObjectLocationList, new RemovedObjectLocation(null, length), Comparator.comparingInt(RemovedObjectLocation::length));
            if (i <= 0) {
                i = (i * -1) - 1;
            }
            return Optional.of(removedObjectLocationList.remove(i));
        }
    }
}
