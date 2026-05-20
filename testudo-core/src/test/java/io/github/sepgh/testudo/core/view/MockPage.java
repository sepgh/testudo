package io.github.sepgh.testudo.core.view;

import io.github.sepgh.testudo.core.Page;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

public class MockPage implements Page {

    private final MemorySegment segment;
    private boolean dirty;

    public MockPage(int size) {
        this.segment = Arena.ofAuto().allocate(size);
        this.dirty = false;
    }

    @Override
    public void dirty() {
        this.dirty = true;
    }

    @Override
    public MemorySegment segment() {
        return segment;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void resetDirty() {
        this.dirty = false;
    }
}
