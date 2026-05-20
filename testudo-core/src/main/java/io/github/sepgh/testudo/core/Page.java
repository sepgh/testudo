package io.github.sepgh.testudo.core;

import java.lang.foreign.MemorySegment;

public interface Page {
    void dirty();
    MemorySegment segment();
}
