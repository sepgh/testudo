package io.github.sepgh.testudo.core.view;

import io.github.sepgh.testudo.core.Page;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

public class LongView extends AbstractView<Long> {
    private static final ValueLayout.OfLong LAYOUT =
            ValueLayout.JAVA_LONG_UNALIGNED
                    .withOrder(ByteOrder.BIG_ENDIAN);

    public LongView(Page page) {
        super(page);
    }

    @Override
    public int size() {
        return Long.BYTES;
    }

    @Override
    protected void doSet(Long value) {
        long encoded = value ^ Long.MIN_VALUE;
        segment.set(LAYOUT, offset, encoded);
    }

    @Override
    protected Long doRead() {
        long encoded = segment.get(LAYOUT, offset);
        return encoded ^ Long.MIN_VALUE;
    }

    @Override
    public int compareTo(Long o) {
        return Long.compare(get(), o);
    }
}
