package io.github.sepgh.testudo.core.view;

import io.github.sepgh.testudo.core.Page;

import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.time.Instant;

public class TimestampView extends AbstractView<Instant> {
    private static final ValueLayout.OfLong LAYOUT =
            ValueLayout.JAVA_LONG_UNALIGNED
                    .withOrder(ByteOrder.BIG_ENDIAN);

    public TimestampView(Page page) {
        super(page);
    }

    @Override
    public int size() {
        return Long.BYTES;
    }

    @Override
    protected void doSet(Instant value) {
        long encoded = value.toEpochMilli() ^ Long.MIN_VALUE;
        segment.set(LAYOUT, offset, encoded);
    }

    @Override
    protected Instant doRead() {
        long encoded = segment.get(LAYOUT, offset);
        return Instant.ofEpochMilli(encoded ^ Long.MIN_VALUE);
    }

    @Override
    public int compareTo(Instant o) {
        return get().compareTo(o);
    }
}
