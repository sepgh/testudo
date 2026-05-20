package io.github.sepgh.testudo.core.view;

import io.github.sepgh.testudo.core.Page;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class ByteView extends AbstractView<Byte> {
    private static final ValueLayout.OfByte LAYOUT =
            ValueLayout.JAVA_BYTE;

    public ByteView(Page page) {
        super(page);
    }

    @Override
    public int size() {
        return Byte.BYTES;
    }

    @Override
    protected void doSet(Byte value) {
        byte encoded = (byte) (value ^ Byte.MIN_VALUE);
        segment.set(LAYOUT, offset, encoded);
    }

    @Override
    protected Byte doRead() {
        byte encoded = segment.get(LAYOUT, offset);
        return (byte) (encoded ^ Byte.MIN_VALUE);
    }

    @Override
    public int compareTo(Byte o) {
        return Byte.compare(get(), o);
    }
}
