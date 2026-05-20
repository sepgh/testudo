package io.github.sepgh.testudo.core.view;

import io.github.sepgh.testudo.core.Page;

import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

public class ShortView extends AbstractView<Short> {
    private static final ValueLayout.OfShort LAYOUT =
            ValueLayout.JAVA_SHORT_UNALIGNED
                    .withOrder(ByteOrder.BIG_ENDIAN);

    public ShortView(Page page) {
        super(page);
    }

    @Override
    public int size() {
        return Short.BYTES;
    }

    @Override
    protected void doSet(Short value) {
        short encoded = (short) (value ^ Short.MIN_VALUE);
        segment.set(LAYOUT, offset, encoded);
    }

    @Override
    protected Short doRead() {
        short encoded = segment.get(LAYOUT, offset);
        return (short) (encoded ^ Short.MIN_VALUE);
    }

    @Override
    public int compareTo(Short o) {
        return Short.compare(get(), o);
    }
}
