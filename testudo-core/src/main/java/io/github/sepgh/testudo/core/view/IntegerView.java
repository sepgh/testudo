package io.github.sepgh.testudo.core.view;

import io.github.sepgh.testudo.core.Page;

import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

public class IntegerView extends AbstractView<Integer> {
    private static final ValueLayout.OfInt LAYOUT =
            ValueLayout.JAVA_INT_UNALIGNED
                    .withOrder(ByteOrder.BIG_ENDIAN);

    public IntegerView(Page page) {
        super(page);
    }

    @Override
    public int size() {
        return Integer.BYTES;
    }

    @Override
    protected void doSet(Integer value) {
        int encoded = value ^ Integer.MIN_VALUE;
        segment.set(LAYOUT, offset, encoded);
    }

    @Override
    protected Integer doRead() {
        int encoded = segment.get(LAYOUT, offset);
        return encoded ^ Integer.MIN_VALUE;
    }

    @Override
    public int compareTo(Integer o) {
        return Integer.compare(get(), o);
    }
}
