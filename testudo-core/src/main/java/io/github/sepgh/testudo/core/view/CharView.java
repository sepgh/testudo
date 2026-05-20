package io.github.sepgh.testudo.core.view;

import io.github.sepgh.testudo.core.Page;

import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

public class CharView extends AbstractView<Character> implements Comparable<Character> {
    private static final ValueLayout.OfShort LAYOUT =
            ValueLayout.JAVA_SHORT_UNALIGNED
                    .withOrder(ByteOrder.BIG_ENDIAN);

    public CharView(Page page) {
        super(page);
    }

    @Override
    public int size() {
        return Character.BYTES;
    }

    @Override
    protected void doSet(Character value) {
        segment.set(LAYOUT, offset, (short) value.charValue());
    }

    @Override
    protected Character doRead() {
        return (char) Short.toUnsignedInt(segment.get(LAYOUT, offset));
    }

    @Override
    public int compareTo(Character o) {
        int stored = Short.toUnsignedInt(segment.get(LAYOUT, offset));
        int encoded = (int) o.charValue();
        return Integer.compare(stored, encoded);
    }
}
