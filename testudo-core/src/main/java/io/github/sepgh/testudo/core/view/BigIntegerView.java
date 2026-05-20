package io.github.sepgh.testudo.core.view;

import io.github.sepgh.testudo.core.Page;

import java.lang.foreign.ValueLayout;
import java.math.BigInteger;
import java.nio.ByteOrder;

public class BigIntegerView extends AbstractView<BigInteger> implements Comparable<BigInteger> {
    private static final ValueLayout.OfLong LAYOUT =
            ValueLayout.JAVA_LONG_UNALIGNED
                    .withOrder(ByteOrder.BIG_ENDIAN);

    public BigIntegerView(Page page) {
        super(page);
    }

    @Override
    public int size() {
        return Long.BYTES;
    }

    @Override
    protected void doSet(BigInteger value) {
        long encoded = value.longValue() ^ Long.MIN_VALUE;
        segment.set(LAYOUT, offset, encoded);
    }

    @Override
    protected BigInteger doRead() {
        long encoded = segment.get(LAYOUT, offset);
        return BigInteger.valueOf(encoded ^ Long.MIN_VALUE);
    }

    @Override
    public int compareTo(BigInteger o) {
        long stored = segment.get(LAYOUT, offset);
        long encoded = o.longValue() ^ Long.MIN_VALUE;
        return Long.compareUnsigned(stored, encoded);
    }
}
