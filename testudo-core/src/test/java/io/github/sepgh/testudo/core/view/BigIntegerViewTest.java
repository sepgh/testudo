package io.github.sepgh.testudo.core.view;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.*;

class BigIntegerViewTest {

    private MockPage page;
    private BigIntegerView view;

    @BeforeEach
    void setUp() {
        page = new MockPage(64);
        view = new BigIntegerView(page);
        view.pointTo(0);
    }

    @Test
    void size_returnsEightByteWidth() {
        assertEquals(Long.BYTES, view.size());
    }

    @Test
    void setAndGet_roundTrip_zero() {
        view.set(BigInteger.ZERO);
        assertEquals(BigInteger.ZERO, view.get());
    }

    @Test
    void setAndGet_roundTrip_positiveValue() {
        BigInteger value = BigInteger.valueOf(123_456_789L);
        view.set(value);
        assertEquals(value, view.get());
    }

    @Test
    void setAndGet_roundTrip_negativeValue() {
        BigInteger value = BigInteger.valueOf(-987_654_321L);
        view.set(value);
        assertEquals(value, view.get());
    }

    @Test
    void setAndGet_roundTrip_longMinValue() {
        BigInteger value = BigInteger.valueOf(Long.MIN_VALUE);
        view.set(value);
        assertEquals(value, view.get());
    }

    @Test
    void setAndGet_roundTrip_longMaxValue() {
        BigInteger value = BigInteger.valueOf(Long.MAX_VALUE);
        view.set(value);
        assertEquals(value, view.get());
    }

    @Test
    void set_marksPageDirty() {
        page.resetDirty();
        view.set(BigInteger.ONE);
        assertTrue(page.isDirty());
    }

    @Test
    void compareTo_lessThan() {
        view.set(BigInteger.valueOf(10));
        assertTrue(view.compareTo(BigInteger.valueOf(20)) < 0);
    }

    @Test
    void compareTo_greaterThan() {
        view.set(BigInteger.valueOf(20));
        assertTrue(view.compareTo(BigInteger.valueOf(10)) > 0);
    }

    @Test
    void compareTo_equal() {
        view.set(BigInteger.valueOf(50));
        assertEquals(0, view.compareTo(BigInteger.valueOf(50)));
    }

    @Test
    void pointTo_differentOffset_independentStorage() {
        view.pointTo(0);
        view.set(BigInteger.valueOf(111L));

        view.pointTo(Long.BYTES);
        view.set(BigInteger.valueOf(222L));

        view.pointTo(0);
        assertEquals(BigInteger.valueOf(111L), view.get());

        view.pointTo(Long.BYTES);
        assertEquals(BigInteger.valueOf(222L), view.get());
    }
}
