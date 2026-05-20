package io.github.sepgh.testudo.core.view;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ByteViewTest {

    private MockPage page;
    private ByteView view;

    @BeforeEach
    void setUp() {
        page = new MockPage(64);
        view = new ByteView(page);
        view.pointTo(0);
    }

    @Test
    void size_returnsOneByteWidth() {
        assertEquals(Byte.BYTES, view.size());
    }

    @Test
    void setAndGet_roundTrip_zero() {
        view.set((byte) 0);
        assertEquals((byte) 0, view.get());
    }

    @Test
    void setAndGet_roundTrip_positiveValue() {
        view.set((byte) 42);
        assertEquals((byte) 42, view.get());
    }

    @Test
    void setAndGet_roundTrip_negativeValue() {
        view.set((byte) -1);
        assertEquals((byte) -1, view.get());
    }

    @Test
    void setAndGet_roundTrip_minValue() {
        view.set(Byte.MIN_VALUE);
        assertEquals(Byte.MIN_VALUE, view.get());
    }

    @Test
    void setAndGet_roundTrip_maxValue() {
        view.set(Byte.MAX_VALUE);
        assertEquals(Byte.MAX_VALUE, view.get());
    }

    @Test
    void set_marksPageDirty() {
        page.resetDirty();
        view.set((byte) 5);
        assertTrue(page.isDirty());
    }

    @Test
    void compareTo_lessThan() {
        view.set((byte) 10);
        assertTrue(view.compareTo((byte) 20) < 0);
    }

    @Test
    void compareTo_greaterThan() {
        view.set((byte) 20);
        assertTrue(view.compareTo((byte) 10) > 0);
    }

    @Test
    void compareTo_equal() {
        view.set((byte) 15);
        assertEquals(0, view.compareTo((byte) 15));
    }

    @Test
    void pointTo_differentOffset_independentStorage() {
        view.pointTo(0);
        view.set((byte) 10);

        view.pointTo(1);
        view.set((byte) 20);

        view.pointTo(0);
        assertEquals((byte) 10, view.get());

        view.pointTo(1);
        assertEquals((byte) 20, view.get());
    }
}
