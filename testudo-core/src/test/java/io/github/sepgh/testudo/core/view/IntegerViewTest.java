package io.github.sepgh.testudo.core.view;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IntegerViewTest {

    private MockPage page;
    private IntegerView view;

    @BeforeEach
    void setUp() {
        page = new MockPage(64);
        view = new IntegerView(page);
        view.pointTo(0);
    }

    @Test
    void size_returnsFourByteWidth() {
        assertEquals(Integer.BYTES, view.size());
    }

    @Test
    void setAndGet_roundTrip_zero() {
        view.set(0);
        assertEquals(0, view.get());
    }

    @Test
    void setAndGet_roundTrip_positiveValue() {
        view.set(123456);
        assertEquals(123456, view.get());
    }

    @Test
    void setAndGet_roundTrip_negativeValue() {
        view.set(-99999);
        assertEquals(-99999, view.get());
    }

    @Test
    void setAndGet_roundTrip_minValue() {
        view.set(Integer.MIN_VALUE);
        assertEquals(Integer.MIN_VALUE, view.get());
    }

    @Test
    void setAndGet_roundTrip_maxValue() {
        view.set(Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE, view.get());
    }

    @Test
    void set_marksPageDirty() {
        page.resetDirty();
        view.set(42);
        assertTrue(page.isDirty());
    }

    @Test
    void compareTo_lessThan() {
        view.set(1);
        assertTrue(view.compareTo(2) < 0);
    }

    @Test
    void compareTo_greaterThan() {
        view.set(2);
        assertTrue(view.compareTo(1) > 0);
    }

    @Test
    void compareTo_equal() {
        view.set(100);
        assertEquals(0, view.compareTo(100));
    }

    @Test
    void pointTo_differentOffset_independentStorage() {
        view.pointTo(0);
        view.set(111);

        view.pointTo(Integer.BYTES);
        view.set(222);

        view.pointTo(0);
        assertEquals(111, view.get());

        view.pointTo(Integer.BYTES);
        assertEquals(222, view.get());
    }
}
