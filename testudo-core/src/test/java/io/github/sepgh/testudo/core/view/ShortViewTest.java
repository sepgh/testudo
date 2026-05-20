package io.github.sepgh.testudo.core.view;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ShortViewTest {

    private MockPage page;
    private ShortView view;

    @BeforeEach
    void setUp() {
        page = new MockPage(64);
        view = new ShortView(page);
        view.pointTo(0);
    }

    @Test
    void size_returnsTwoByteWidth() {
        assertEquals(Short.BYTES, view.size());
    }

    @Test
    void setAndGet_roundTrip_zero() {
        view.set((short) 0);
        assertEquals((short) 0, view.get());
    }

    @Test
    void setAndGet_roundTrip_positiveValue() {
        view.set((short) 1000);
        assertEquals((short) 1000, view.get());
    }

    @Test
    void setAndGet_roundTrip_negativeValue() {
        view.set((short) -500);
        assertEquals((short) -500, view.get());
    }

    @Test
    void setAndGet_roundTrip_minValue() {
        view.set(Short.MIN_VALUE);
        assertEquals(Short.MIN_VALUE, view.get());
    }

    @Test
    void setAndGet_roundTrip_maxValue() {
        view.set(Short.MAX_VALUE);
        assertEquals(Short.MAX_VALUE, view.get());
    }

    @Test
    void set_marksPageDirty() {
        page.resetDirty();
        view.set((short) 7);
        assertTrue(page.isDirty());
    }

    @Test
    void compareTo_lessThan() {
        view.set((short) 100);
        assertTrue(view.compareTo((short) 200) < 0);
    }

    @Test
    void compareTo_greaterThan() {
        view.set((short) 200);
        assertTrue(view.compareTo((short) 100) > 0);
    }

    @Test
    void compareTo_equal() {
        view.set((short) 150);
        assertEquals(0, view.compareTo((short) 150));
    }

    @Test
    void pointTo_differentOffset_independentStorage() {
        view.pointTo(0);
        view.set((short) 100);

        view.pointTo(Short.BYTES);
        view.set((short) 200);

        view.pointTo(0);
        assertEquals((short) 100, view.get());

        view.pointTo(Short.BYTES);
        assertEquals((short) 200, view.get());
    }
}
