package io.github.sepgh.testudo.core.view;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LongViewTest {

    private MockPage page;
    private LongView view;

    @BeforeEach
    void setUp() {
        page = new MockPage(64);
        view = new LongView(page);
        view.pointTo(0);
    }

    @Test
    void size_returnsEightByteWidth() {
        assertEquals(Long.BYTES, view.size());
    }

    @Test
    void setAndGet_roundTrip_zero() {
        view.set(0L);
        assertEquals(0L, view.get());
    }

    @Test
    void setAndGet_roundTrip_positiveValue() {
        view.set(9_876_543_210L);
        assertEquals(9_876_543_210L, view.get());
    }

    @Test
    void setAndGet_roundTrip_negativeValue() {
        view.set(-1_234_567_890L);
        assertEquals(-1_234_567_890L, view.get());
    }

    @Test
    void setAndGet_roundTrip_minValue() {
        view.set(Long.MIN_VALUE);
        assertEquals(Long.MIN_VALUE, view.get());
    }

    @Test
    void setAndGet_roundTrip_maxValue() {
        view.set(Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, view.get());
    }

    @Test
    void set_marksPageDirty() {
        page.resetDirty();
        view.set(1L);
        assertTrue(page.isDirty());
    }

    @Test
    void compareTo_lessThan() {
        view.set(10L);
        assertTrue(view.compareTo(20L) < 0);
    }

    @Test
    void compareTo_greaterThan() {
        view.set(20L);
        assertTrue(view.compareTo(10L) > 0);
    }

    @Test
    void compareTo_equal() {
        view.set(99L);
        assertEquals(0, view.compareTo(99L));
    }

    @Test
    void pointTo_differentOffset_independentStorage() {
        view.pointTo(0);
        view.set(111L);

        view.pointTo(Long.BYTES);
        view.set(222L);

        view.pointTo(0);
        assertEquals(111L, view.get());

        view.pointTo(Long.BYTES);
        assertEquals(222L, view.get());
    }
}
