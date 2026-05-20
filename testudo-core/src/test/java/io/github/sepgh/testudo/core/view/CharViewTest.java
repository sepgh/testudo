package io.github.sepgh.testudo.core.view;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CharViewTest {

    private MockPage page;
    private CharView view;

    @BeforeEach
    void setUp() {
        page = new MockPage(64);
        view = new CharView(page);
        view.pointTo(0);
    }

    @Test
    void size_returnsTwoByteWidth() {
        assertEquals(Character.BYTES, view.size());
    }

    @Test
    void setAndGet_roundTrip_asciiChar() {
        view.set('A');
        assertEquals('A', view.get());
    }

    @Test
    void setAndGet_roundTrip_lowercaseChar() {
        view.set('z');
        assertEquals('z', view.get());
    }

    @Test
    void setAndGet_roundTrip_digitChar() {
        view.set('9');
        assertEquals('9', view.get());
    }

    @Test
    void setAndGet_roundTrip_nullChar() {
        view.set('\0');
        assertEquals('\0', view.get());
    }

    @Test
    void setAndGet_roundTrip_maxChar() {
        view.set(Character.MAX_VALUE);
        assertEquals(Character.MAX_VALUE, view.get());
    }

    @Test
    void setAndGet_roundTrip_unicodeChar() {
        view.set('\u4E2D');
        assertEquals('\u4E2D', view.get());
    }

    @Test
    void set_marksPageDirty() {
        page.resetDirty();
        view.set('X');
        assertTrue(page.isDirty());
    }

    @Test
    void compareTo_lessThan() {
        view.set('A');
        assertTrue(view.compareTo('B') < 0);
    }

    @Test
    void compareTo_greaterThan() {
        view.set('B');
        assertTrue(view.compareTo('A') > 0);
    }

    @Test
    void compareTo_equal() {
        view.set('M');
        assertEquals(0, view.compareTo('M'));
    }

    @Test
    void pointTo_differentOffset_independentStorage() {
        view.pointTo(0);
        view.set('X');

        view.pointTo(Character.BYTES);
        view.set('Y');

        view.pointTo(0);
        assertEquals('X', view.get());

        view.pointTo(Character.BYTES);
        assertEquals('Y', view.get());
    }
}
