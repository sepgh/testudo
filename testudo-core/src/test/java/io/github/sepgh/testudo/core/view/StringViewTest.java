package io.github.sepgh.testudo.core.view;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class StringViewTest {

    private MockPage page;

    @BeforeEach
    void setUp() {
        page = new MockPage(1024);
    }

    @Test
    void size_defaultMaxSize() {
        StringView view = new StringView(page);
        assertEquals(StringView.DEFAULT_MAX_SIZE, view.size());
    }

    @Test
    void size_customMaxSize() {
        StringView view = new StringView(page, 64);
        assertEquals(64, view.size());
    }

    @Test
    void setAndGet_roundTrip_simpleAscii() {
        StringView view = new StringView(page, 64);
        view.pointTo(0);
        view.set("hello");
        assertEquals("hello", view.get());
    }

    @Test
    void setAndGet_roundTrip_emptyString() {
        StringView view = new StringView(page, 64);
        view.pointTo(0);
        view.set("");
        assertEquals("", view.get());
    }

    @Test
    void setAndGet_roundTrip_exactMaxLength() {
        int maxSize = 10;
        StringView view = new StringView(page, maxSize);
        view.pointTo(0);
        String value = "1234567890";
        view.set(value);
        assertEquals(value, view.get());
    }

    @Test
    void setAndGet_truncatesWhenExceedingMaxSize() {
        int maxSize = 5;
        StringView view = new StringView(page, maxSize);
        view.pointTo(0);
        view.set("hello world");
        assertEquals("hello", view.get());
    }

    @Test
    void setAndGet_roundTrip_withSpaces() {
        StringView view = new StringView(page, 64);
        view.pointTo(0);
        view.set("hello world");
        assertEquals("hello world", view.get());
    }

    @Test
    void setAndGet_roundTrip_customCharset() {
        StringView view = new StringView(page, 64, StandardCharsets.ISO_8859_1);
        view.pointTo(0);
        view.set("caf\u00e9");
        assertEquals("caf\u00e9", view.get());
    }

    @Test
    void set_marksPageDirty() {
        StringView view = new StringView(page, 64);
        view.pointTo(0);
        page.resetDirty();
        view.set("test");
        assertTrue(page.isDirty());
    }

    @Test
    void compareTo_lessThan() {
        StringView view = new StringView(page, 64);
        view.pointTo(0);
        view.set("apple");
        assertTrue(view.compareTo("banana") < 0);
    }

    @Test
    void compareTo_greaterThan() {
        StringView view = new StringView(page, 64);
        view.pointTo(0);
        view.set("banana");
        assertTrue(view.compareTo("apple") > 0);
    }

    @Test
    void compareTo_equal() {
        StringView view = new StringView(page, 64);
        view.pointTo(0);
        view.set("same");
        assertEquals(0, view.compareTo("same"));
    }

    @Test
    void pointTo_differentOffset_independentStorage() {
        StringView view = new StringView(page, 32);

        view.pointTo(0);
        view.set("first");

        view.pointTo(32);
        view.set("second");

        view.pointTo(0);
        assertEquals("first", view.get());

        view.pointTo(32);
        assertEquals("second", view.get());
    }

    @Test
    void overwrite_shorterString_doesNotLeakPreviousContent() {
        StringView view = new StringView(page, 64);
        view.pointTo(0);
        view.set("longer string here");
        view.set("short");
        assertEquals("short", view.get());
    }
}
