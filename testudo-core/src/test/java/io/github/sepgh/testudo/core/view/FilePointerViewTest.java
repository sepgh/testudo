package io.github.sepgh.testudo.core.view;

import io.github.sepgh.testudo.core.model.FilePointer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FilePointerViewTest {

    private MockPage page;
    private FilePointerView view;

    @BeforeEach
    void setUp() {
        page = new MockPage(128);
        view = new FilePointerView(page);
        view.pointTo(0);
    }

    @Test
    void size_returnsEightByteWidth() {
        assertEquals(Integer.BYTES + Integer.BYTES, view.size());
    }

    @Test
    void setAndGet_roundTrip_zeroValues() {
        FilePointer fp = new FilePointer(0, 0);
        view.set(fp);
        assertEquals(fp, view.get());
    }

    @Test
    void setAndGet_roundTrip_positiveValues() {
        FilePointer fp = new FilePointer(5, 10);
        view.set(fp);
        assertEquals(fp, view.get());
    }

    @Test
    void setAndGet_roundTrip_maxValues() {
        FilePointer fp = new FilePointer(Integer.MAX_VALUE, Integer.MAX_VALUE);
        view.set(fp);
        assertEquals(fp, view.get());
    }

    @Test
    void setAndGet_roundTrip_minValues() {
        FilePointer fp = new FilePointer(Integer.MIN_VALUE, Integer.MIN_VALUE);
        view.set(fp);
        assertEquals(fp, view.get());
    }

    @Test
    void set_marksPageDirty() {
        page.resetDirty();
        view.set(new FilePointer(1, 2));
        assertTrue(page.isDirty());
    }

    @Test
    void getPage_returnsCorrectPageValue() {
        view.set(new FilePointer(7, 3));
        assertEquals(7, view.getPage());
    }

    @Test
    void getSlot_returnsCorrectSlotValue() {
        view.set(new FilePointer(7, 3));
        assertEquals(3, view.getSlot());
    }

    @Test
    void setPage_updatesPageField() {
        view.set(new FilePointer(1, 5));
        view.setPage(99);
        assertEquals(99, view.getPage());
        assertEquals(5, view.getSlot());
    }

    @Test
    void setPage_marksPageDirty() {
        view.set(new FilePointer(1, 1));
        page.resetDirty();
        view.setPage(10);
        assertTrue(page.isDirty());
    }

    @Test
    void setSlot_updatesSlotField() {
        view.set(new FilePointer(3, 1));
        view.setSlot(77);
        assertEquals(3, view.getPage());
        assertEquals(77, view.getSlot());
    }

    @Test
    void setSlot_marksPageDirty() {
        view.set(new FilePointer(1, 1));
        page.resetDirty();
        view.setSlot(10);
        assertTrue(page.isDirty());
    }

    @Test
    void compareTo_lessThan_byPage() {
        view.set(new FilePointer(1, 0));
        assertTrue(view.compareTo(new FilePointer(2, 0)) < 0);
    }

    @Test
    void compareTo_lessThan_bySlot() {
        view.set(new FilePointer(1, 0));
        assertTrue(view.compareTo(new FilePointer(1, 1)) < 0);
    }

    @Test
    void compareTo_greaterThan_byPage() {
        view.set(new FilePointer(3, 0));
        assertTrue(view.compareTo(new FilePointer(2, 0)) > 0);
    }

    @Test
    void compareTo_equal() {
        FilePointer fp = new FilePointer(4, 7);
        view.set(fp);
        assertEquals(0, view.compareTo(fp));
    }

    @Test
    void pointTo_differentOffset_independentStorage() {
        FilePointer fp1 = new FilePointer(1, 2);
        FilePointer fp2 = new FilePointer(3, 4);

        int stride = view.size();

        view.pointTo(0);
        view.set(fp1);

        view.pointTo(stride);
        view.set(fp2);

        view.pointTo(0);
        assertEquals(fp1, view.get());

        view.pointTo(stride);
        assertEquals(fp2, view.get());
    }
}
