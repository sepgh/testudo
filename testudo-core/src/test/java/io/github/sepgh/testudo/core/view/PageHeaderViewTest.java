package io.github.sepgh.testudo.core.view;

import io.github.sepgh.testudo.core.model.PageHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PageHeaderViewTest {

    private MockPage page;
    private PageHeaderView view;

    private static final PageHeader SAMPLE = new PageHeader(
            0xDEADBEEF,
            (short) 1,
            (short) 2,
            9999999999L,
            0x1A2B3C4D,
            0b0101,
            128,
            16,
            512,
            0
    );

    @BeforeEach
    void setUp() {
        page = new MockPage(PageHeaderView.getLayoutSize() * 2);
        view = new PageHeaderView(page);
        view.pointTo(0);
    }

    @Test
    void getLayoutSize_returnsFortyBytes() {
        assertEquals(40, PageHeaderView.getLayoutSize());
    }

    @Test
    void size_matchesLayoutSize() {
        assertEquals(PageHeaderView.getLayoutSize(), view.size());
    }

    @Test
    void setAndGet_roundTrip_fullHeader() {
        view.set(SAMPLE);
        PageHeader result = view.get();
        assertEquals(SAMPLE.magic(),       result.magic());
        assertEquals(SAMPLE.version(),     result.version());
        assertEquals(SAMPLE.pageType(),    result.pageType());
        assertEquals(SAMPLE.pageId(),      result.pageId());
        assertEquals(SAMPLE.checksum(),    result.checksum());
        assertEquals(SAMPLE.flags(),       result.flags());
        assertEquals(SAMPLE.slotOffset(),  result.slotOffset());
        assertEquals(SAMPLE.slotCount(),   result.slotCount());
        assertEquals(SAMPLE.freeSpace(),   result.freeSpace());
        assertEquals(SAMPLE.reserved(),    result.reserved());
    }

    @Test
    void set_marksPageDirty() {
        page.resetDirty();
        view.set(SAMPLE);
        assertTrue(page.isDirty());
    }

    @Test
    void getMagic_and_setMagic() {
        view.set(SAMPLE);
        view.setMagic(0xCAFEBABE);
        assertEquals(0xCAFEBABE, view.getMagic());
    }

    @Test
    void setMagic_marksPageDirty() {
        view.set(SAMPLE);
        page.resetDirty();
        view.setMagic(1);
        assertTrue(page.isDirty());
    }

    @Test
    void getVersion_and_setVersion() {
        view.set(SAMPLE);
        view.setVersion((short) 42);
        assertEquals((short) 42, view.getVersion());
    }

    @Test
    void setVersion_marksPageDirty() {
        view.set(SAMPLE);
        page.resetDirty();
        view.setVersion((short) 3);
        assertTrue(page.isDirty());
    }

    @Test
    void getPageType_and_setPageType() {
        view.set(SAMPLE);
        view.setPageType((short) 7);
        assertEquals((short) 7, view.getPageType());
    }

    @Test
    void getPageId_and_setPageId() {
        view.set(SAMPLE);
        view.setPageId(Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, view.getPageId());
    }

    @Test
    void setPageId_marksPageDirty() {
        view.set(SAMPLE);
        page.resetDirty();
        view.setPageId(1L);
        assertTrue(page.isDirty());
    }

    @Test
    void getChecksum_and_setChecksum() {
        view.set(SAMPLE);
        view.setChecksum(0xFFFFFFFF);
        assertEquals(0xFFFFFFFF, view.getChecksum());
    }

    @Test
    void getFlags_and_setFlags() {
        view.set(SAMPLE);
        view.setFlags(0b1111);
        assertEquals(0b1111, view.getFlags());
    }

    @Test
    void getSlotOffset_and_setSlotOffset() {
        view.set(SAMPLE);
        view.setSlotOffset(256);
        assertEquals(256, view.getSlotOffset());
    }

    @Test
    void getSlotCount_and_setSlotCount() {
        view.set(SAMPLE);
        view.setSlotCount(32);
        assertEquals(32, view.getSlotCount());
    }

    @Test
    void getFreeSpace_and_setFreeSpace() {
        view.set(SAMPLE);
        view.setFreeSpace(1024);
        assertEquals(1024, view.getFreeSpace());
    }

    @Test
    void getReserved_and_setReserved() {
        view.set(SAMPLE);
        view.setReserved(99);
        assertEquals(99, view.getReserved());
    }

    @Test
    void individualSetters_doNotAffectOtherFields() {
        view.set(SAMPLE);
        view.setSlotCount(99);
        assertEquals(SAMPLE.magic(),      view.getMagic());
        assertEquals(SAMPLE.version(),    view.getVersion());
        assertEquals(SAMPLE.pageType(),   view.getPageType());
        assertEquals(SAMPLE.pageId(),     view.getPageId());
        assertEquals(SAMPLE.checksum(),   view.getChecksum());
        assertEquals(SAMPLE.flags(),      view.getFlags());
        assertEquals(SAMPLE.slotOffset(), view.getSlotOffset());
        assertEquals(99,                  view.getSlotCount());
        assertEquals(SAMPLE.freeSpace(),  view.getFreeSpace());
        assertEquals(SAMPLE.reserved(),   view.getReserved());
    }

    @Test
    void pointTo_differentOffset_independentStorage() {
        PageHeader other = new PageHeader(0xCAFEBABE, (short) 2, (short) 3, 1L, 0, 0, 0, 0, 0, 0);

        view.pointTo(0);
        view.set(SAMPLE);

        view.pointTo(PageHeaderView.getLayoutSize());
        view.set(other);

        view.pointTo(0);
        assertEquals(SAMPLE.magic(), view.getMagic());

        view.pointTo(PageHeaderView.getLayoutSize());
        assertEquals(other.magic(), view.getMagic());
    }
}
