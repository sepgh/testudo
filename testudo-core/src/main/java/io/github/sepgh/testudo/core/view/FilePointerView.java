package io.github.sepgh.testudo.core.view;

import io.github.sepgh.testudo.core.Page;
import io.github.sepgh.testudo.core.model.FilePointer;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

public class FilePointerView extends AbstractView<FilePointer> {
    private static final ValueLayout.OfInt LAYOUT =
            ValueLayout.JAVA_INT_UNALIGNED
                    .withOrder(ByteOrder.BIG_ENDIAN);

    private static final int PAGE_OFFSET = 0;
    private static final int SLOT_OFFSET = Integer.BYTES;

    public FilePointerView(Page page) {
        super(page);
    }

    @Override
    public int size() {
        return Integer.BYTES + Integer.BYTES;
    }

    @Override
    protected void doSet(FilePointer value) {
        int encodedPage = value.page() ^ Integer.MIN_VALUE;
        int encodedSlot = value.slot() ^ Integer.MIN_VALUE;
        segment.set(LAYOUT, offset + PAGE_OFFSET, encodedPage);
        segment.set(LAYOUT, offset + SLOT_OFFSET, encodedSlot);
    }

    @Override
    protected FilePointer doRead() {
        int encodedPage = segment.get(LAYOUT, offset + PAGE_OFFSET);
        int encodedSlot = segment.get(LAYOUT, offset + SLOT_OFFSET);
        int page = encodedPage ^ Integer.MIN_VALUE;
        int slot = encodedSlot ^ Integer.MIN_VALUE;
        return new FilePointer(page, slot);
    }

    public int getPage() {
        int encoded = segment.get(LAYOUT, offset + PAGE_OFFSET);
        return encoded ^ Integer.MIN_VALUE;
    }

    public void setPage(int pageValue) {
        int encoded = pageValue ^ Integer.MIN_VALUE;
        segment.set(LAYOUT, offset + PAGE_OFFSET, encoded);
        page.dirty();
    }

    public int getSlot() {
        int encoded = segment.get(LAYOUT, offset + SLOT_OFFSET);
        return encoded ^ Integer.MIN_VALUE;
    }

    public void setSlot(int slot) {
        int encoded = slot ^ Integer.MIN_VALUE;
        segment.set(LAYOUT, offset + SLOT_OFFSET, encoded);
        page.dirty();
    }

    @Override
    public int compareTo(FilePointer o) {
        return get().compareTo(o);
    }
}
