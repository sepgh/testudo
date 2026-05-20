package io.github.sepgh.testudo.core.view;

import io.github.sepgh.testudo.core.Page;
import io.github.sepgh.testudo.core.model.FilePointer;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

public class FilePointerView extends AbstractView<FilePointer> implements Comparable<FilePointer> {

    private static final ValueLayout.OfInt INT_LAYOUT =
            ValueLayout.JAVA_INT_UNALIGNED
                    .withOrder(ByteOrder.BIG_ENDIAN)
                    .withByteAlignment(1);

    public static final StructLayout STRUCT_LAYOUT = MemoryLayout.structLayout(
            INT_LAYOUT.withName("page"),
            INT_LAYOUT.withName("slot")
    );

    private static final VarHandle VH_PAGE = STRUCT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("page"));
    private static final VarHandle VH_SLOT = STRUCT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("slot"));

    public FilePointerView(Page page) {
        super(page);
    }

    public static int getLayoutSize() {
        return (int) STRUCT_LAYOUT.byteSize();
    }

    @Override
    public int size() {
        return getLayoutSize();
    }

    @Override
    protected void doSet(FilePointer value) {
        VH_PAGE.set(segment, offset, value.page() ^ Integer.MIN_VALUE);
        VH_SLOT.set(segment, offset, value.slot() ^ Integer.MIN_VALUE);
    }

    @Override
    protected FilePointer doRead() {
        int encodedPage = (int) VH_PAGE.get(segment, offset);
        int encodedSlot = (int) VH_SLOT.get(segment, offset);
        return new FilePointer(encodedPage ^ Integer.MIN_VALUE, encodedSlot ^ Integer.MIN_VALUE);
    }

    public int getPage() {
        int encoded = (int) VH_PAGE.get(segment, offset);
        return encoded ^ Integer.MIN_VALUE;
    }

    public void setPage(int pageValue) {
        VH_PAGE.set(segment, offset, pageValue ^ Integer.MIN_VALUE);
        page.dirty();
    }

    public int getSlot() {
        int encoded = (int) VH_SLOT.get(segment, offset);
        return encoded ^ Integer.MIN_VALUE;
    }

    public void setSlot(int slot) {
        VH_SLOT.set(segment, offset, slot ^ Integer.MIN_VALUE);
        page.dirty();
    }

    @Override
    public int compareTo(FilePointer o) {
        int storedPage = (int) VH_PAGE.get(segment, offset);
        int encodedPage = o.page() ^ Integer.MIN_VALUE;
        int pageCmp = Integer.compareUnsigned(storedPage, encodedPage);
        if (pageCmp != 0) return pageCmp;
        int storedSlot = (int) VH_SLOT.get(segment, offset);
        int encodedSlot = o.slot() ^ Integer.MIN_VALUE;
        return Integer.compareUnsigned(storedSlot, encodedSlot);
    }
}
