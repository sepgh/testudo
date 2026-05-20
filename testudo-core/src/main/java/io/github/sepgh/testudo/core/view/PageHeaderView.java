package io.github.sepgh.testudo.core.view;

import io.github.sepgh.testudo.core.Page;
import io.github.sepgh.testudo.core.model.PageHeader;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

public class PageHeaderView extends AbstractView<PageHeader> {

    private static final ValueLayout.OfInt   INT_LAYOUT   = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN).withByteAlignment(1);
    private static final ValueLayout.OfShort SHORT_LAYOUT = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN).withByteAlignment(1);
    private static final ValueLayout.OfLong  LONG_LAYOUT  = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN).withByteAlignment(1);

    public static final StructLayout STRUCT_LAYOUT = MemoryLayout.structLayout(
            INT_LAYOUT.withName("magic"),
            SHORT_LAYOUT.withName("version"),
            SHORT_LAYOUT.withName("pageType"),
            LONG_LAYOUT.withName("pageId"),
            INT_LAYOUT.withName("checksum"),
            INT_LAYOUT.withName("flags"),
            INT_LAYOUT.withName("slotOffset"),
            INT_LAYOUT.withName("slotCount"),
            INT_LAYOUT.withName("freeSpace"),
            INT_LAYOUT.withName("reserved")
    );

    private static final VarHandle VH_MAGIC       = STRUCT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("magic"));
    private static final VarHandle VH_VERSION      = STRUCT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("version"));
    private static final VarHandle VH_PAGE_TYPE    = STRUCT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("pageType"));
    private static final VarHandle VH_PAGE_ID      = STRUCT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("pageId"));
    private static final VarHandle VH_CHECKSUM     = STRUCT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("checksum"));
    private static final VarHandle VH_FLAGS        = STRUCT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("flags"));
    private static final VarHandle VH_SLOT_OFFSET  = STRUCT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("slotOffset"));
    private static final VarHandle VH_SLOT_COUNT   = STRUCT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("slotCount"));
    private static final VarHandle VH_FREE_SPACE   = STRUCT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("freeSpace"));
    private static final VarHandle VH_RESERVED     = STRUCT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("reserved"));

    public PageHeaderView(Page page) {
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
    protected void doSet(PageHeader value) {
        VH_MAGIC.set(segment,      offset, value.magic());
        VH_VERSION.set(segment,    offset, value.version());
        VH_PAGE_TYPE.set(segment,  offset, value.pageType());
        VH_PAGE_ID.set(segment,    offset, value.pageId());
        VH_CHECKSUM.set(segment,   offset, value.checksum());
        VH_FLAGS.set(segment,      offset, value.flags());
        VH_SLOT_OFFSET.set(segment, offset, value.slotOffset());
        VH_SLOT_COUNT.set(segment,  offset, value.slotCount());
        VH_FREE_SPACE.set(segment,  offset, value.freeSpace());
        VH_RESERVED.set(segment,    offset, value.reserved());
    }

    @Override
    protected PageHeader doRead() {
        return new PageHeader(
                getMagic(),
                getVersion(),
                getPageType(),
                getPageId(),
                getChecksum(),
                getFlags(),
                getSlotOffset(),
                getSlotCount(),
                getFreeSpace(),
                getReserved()
        );
    }

    public int getMagic() {
        return (int) VH_MAGIC.get(segment, offset);
    }

    public void setMagic(int magic) {
        VH_MAGIC.set(segment, offset, magic);
        page.dirty();
    }

    public short getVersion() {
        return (short) VH_VERSION.get(segment, offset);
    }

    public void setVersion(short version) {
        VH_VERSION.set(segment, offset, version);
        page.dirty();
    }

    public short getPageType() {
        return (short) VH_PAGE_TYPE.get(segment, offset);
    }

    public void setPageType(short pageType) {
        VH_PAGE_TYPE.set(segment, offset, pageType);
        page.dirty();
    }

    public long getPageId() {
        return (long) VH_PAGE_ID.get(segment, offset);
    }

    public void setPageId(long pageId) {
        VH_PAGE_ID.set(segment, offset, pageId);
        page.dirty();
    }

    public int getChecksum() {
        return (int) VH_CHECKSUM.get(segment, offset);
    }

    public void setChecksum(int checksum) {
        VH_CHECKSUM.set(segment, offset, checksum);
        page.dirty();
    }

    public int getFlags() {
        return (int) VH_FLAGS.get(segment, offset);
    }

    public void setFlags(int flags) {
        VH_FLAGS.set(segment, offset, flags);
        page.dirty();
    }

    public int getSlotOffset() {
        return (int) VH_SLOT_OFFSET.get(segment, offset);
    }

    public void setSlotOffset(int slotOffset) {
        VH_SLOT_OFFSET.set(segment, offset, slotOffset);
        page.dirty();
    }

    public int getSlotCount() {
        return (int) VH_SLOT_COUNT.get(segment, offset);
    }

    public void setSlotCount(int slotCount) {
        VH_SLOT_COUNT.set(segment, offset, slotCount);
        page.dirty();
    }

    public int getFreeSpace() {
        return (int) VH_FREE_SPACE.get(segment, offset);
    }

    public void setFreeSpace(int freeSpace) {
        VH_FREE_SPACE.set(segment, offset, freeSpace);
        page.dirty();
    }

    public int getReserved() {
        return (int) VH_RESERVED.get(segment, offset);
    }

    public void setReserved(int reserved) {
        VH_RESERVED.set(segment, offset, reserved);
        page.dirty();
    }
}
