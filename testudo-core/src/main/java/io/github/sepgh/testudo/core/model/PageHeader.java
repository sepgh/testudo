package io.github.sepgh.testudo.core.model;

public record PageHeader(
        int magic,
        short version,
        short pageType,
        long pageId,
        int checksum,
        int flags,
        int slotOffset,
        int slotCount,
        int freeSpace,
        int reserved
) {

}
