package io.github.sepgh.testudo.core.view;

import io.github.sepgh.testudo.core.Page;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class StringView extends AbstractView<String> {
    public static final int DEFAULT_MAX_SIZE = 256;
    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    private final int maxSize;
    private final Charset charset;

    public StringView(Page page) {
        this(page, DEFAULT_MAX_SIZE, DEFAULT_CHARSET);
    }

    public StringView(Page page, int maxSize) {
        this(page, maxSize, DEFAULT_CHARSET);
    }

    public StringView(Page page, int maxSize, Charset charset) {
        super(page);
        this.maxSize = maxSize;
        this.charset = charset;
    }

    @Override
    public int size() {
        return maxSize;
    }

    @Override
    protected void doSet(String value) {
        byte[] bytes = value.getBytes(charset);
        int length = Math.min(bytes.length, maxSize);
        MemorySegment.copy(MemorySegment.ofArray(bytes), ValueLayout.JAVA_BYTE, 0, segment, ValueLayout.JAVA_BYTE, offset, length);
        if (length < maxSize) {
            segment.set(ValueLayout.JAVA_BYTE, offset + length, (byte) 0);
        }
    }

    @Override
    protected String doRead() {
        byte[] bytes = new byte[maxSize];
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, offset, MemorySegment.ofArray(bytes), ValueLayout.JAVA_BYTE, 0, maxSize);
        int length = 0;
        while (length < maxSize && bytes[length] != 0) {
            length++;
        }
        return new String(bytes, 0, length, charset);
    }

    @Override
    public int compareTo(String o) {
        return get().compareTo(o);
    }
}
