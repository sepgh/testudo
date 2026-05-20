package io.github.sepgh.testudo.core.view;

import io.github.sepgh.testudo.core.Page;

import java.lang.foreign.MemorySegment;

public abstract class AbstractView<T>
        implements View<T> {

    protected final Page page;

    protected MemorySegment segment;
    protected long offset;

    protected AbstractView(Page page) {
        this.page = page;
        this.segment = page.segment();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V extends View<T>> V pointTo(long offset) {
        this.offset = offset;
        return (V) this;
    }

    public MemorySegment segment() {
        return segment;
    }

    public long offset() {
        return offset;
    }

    public final void set(T t) {
        doSet(t);
        page.dirty();
    }

    public final T get() {
        return doRead();
    }

    protected abstract void doSet(T t);
    protected abstract T doRead();
}