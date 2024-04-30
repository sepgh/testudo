package com.github.sepgh.internal.storage;

import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class InMemoryHeaderManager implements HeaderManager {
    private final AtomicReference<Header> atomicReference = new AtomicReference<>();

    public InMemoryHeaderManager() {
    }
    public InMemoryHeaderManager(Header header) {
        atomicReference.set(header);
    }


    @Override
    public Header getHeader() {
        return atomicReference.get();
    }

    @Override
    public void update(Header header) throws IOException {
        atomicReference.set(header);
    }

    @Override
    public void update() throws IOException {}
}
