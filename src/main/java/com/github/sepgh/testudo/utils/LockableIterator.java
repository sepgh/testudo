package com.github.sepgh.testudo.utils;

import java.util.Iterator;

public interface LockableIterator<X> extends Iterator<X> {
    void lock();
    void unlock();
}
