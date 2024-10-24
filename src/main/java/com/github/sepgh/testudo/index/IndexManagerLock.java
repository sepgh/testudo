package com.github.sepgh.testudo.index;

import lombok.Getter;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


@Getter
public class IndexManagerLock {
    private final ReadWriteLock lock;
    private final Lock readLock;
    private final Lock writeLock;

    public IndexManagerLock() {
        this.lock = new ReentrantReadWriteLock();
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
    }

    public IndexManagerLock(ReadWriteLock readWriteLock) {
        this.lock = readWriteLock;
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
    }

}
