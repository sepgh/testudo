package com.github.sepgh.testudo.utils;

import lombok.Getter;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


@Getter
public class ReaderWriterLock {
    private final ReadWriteLock lock;
    private final Lock readLock;
    private final Lock writeLock;

    public ReaderWriterLock() {
        this.lock = new ReentrantReadWriteLock();
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
    }

    public ReaderWriterLock(ReadWriteLock readWriteLock) {
        this.lock = readWriteLock;
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
    }

}
