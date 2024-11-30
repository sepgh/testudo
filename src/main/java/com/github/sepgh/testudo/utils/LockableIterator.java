package com.github.sepgh.testudo.utils;

import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;


// Todo: can improve this to automatically lock when hasNext is called for first time
//       and unlock when hasNext returns false for first time
public abstract class LockableIterator<T> implements Iterator<T> {
    public abstract void lock();
    public abstract void unlock();
    
    public static <T> LockableIterator<T> wrapReader(Iterator<T> iterator, ReaderWriterLock readerWriterLock){
        return new LockableIterator<T>() {
            @Override
            public void lock() {
                readerWriterLock.getReadLock().lock();
            }

            @Override
            public void unlock() {
                readerWriterLock.getReadLock().unlock();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                return iterator.next();
            }
        };
    }

    public List<T> asList() {
        try {
            this.lock();
            return Lists.newArrayList(this);
        } finally {
            this.unlock();
        }
    }


}
