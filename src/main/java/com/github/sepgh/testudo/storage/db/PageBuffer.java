package com.github.sepgh.testudo.storage.db;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import lombok.Getter;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class PageBuffer {
    private final Cache<PageTitle, PageWrapper> buffer;
    private final Function<PageTitle, Page> factory;
    private final Map<PageTitle, PageWrapper> referencedWrappers = new ConcurrentHashMap<>();

    public PageBuffer(int limit, Function<PageTitle, Page> factory) {
        this.factory = factory;
        this.buffer = CacheBuilder
                .newBuilder()
                .maximumSize(limit)
                .initialCapacity(limit / 2)
                .removalListener((RemovalListener<PageTitle, PageWrapper>) notification -> {
                    if (notification.getValue().getRefCount() == 0) {
                        referencedWrappers.remove(notification.getKey());
                    }
                })
                .build();
    }

    public synchronized Page acquire(PageTitle title) {
        PageWrapper pageWrapper1 = this.referencedWrappers.computeIfAbsent(title, pageTitle -> {
            PageWrapper pageWrapper;
            pageWrapper = buffer.getIfPresent(title);
            if (pageWrapper == null) {
                Page page = factory.apply(title);
                pageWrapper = new PageWrapper(page);
                buffer.put(title, pageWrapper);
            }
            return pageWrapper;
        });

        pageWrapper1.incrementRefCount();
        return pageWrapper1.getPage();
    }

    public synchronized void release(PageTitle title) {
        this.referencedWrappers.computeIfPresent(title, (pageTitle, pageWrapper) -> {
            pageWrapper.decrementRefCount();
            if (pageWrapper.getRefCount() == 0){
                buffer.invalidate(title);
                return null;
            }
            return pageWrapper;
        });
    }

    public void release(Page page){
        this.release(PageTitle.of(page));
    }

    public static class PageWrapper {
        @Getter
        private final Page page;
        private final AtomicInteger refCount = new AtomicInteger(0);

        private PageWrapper(Page page) {
            this.page = page;
        }

        public void incrementRefCount() {
            refCount.incrementAndGet();
        }

        public void decrementRefCount() {
            refCount.decrementAndGet();
        }

        public int getRefCount() {
            return refCount.get();
        }
    }

    public record PageTitle(int chunk, int pageNumber) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PageTitle pageTitle = (PageTitle) o;
            return chunk == pageTitle.chunk && pageNumber == pageTitle.pageNumber;
        }

        @Override
        public int hashCode() {
            return Objects.hash(chunk, pageNumber);
        }

        public static PageTitle of(Page page){
            return new PageTitle(page.getChunk(), page.getPageNumber());
        }
    }
}
