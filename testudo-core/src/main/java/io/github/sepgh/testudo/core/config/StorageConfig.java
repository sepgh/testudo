package io.github.sepgh.testudo.core.config;

public class StorageConfig {
    private PageSize pageSize;

    public int getPageSize() {
        return pageSize.size;
    }

    public enum PageSize {
        TINY(4),
        SMALL(8),
        MEDIUM(16),
        LARGE(32),
        ;
        private final int size;

        PageSize(int size) {
            this.size = size;
        }

        public int getSize() {
            return size * 1024;
        }
    }
}
